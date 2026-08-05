"""Per-envelope redelivery-dedup progress records.

Replaces one Redis key per observation per destination with ONE key per
envelope: an item-sequence fingerprint plus a bitmap of delivered items. See
docs/superpowers/specs/2026-08-05-batch-envelope-dedup-design.md for why the
fingerprint is load-bearing.
"""
import hashlib
import logging

from core import utils

logger = logging.getLogger(__name__)

KEY_PREFIX = "batch_progress"
FINGERPRINT_BYTES = 8


def progress_key(batch_id, destination_id, provider_key):
    # provider_key MUST participate: cdip-routing groups transformed items per
    # (destination, effective_provider_key), so one batch_id + destination_id
    # legitimately yields several envelopes with disjoint item sets, and
    # omitting it would collide them onto one record.
    #
    # It is hashed rather than embedded because it is free-form and may contain
    # '.' or ':', which would break prefix-bucketed key analysis (the ops
    # profiler splits on exactly those) and make the key length unbounded.
    provider_digest = hashlib.sha256(str(provider_key).encode()).hexdigest()[:8]
    return f"{KEY_PREFIX}.{batch_id}.{destination_id}.{provider_digest}"


def fingerprint(items):
    """8-byte digest binding a record to an exact ordered item-identity list.

    Length-prefixed rather than delimiter-joined: `gundi_id` is declared as
    `Union[UUID, str]` in gundi_core, so arbitrary strings (including ones
    containing "|") are schema-legal. A delimiter join lets two different
    item lists collide (e.g. ["a|b", "c"] vs ["a", "b|c"]), and a collision
    here is not a benign duplicate - it makes decode() report a match against
    the wrong item list, so a bit gets read as "delivered" for an observation
    that was never sent. That is the one outcome this design forbids, so the
    encoding is built to be injective: no two different ordered id sequences
    produce the same bytes to hash.

    Every length is fixed-width (4-byte big-endian), including the leading item
    count. A decimal count would itself be variable-length, which would leave
    the encoding injective only under a side argument about how large a single
    `gundi_id` can get; fixed-width makes it unconditional.

    Injective *encoding* is not a collision-free *digest* — this truncates
    SHA-256 to 8 bytes, so two distinct inputs can still collide at ~2^-64.
    What makes that acceptable is scope: a fingerprint is only ever compared
    against records under the same `(batch_id, destination_id, provider_key)`
    key, and only a handful of item lists ever exist for one key within its 25h
    lifetime. So the exposure is ~2^-64 per comparison, not a birthday problem
    across every envelope in the system. Widen FINGERPRINT_BYTES if that
    scoping assumption ever stops holding.
    """
    h = hashlib.sha256()
    h.update(len(items).to_bytes(4, "big"))
    for item in items:
        raw = str(item.gundi_id).encode()
        h.update(len(raw).to_bytes(4, "big"))
        h.update(raw)
    return h.digest()[:FINGERPRINT_BYTES]


def encode(fp, delivered, n):
    """fingerprint || bitmap, where bit i is set when item i was delivered."""
    bitmap = bytearray((n + 7) // 8)
    for index in delivered:
        if 0 <= index < n:
            bitmap[index // 8] |= 1 << (index % 8)
    return bytes(fp) + bytes(bitmap)


def decode(raw, expected_fingerprint, n):
    """Delivered indices, or an empty set when the record is unusable.

    Empty always means "nothing known to be delivered" - a missing record, a
    truncated value, or a fingerprint mismatch (the envelope was re-published
    with a different item list, so positional bits no longer refer to the same
    observations). Callers must treat that as "deliver everything": a duplicate
    is acceptable, a silently skipped observation is not.
    """
    try:
        if len(expected_fingerprint) != FINGERPRINT_BYTES:
            # A caller-supplied fingerprint of the wrong length can never
            # match a validly-encoded record; trusting it anyway risks a
            # spurious match on truncated/malformed input.
            return set()
        if not raw or len(raw) < FINGERPRINT_BYTES:
            return set()
        if bytes(raw[:FINGERPRINT_BYTES]) != bytes(expected_fingerprint):
            return set()
        bitmap = raw[FINGERPRINT_BYTES:]
        delivered = set()
        for index in range(n):
            byte_index = index // 8
            if byte_index >= len(bitmap):
                break  # bitmap shorter than n: the remaining bits are absent
            if bitmap[byte_index] & (1 << (index % 8)):
                delivered.add(index)
        return delivered
    except (TypeError, ValueError) as e:
        # A cache returning an unexpected type must fail open, never raise.
        logger.warning(f"Discarding unusable batch progress record: {type(e).__name__} {e}")
        return set()


def read_progress(batch_id, destination_id, provider_key):
    """Raw record bytes, or None. Never raises.

    Reads utils._cache_db at call time (not via a module-level import) so the
    test suite's `mocker.patch("core.utils._cache_db", ...)` takes effect -
    same pattern as core/throttling.py.
    """
    try:
        return utils._cache_db.get(progress_key(batch_id, destination_id, provider_key))
    except Exception as e:
        logger.warning(f"Error reading batch progress from cache: {e}", exc_info=True)
        return None


def write_progress(batch_id, destination_id, provider_key, fp, delivered, n, ttl):
    """Persist the record. No-op when nothing was delivered. Never raises."""
    if not delivered:
        return
    try:
        utils._cache_db.setex(
            name=progress_key(batch_id, destination_id, provider_key),
            time=ttl,
            value=encode(fp, delivered, n),
        )
    except Exception as e:
        logger.warning(f"Error writing batch progress to cache: {e}", exc_info=True)

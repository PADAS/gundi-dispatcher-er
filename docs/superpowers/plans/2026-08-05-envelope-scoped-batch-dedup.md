# Envelope-Scoped Batch Redelivery Dedup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the batch path's one-Redis-key-per-observation idempotency cache with one per-envelope progress record, cutting projected steady-state Redis usage from ~9.4 GB to ~0.1-0.2 GB.

**Architecture:** A new `core/batch_progress.py` module stores, per envelope, a single Redis string keyed on `(batch_id, destination_id, sha256(provider_key)[:8])`. Its value is an 8-byte fingerprint of the item-identity sequence followed by a bitmap whose bit `i` marks `batch.items[i]` as delivered. `dispatch_observations_batch_v2` reads that record once instead of doing N `GET`s, and flushes it once per chunk instead of doing N `SETEX`s. A transitional dual-read fallback to the legacy per-item keys prevents a duplicate burst from envelopes in flight at deploy time.

**Tech Stack:** Python 3.10, `redis`/`walrus` via `core.utils._cache_db`, pytest 7.2.1 + pytest-asyncio 0.20.3 + pytest-mock 3.10.0, OpenTelemetry spans.

**Spec:** `docs/superpowers/specs/2026-08-05-batch-envelope-dedup-design.md`

## Global Constraints

- Every function in `core/batch_progress.py` must **never raise**. The dedup layer failing must never stall delivery. On any error, behave as "nothing known to be delivered".
- Duplicates are the accepted failure currency; a **skipped (never-delivered) observation is never acceptable**. Every ambiguous case fails open toward re-posting.
- `DISPATCHED_BATCH_PROGRESS_CACHE_TTL` default is `90000` and must exceed `MAX_EVENT_AGE_SECONDS` (86400 in prod).
- Access the Redis client as `utils._cache_db` **at call time**, never `from core.utils import _cache_db`. The test suite patches `core.utils._cache_db`, so an import-time binding would silently bypass the mock. `core/throttling.py` sets this precedent (`db = utils._cache_db` inside each function).
- Do **not** touch the single-item path. `cache_dispatched_observation`, `get_dispatched_observation`, and the 1h `DISPATCHED_OBSERVATIONS_CACHE_TTL` stay exactly as they are — `get_dispatched_observation` supplies `external_id` for `related_to` attachment delivery.
- There is **no `fakeredis`** in this repo. Redis is mocked with `MagicMock` via the `mock_cache_*` fixtures in `tests/conftest.py`.
- `core.utils._cache_db` is **one shared mock** serving both `get_integration_details`' config-object cache and the dedup cache. Any test using `get.side_effect` must account for the config-cache read that happens **first**, once per envelope.
- Call `_cache_db.setex` with keyword arguments (`name=`, `time=`, `value=`) so tests can filter calls by `call.kwargs["name"]` prefix.

---

### Task 1: Pure progress-record encoding in `core/batch_progress.py`

Key construction, fingerprinting, and bitmap encode/decode. All pure functions — no Redis, no mocking needed. This is where the correctness-critical fail-open logic lives.

**Files:**
- Create: `core/batch_progress.py`
- Test: `tests/test_batch_progress.py`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `progress_key(batch_id, destination_id, provider_key) -> str`
  - `fingerprint(items) -> bytes` (8 bytes; `items` is any sequence of objects with a `.gundi_id` attribute)
  - `encode(fp: bytes, delivered: set[int], n: int) -> bytes`
  - `decode(raw: bytes | None, expected_fingerprint: bytes, n: int) -> set[int]`
  - Module constants `KEY_PREFIX = "batch_progress"`, `FINGERPRINT_BYTES = 8`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_batch_progress.py`:

```python
from types import SimpleNamespace

from core import batch_progress


def _items(*gundi_ids):
    return [SimpleNamespace(gundi_id=g) for g in gundi_ids]


def test_progress_key_hashes_provider_key_and_is_delimiter_safe():
    key = batch_progress.progress_key(
        "8a5535df-1b9b-412b-9fd5-e29b09582222",
        "338225f3-91f9-4fe1-b013-353a229ce504",
        "gundi.movebank:abc123",  # contains BOTH separators
    )
    assert key.startswith("batch_progress.8a5535df-1b9b-412b-9fd5-e29b09582222.")
    # 4 segments exactly: prefix, batch_id, destination_id, provider digest
    assert len(key.split(".")) == 4
    assert "movebank" not in key  # provider_key never appears literally


def test_progress_key_differs_per_provider_key():
    args = ("batch-1", "dest-1")
    assert batch_progress.progress_key(*args, "key-a") != batch_progress.progress_key(*args, "key-b")


def test_fingerprint_is_stable_and_order_sensitive():
    a = batch_progress.fingerprint(_items("id-0", "id-1", "id-2"))
    assert a == batch_progress.fingerprint(_items("id-0", "id-1", "id-2"))
    assert len(a) == 8
    assert a != batch_progress.fingerprint(_items("id-2", "id-1", "id-0"))  # reorder
    assert a != batch_progress.fingerprint(_items("id-0", "id-1"))          # removed
    assert a != batch_progress.fingerprint(_items("id-0", "id-1", "id-2", "id-3"))  # added


def test_encode_sets_expected_bits():
    fp = b"\x00" * 8
    assert batch_progress.encode(fp, {0, 1, 2}, 3)[8:] == bytes([0b00000111])
    assert batch_progress.encode(fp, {0, 2}, 3)[8:] == bytes([0b00000101])
    assert batch_progress.encode(fp, {8}, 9)[8:] == bytes([0b00000000, 0b00000001])


def test_encode_ignores_out_of_range_indices():
    fp = b"\x00" * 8
    assert batch_progress.encode(fp, {-1, 5, 99}, 3)[8:] == bytes([0b00000000])


def test_encode_decode_round_trips_across_byte_boundaries():
    for n in (0, 1, 7, 8, 9, 200):
        items = _items(*[f"id-{i}" for i in range(n)])
        fp = batch_progress.fingerprint(items)
        delivered = set(range(0, n, 3))
        raw = batch_progress.encode(fp, delivered, n)
        assert batch_progress.decode(raw, fp, n) == delivered


def test_decode_returns_empty_when_record_missing_or_truncated():
    fp = b"\x01" * 8
    assert batch_progress.decode(None, fp, 3) == set()
    assert batch_progress.decode(b"", fp, 3) == set()
    assert batch_progress.decode(b"\x01" * 7, fp, 3) == set()  # shorter than fingerprint


def test_decode_returns_empty_on_fingerprint_mismatch():
    # The envelope was re-published with a different item list, so positional
    # bits no longer refer to the same observations. Must fail open.
    items = _items("id-0", "id-1", "id-2")
    raw = batch_progress.encode(batch_progress.fingerprint(items), {0, 1, 2}, 3)
    other = batch_progress.fingerprint(_items("id-9", "id-8", "id-7"))
    assert batch_progress.decode(raw, other, 3) == set()


def test_decode_treats_short_bitmap_as_absent_bits():
    fp = b"\x02" * 8
    raw = fp + bytes([0b00000101])  # only 1 byte of bitmap, but n spans 2
    assert batch_progress.decode(raw, fp, 12) == {0, 2}


def test_decode_ignores_extra_bitmap_bytes_beyond_n():
    fp = b"\x03" * 8
    raw = fp + bytes([0b11111111, 0b11111111])
    assert batch_progress.decode(raw, fp, 3) == {0, 1, 2}


def test_decode_returns_empty_for_non_bytes_value():
    # A cache returning a str (or anything unexpected) must fail open, not raise.
    fp = b"\x04" * 8
    assert batch_progress.decode("not-bytes", fp, 3) == set()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_batch_progress.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'core.batch_progress'`

- [ ] **Step 3: Write the implementation**

Create `core/batch_progress.py`:

```python
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
    """8-byte digest binding a record to an exact ordered item-identity list."""
    joined = "|".join(str(item.gundi_id) for item in items)
    return hashlib.sha256(joined.encode()).digest()[:FINGERPRINT_BYTES]


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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_batch_progress.py -v`
Expected: PASS (12 tests)

- [ ] **Step 5: Commit**

```bash
git add core/batch_progress.py tests/test_batch_progress.py
git commit -m "feat: add per-envelope batch progress record encoding"
```

---

### Task 2: Redis read/write wrappers

Thin, never-raising accessors around the shared cache client.

**Files:**
- Modify: `core/batch_progress.py` (append)
- Test: `tests/test_batch_progress.py` (append)

**Interfaces:**
- Consumes: `progress_key`, `encode` from Task 1.
- Produces:
  - `read_progress(batch_id, destination_id, provider_key) -> bytes | None`
  - `write_progress(batch_id, destination_id, provider_key, fp, delivered, n, ttl) -> None`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_batch_progress.py`:

```python
def test_read_progress_returns_cached_value(mocker):
    db = mocker.MagicMock()
    db.get.return_value = b"payload"
    mocker.patch("core.utils._cache_db", db)

    assert batch_progress.read_progress("b1", "d1", "pk") == b"payload"
    db.get.assert_called_once_with(batch_progress.progress_key("b1", "d1", "pk"))


def test_read_progress_returns_none_on_redis_error(mocker):
    db = mocker.MagicMock()
    db.get.side_effect = RuntimeError("redis down")
    mocker.patch("core.utils._cache_db", db)

    assert batch_progress.read_progress("b1", "d1", "pk") is None


def test_write_progress_stores_encoded_record_with_ttl(mocker):
    db = mocker.MagicMock()
    mocker.patch("core.utils._cache_db", db)
    fp = batch_progress.fingerprint(_items("id-0", "id-1", "id-2"))

    batch_progress.write_progress("b1", "d1", "pk", fp, {0, 2}, 3, ttl=90000)

    db.setex.assert_called_once_with(
        name=batch_progress.progress_key("b1", "d1", "pk"),
        time=90000,
        value=fp + bytes([0b00000101]),
    )


def test_write_progress_is_a_noop_when_nothing_delivered(mocker):
    # An all-zero record is indistinguishable from a missing one, so never
    # write it - that keeps decode's empty-set result unambiguous.
    db = mocker.MagicMock()
    mocker.patch("core.utils._cache_db", db)

    batch_progress.write_progress("b1", "d1", "pk", b"\x00" * 8, set(), 3, ttl=90000)

    assert not db.setex.called


def test_write_progress_swallows_redis_error(mocker):
    db = mocker.MagicMock()
    db.setex.side_effect = RuntimeError("redis down")
    mocker.patch("core.utils._cache_db", db)

    batch_progress.write_progress("b1", "d1", "pk", b"\x00" * 8, {0}, 3, ttl=90000)  # must not raise
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_batch_progress.py -k "read_progress or write_progress" -v`
Expected: FAIL with `AttributeError: module 'core.batch_progress' has no attribute 'read_progress'`

- [ ] **Step 3: Write the implementation**

Append to `core/batch_progress.py`:

```python
def read_progress(batch_id, destination_id, provider_key):
    """Raw record bytes, or None. Never raises.

    Reads utils._cache_db at call time (not via a module-level import) so the
    test suite's `mocker.patch("core.utils._cache_db", ...)` takes effect -
    same pattern as core/throttling.py.
    """
    try:
        return utils._cache_db.get(progress_key(batch_id, destination_id, provider_key))
    except Exception as e:
        logger.warning(f"Error reading batch progress from cache: {e}")
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
        logger.warning(f"Error writing batch progress to cache: {e}")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_batch_progress.py -v`
Expected: PASS (17 tests)

- [ ] **Step 5: Commit**

```bash
git add core/batch_progress.py tests/test_batch_progress.py
git commit -m "feat: add never-raising Redis accessors for batch progress"
```

---

### Task 3: Settings for the new record and the transition flag

**Files:**
- Modify: `core/settings.py:55-60`

**Interfaces:**
- Produces: `settings.DISPATCHED_BATCH_PROGRESS_CACHE_TTL` (int), `settings.BATCH_DEDUP_LEGACY_FALLBACK_ENABLED` (bool)

- [ ] **Step 1: Add the settings**

In `core/settings.py`, immediately after the existing `DISPATCHED_OBSERVATIONS_BATCH_CACHE_TTL` line, add:

```python
# Idempotency record for batch-delivered observations: ONE key per envelope
# holding an item-sequence fingerprint plus a delivered-item bitmap. Must
# exceed the PubSub retry window (MAX_EVENT_AGE_SECONDS, 86400 in prod) so
# envelope redeliveries keep skipping delivered items. Replaces the per-item
# dispatched_observation keys that filled prod Redis on 2026-08-05.
DISPATCHED_BATCH_PROGRESS_CACHE_TTL = env.int("DISPATCHED_BATCH_PROGRESS_CACHE_TTL", 90000)
# Transitional: when an envelope has no progress record, fall back to reading
# the legacy per-item dispatched_observation keys. Stops the deploy from
# re-posting everything already delivered for envelopes in flight at rollout.
# Set false >=25h after deploy, then delete the fallback (see the design doc).
BATCH_DEDUP_LEGACY_FALLBACK_ENABLED = env.bool("BATCH_DEDUP_LEGACY_FALLBACK_ENABLED", True)
```

Leave `DISPATCHED_OBSERVATIONS_BATCH_CACHE_TTL` in place for now; it is removed in the follow-up PR described in Task 5.

- [ ] **Step 2: Verify settings import cleanly**

Run: `python -c "from core import settings; print(settings.DISPATCHED_BATCH_PROGRESS_CACHE_TTL, settings.BATCH_DEDUP_LEGACY_FALLBACK_ENABLED)"`
Expected: `90000 True`

- [ ] **Step 3: Commit**

```bash
git add core/settings.py
git commit -m "feat: add batch progress TTL and legacy-fallback settings"
```

---

### Task 4: Rewire `dispatch_observations_batch_v2` onto the progress record

Replace the N-`GET` filter with one read, and the N-`SETEX` writes with one flush per chunk. Includes the legacy dual-read fallback, since read-path behaviour is a single reviewable unit.

**Files:**
- Modify: `core/event_handlers.py:17-27` (imports), `core/event_handlers.py:411-421` (delete `_cache_item_as_dispatched`), `core/event_handlers.py:424-530` (handler)
- Test: `tests/test_process_observation_batches_v2.py`

**Interfaces:**
- Consumes: `batch_progress.fingerprint`, `.read_progress`, `.write_progress`, `.decode` (Tasks 1-2); `settings.DISPATCHED_BATCH_PROGRESS_CACHE_TTL`, `settings.BATCH_DEDUP_LEGACY_FALLBACK_ENABLED` (Task 3).
- Produces: no new public names. `_cache_item_as_dispatched` is **removed**; `_flush_progress(batch, destination_id, fp, delivered)` and `_legacy_delivered_indices(batch, destination_id)` are added as module-private helpers.

- [ ] **Step 1: Write the failing tests**

In `tests/test_process_observation_batches_v2.py`, add these helpers next to the existing `_dispatched_observation_setex_calls`:

```python
def _progress_setex_calls(mock_cache):
    return [
        call for call in mock_cache.setex.call_args_list
        if call.kwargs.get("name", "").startswith("batch_progress.")
    ]


def _progress_value(items_count, delivered, gundi_ids=None):
    """Build a record matching what _make_batch_request's items fingerprint to."""
    from types import SimpleNamespace

    from core import batch_progress

    ids = gundi_ids or [f"23ca4b15-18b6-4cf4-9da6-36dd69c6f63{i}" for i in range(items_count)]
    items = [SimpleNamespace(gundi_id=g) for g in ids]
    return batch_progress.encode(batch_progress.fingerprint(items), delivered, items_count)
```

The gundi_ids in `_progress_value` must stay in sync with `_make_batch_request`,
which generates `23ca4b15-18b6-4cf4-9da6-36dd69c6f63{i}` — the fingerprint is
computed over exactly those, so a drift here silently turns every
progress-record test into a fingerprint-mismatch test.

Then add these tests:

```python
@pytest.mark.asyncio
async def test_batch_writes_one_progress_record_instead_of_per_item_keys(
    mocker,
    mock_cache_empty,
    mock_gundi_client_v2_class,
    mock_erclient_class,
    mock_pubsub_client,
):
    mocker.patch("core.utils._cache_db", mock_cache_empty)
    mocker.patch("core.utils.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("core.dispatchers.TokenCachingAsyncERClient", mock_erclient_class)
    mocker.patch("core.utils.pubsub", mock_pubsub_client)
    mocker.patch.object(settings, "BATCH_DEDUP_LEGACY_FALLBACK_ENABLED", False)

    await process_request(_make_batch_request(mocker, items_count=3))

    assert _dispatched_observation_setex_calls(mock_cache_empty) == []
    calls = _progress_setex_calls(mock_cache_empty)
    assert len(calls) == 1
    assert calls[0].kwargs["time"] == settings.DISPATCHED_BATCH_PROGRESS_CACHE_TTL
    assert calls[0].kwargs["value"][8:] == bytes([0b00000111])


@pytest.mark.asyncio
async def test_batch_reads_progress_once_not_per_item(
    mocker,
    mock_cache_empty,
    mock_gundi_client_v2_class,
    mock_erclient_class,
    mock_pubsub_client,
):
    mocker.patch("core.utils._cache_db", mock_cache_empty)
    mocker.patch("core.utils.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("core.dispatchers.TokenCachingAsyncERClient", mock_erclient_class)
    mocker.patch("core.utils.pubsub", mock_pubsub_client)
    mocker.patch.object(settings, "BATCH_DEDUP_LEGACY_FALLBACK_ENABLED", False)

    await process_request(_make_batch_request(mocker, items_count=25))

    progress_reads = [
        call for call in mock_cache_empty.get.call_args_list
        if str(call.args[0]).startswith("batch_progress.")
    ]
    assert len(progress_reads) == 1
    assert not any(
        str(call.args[0]).startswith("dispatched_observation.")
        for call in mock_cache_empty.get.call_args_list
    )


@pytest.mark.asyncio
async def test_batch_skips_items_already_marked_in_bitmap(
    mocker,
    mock_gundi_client_v2_class,
    mock_erclient_class,
    mock_pubsub_client,
):
    # Leading None is get_integration_details' own config-cache miss.
    mock_cache = mocker.MagicMock()
    mock_cache.get.side_effect = (None, _progress_value(3, {0}))
    mocker.patch("core.utils._cache_db", mock_cache)
    mocker.patch("core.utils.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("core.dispatchers.TokenCachingAsyncERClient", mock_erclient_class)
    mocker.patch("core.utils.pubsub", mock_pubsub_client)
    mocker.patch.object(settings, "BATCH_DEDUP_LEGACY_FALLBACK_ENABLED", False)

    await process_request(_make_batch_request(mocker, items_count=3))

    posted = mock_erclient_class.return_value._post.call_args.kwargs["payload"]
    assert len(posted) == 2
    # The flush unions the pre-existing bit with the newly delivered ones
    assert _progress_setex_calls(mock_cache)[-1].kwargs["value"][8:] == bytes([0b00000111])


@pytest.mark.asyncio
async def test_batch_fully_delivered_posts_nothing_but_still_publishes(
    mocker,
    mock_gundi_client_v2_class,
    mock_erclient_class,
    mock_pubsub_client,
):
    mock_cache = mocker.MagicMock()
    mock_cache.get.side_effect = (None, _progress_value(3, {0, 1, 2}))
    mocker.patch("core.utils._cache_db", mock_cache)
    mocker.patch("core.utils.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("core.dispatchers.TokenCachingAsyncERClient", mock_erclient_class)
    mocker.patch("core.utils.pubsub", mock_pubsub_client)
    mocker.patch.object(settings, "BATCH_DEDUP_LEGACY_FALLBACK_ENABLED", False)

    await process_request(_make_batch_request(mocker, items_count=3))

    assert not mock_erclient_class.return_value._post.called
    # The original attempt may have died after recording progress and before
    # publishing, so the delivered event must still go out for ALL items.
    assert mock_pubsub_client.PublisherClient.return_value.publish.called


@pytest.mark.asyncio
async def test_batch_fingerprint_mismatch_reposts_everything(
    mocker,
    mock_gundi_client_v2_class,
    mock_erclient_class,
    mock_pubsub_client,
):
    # A record whose bits are all set, but computed over DIFFERENT item ids -
    # positional bits are meaningless, so it must fail open.
    stale = _progress_value(3, {0, 1, 2}, gundi_ids=["other-0", "other-1", "other-2"])
    mock_cache = mocker.MagicMock()
    mock_cache.get.side_effect = (None, stale)
    mocker.patch("core.utils._cache_db", mock_cache)
    mocker.patch("core.utils.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("core.dispatchers.TokenCachingAsyncERClient", mock_erclient_class)
    mocker.patch("core.utils.pubsub", mock_pubsub_client)
    mocker.patch.object(settings, "BATCH_DEDUP_LEGACY_FALLBACK_ENABLED", False)

    await process_request(_make_batch_request(mocker, items_count=3))

    posted = mock_erclient_class.return_value._post.call_args.kwargs["payload"]
    assert len(posted) == 3


@pytest.mark.asyncio
async def test_batch_flushes_progress_per_chunk(
    mocker,
    mock_cache_empty,
    mock_gundi_client_v2_class,
    mock_erclient_class,
    mock_pubsub_client,
):
    mocker.patch("core.utils._cache_db", mock_cache_empty)
    mocker.patch("core.utils.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("core.dispatchers.TokenCachingAsyncERClient", mock_erclient_class)
    mocker.patch("core.utils.pubsub", mock_pubsub_client)
    mocker.patch.object(settings, "ER_BULK_SIZE", 2)
    mocker.patch.object(settings, "BATCH_DEDUP_LEGACY_FALLBACK_ENABLED", False)

    await process_request(_make_batch_request(mocker, items_count=3))

    calls = _progress_setex_calls(mock_cache_empty)
    assert len(calls) == 2  # one per chunk
    assert calls[0].kwargs["value"][8:] == bytes([0b00000011])
    assert calls[1].kwargs["value"][8:] == bytes([0b00000111])


@pytest.mark.asyncio
async def test_batch_persists_progress_before_raising_on_transient_error(
    mocker,
    mock_cache_empty,
    mock_gundi_client_v2_class,
    mock_erclient_class,
    mock_pubsub_client,
):
    mocker.patch("core.utils._cache_db", mock_cache_empty)
    mocker.patch("core.utils.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("core.dispatchers.TokenCachingAsyncERClient", mock_erclient_class)
    mocker.patch("core.utils.pubsub", mock_pubsub_client)
    mocker.patch.object(settings, "ER_BULK_SIZE", 2)
    mocker.patch.object(settings, "BATCH_DEDUP_LEGACY_FALLBACK_ENABLED", False)
    err = ERClientException("ER error ON POST: service unavailable")
    err.status_code = 503
    mock_erclient_class.return_value._post.side_effect = [None, err]

    with pytest.raises(Exception):
        await process_request(_make_batch_request(mocker, items_count=3))

    # Chunk 1's progress must be durable before the nack, or redelivery
    # re-posts it.
    calls = _progress_setex_calls(mock_cache_empty)
    assert len(calls) == 1
    assert calls[0].kwargs["value"][8:] == bytes([0b00000011])


@pytest.mark.asyncio
async def test_batch_permanent_error_marks_only_individually_delivered_items(
    mocker,
    mock_cache_empty,
    mock_gundi_client_v2_class,
    mock_erclient_class,
    mock_pubsub_client,
):
    mocker.patch("core.utils._cache_db", mock_cache_empty)
    mocker.patch("core.utils.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("core.dispatchers.TokenCachingAsyncERClient", mock_erclient_class)
    mocker.patch("core.utils.pubsub", mock_pubsub_client)
    mocker.patch.object(settings, "BATCH_DEDUP_LEGACY_FALLBACK_ENABLED", False)
    bulk_err = ERClientException("ER error ON POST: bad request")
    bulk_err.status_code = 400
    mock_erclient_class.return_value._post.side_effect = bulk_err
    item_err = ERClientException("ER error: bad record")
    item_err.status_code = 400
    mock_erclient_class.return_value.post_sensor_observation.side_effect = [None, item_err, None]

    await process_request(_make_batch_request(mocker, items_count=3))

    # Items 0 and 2 succeeded individually; item 1 failed and keeps its bit
    # unset so redelivery retries it.
    assert _progress_setex_calls(mock_cache_empty)[-1].kwargs["value"][8:] == bytes([0b00000101])


@pytest.mark.asyncio
async def test_batch_falls_back_to_legacy_per_item_keys(
    mocker,
    mock_gundi_client_v2_class,
    mock_erclient_class,
    mock_pubsub_client,
    dispatched_event,
):
    # No progress record; legacy key present for item 0 only. Order of gets:
    # config cache, progress, then one per item for the legacy sweep.
    mock_cache = mocker.MagicMock()
    mock_cache.get.side_effect = (None, None, dispatched_event.json(), None, None)
    mocker.patch("core.utils._cache_db", mock_cache)
    mocker.patch("core.utils.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("core.dispatchers.TokenCachingAsyncERClient", mock_erclient_class)
    mocker.patch("core.utils.pubsub", mock_pubsub_client)
    mocker.patch.object(settings, "BATCH_DEDUP_LEGACY_FALLBACK_ENABLED", True)

    await process_request(_make_batch_request(mocker, items_count=3))

    posted = mock_erclient_class.return_value._post.call_args.kwargs["payload"]
    assert len(posted) == 2
    # The envelope migrates to the new format, including the legacy-derived bit
    assert _progress_setex_calls(mock_cache)[-1].kwargs["value"][8:] == bytes([0b00000111])


@pytest.mark.asyncio
async def test_batch_does_not_read_legacy_keys_when_fallback_disabled(
    mocker,
    mock_cache_empty,
    mock_gundi_client_v2_class,
    mock_erclient_class,
    mock_pubsub_client,
):
    mocker.patch("core.utils._cache_db", mock_cache_empty)
    mocker.patch("core.utils.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("core.dispatchers.TokenCachingAsyncERClient", mock_erclient_class)
    mocker.patch("core.utils.pubsub", mock_pubsub_client)
    mocker.patch.object(settings, "BATCH_DEDUP_LEGACY_FALLBACK_ENABLED", False)

    await process_request(_make_batch_request(mocker, items_count=3))

    assert not any(
        str(call.args[0]).startswith("dispatched_observation.")
        for call in mock_cache_empty.get.call_args_list
    )


@pytest.mark.asyncio
async def test_batch_delivers_normally_when_cache_read_and_write_fail(
    mocker,
    mock_gundi_client_v2_class,
    mock_erclient_class,
    mock_pubsub_client,
):
    mock_cache = mocker.MagicMock()
    mock_cache.get.return_value = None
    mock_cache.setex.side_effect = RuntimeError("redis down")
    mocker.patch("core.utils._cache_db", mock_cache)
    mocker.patch("core.utils.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("core.dispatchers.TokenCachingAsyncERClient", mock_erclient_class)
    mocker.patch("core.utils.pubsub", mock_pubsub_client)
    mocker.patch.object(settings, "BATCH_DEDUP_LEGACY_FALLBACK_ENABLED", False)

    await process_request(_make_batch_request(mocker, items_count=3))  # must not raise

    posted = mock_erclient_class.return_value._post.call_args.kwargs["payload"]
    assert len(posted) == 3
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_process_observation_batches_v2.py -v`
Expected: the new tests FAIL (no `batch_progress.` keys are written yet, so `_progress_setex_calls` returns `[]`)

- [ ] **Step 3: Update the imports**

In `core/event_handlers.py`, add `batch_progress` to the `core` import on line 16 and drop the now-unused `mark_observation_dispatched` from the `core.utils` import block:

```python
from core import tracing, dispatchers, settings, throttling, batch_progress
from core.errors import ReferenceDataError, DispatcherException
from core.utils import (
    ExtraKeys,
    get_integration_details,
    get_dispatched_observation,
    cache_dispatched_observation,
    is_observation_dispatched,
    is_null,
    publish_event,
)
```

`is_observation_dispatched` stays — the legacy fallback uses it.

- [ ] **Step 4: Replace `_cache_item_as_dispatched` with the new helpers**

Delete the whole `_cache_item_as_dispatched` function (`core/event_handlers.py:411-421`) and put these in its place:

```python
def _flush_progress(batch, destination_id, fp, delivered):
    # One write per chunk, not per item. Called after every successful chunk so
    # progress is durable BEFORE the transient-error branch raises to nack.
    batch_progress.write_progress(
        batch_id=batch.batch_id,
        destination_id=destination_id,
        provider_key=batch.provider_key,
        fp=fp,
        delivered=delivered,
        n=len(batch.items),
        ttl=settings.DISPATCHED_BATCH_PROGRESS_CACHE_TTL,
    )


def _legacy_delivered_indices(batch, destination_id):
    # Transitional: envelopes in flight when the progress record shipped carry
    # per-item dispatched_observation keys instead. Reading them for one 25h
    # window (> MAX_EVENT_AGE_SECONDS) keeps the deploy from re-posting
    # everything already delivered. Deleted with the flag.
    return {
        index for index, item in enumerate(batch.items)
        if is_observation_dispatched(
            gundi_id=str(item.gundi_id), destination_id=destination_id
        )
    }
```

- [ ] **Step 5: Rewire the read path**

In `dispatch_observations_batch_v2`, replace the `pending = [...]` list comprehension and its surrounding lines (`core/event_handlers.py`, the block starting `# Skip items already delivered`) with:

```python
        if not batch.items:
            # Empty batches are valid to parse and are a no-op by contract
            # (see gundi_core/events/batches.py).
            return

        fp = batch_progress.fingerprint(batch.items)
        delivered = batch_progress.decode(
            batch_progress.read_progress(
                batch.batch_id, destination_id, batch.provider_key
            ),
            fp,
            len(batch.items),
        )
        dedup_source = "batch_progress" if delivered else "none"
        if not delivered and settings.BATCH_DEDUP_LEGACY_FALLBACK_ENABLED:
            legacy = _legacy_delivered_indices(batch, destination_id)
            if legacy:
                delivered = legacy
                dedup_source = "legacy"
        current_span.set_attribute("dedup_source", dedup_source)

        # Skip items already delivered — makes envelope redelivery idempotent
        pending = [
            (index, item) for index, item in enumerate(batch.items)
            if index not in delivered
        ]
```

Move the `if not batch.items: return` guard to sit immediately after `destination_id`/`stream_type` are assigned and the span attributes are set, but **before** the `get_integration_details` call — an empty envelope should not cost a portal lookup.

- [ ] **Step 6: Rewire the write path**

Within the chunk loop, `chunk` is now a list of `(index, item)` pairs. Make these four edits:

The bulk send:

```python
                await dispatcher.send([item.observation for _, item in chunk])
```

The per-item fallback loop header and its success branch:

```python
                    fallback_delivered_any = False
                    for index, item in chunk:
                        single_dispatcher = dispatchers.ERObservationDispatcher(
                            integration=destination_integration,
                            provider=batch.provider_key,
                        )
                        try:
                            await single_dispatcher.send(item.observation)
                        except Exception as item_exc:
                            logger.warning(
                                f"Observation {item.gundi_id} in batch {batch.batch_id} failed individually: {item_exc}"
                            )
                            await _publish_item_delivery_failed(batch, item, item_exc)
                        else:
                            delivered.add(index)
                            delivered_gundi_ids.append(str(item.gundi_id))
                            fallback_delivered_any = True
                    _flush_progress(batch, destination_id, fp, delivered)
                    if fallback_delivered_any:
```

The successful-chunk branch:

```python
            else:
                for index, item in chunk:
                    delivered.add(index)
                    delivered_gundi_ids.append(str(item.gundi_id))
                _flush_progress(batch, destination_id, fp, delivered)
                throttling.record_success(destination_id=destination_id, stream_type=stream_type)
```

Leave the transient branch's `record_distress` / `_publish_batch_delivered` / `raise` sequence unchanged — it deliberately does **not** flush, because every previously completed chunk already did.

- [ ] **Step 7: Run the full suite**

Run: `pytest -v`
Expected: PASS. The pre-existing `test_process_observations_batch_posts_one_bulk_request` and `test_batch_skips_already_delivered_items` assert on per-item `dispatched_observation.*` writes, which no longer happen — update those two to assert on `_progress_setex_calls` instead, matching the new tests. Do not weaken them; they should assert the same delivery outcomes.

- [ ] **Step 8: Commit**

```bash
git add core/event_handlers.py tests/test_process_observation_batches_v2.py
git commit -m "feat: dedup batch redelivery per envelope instead of per observation"
```

---

### Task 5: Document the mechanism, env vars, and rollout runbook

**Files:**
- Modify: `CLAUDE.md` (the `### Redis usage (core/utils.py)` section at ~line 79, the `### Key modules` list at ~line 91, and `## Key env vars` at ~line 109)
- Create: `docs/rollout-envelope-dedup.md`

**Interfaces:**
- Consumes: everything from Tasks 1-4.
- Produces: no code.

- [ ] **Step 1: Update `CLAUDE.md`**

Retitle the Redis section to `### Redis usage (core/utils.py, core/batch_progress.py)` and add:

```markdown
- **Batch idempotency** (`core/batch_progress.py`): ONE key per envelope,
  `batch_progress.{batch_id}.{destination_id}.{sha256(provider_key)[:8]}`, whose
  value is an 8-byte item-sequence fingerprint followed by a delivered-item
  bitmap. TTL `DISPATCHED_BATCH_PROGRESS_CACHE_TTL` (90000s) must exceed
  `MAX_EVENT_AGE_SECONDS`. This replaced one `dispatched_observation.*` key per
  observation, which filled prod Redis on 2026-08-05 (33.7M keys / 10 GB).
  `provider_key` is part of the key because cdip-routing groups items per
  `(destination, effective_provider_key)`.
- The fingerprint exists because cdip-routing can re-publish the same
  `batch_id` with a different item list; a mismatch fails open and re-posts.
- **The single-item path still writes per-observation keys** at
  `DISPATCHED_OBSERVATIONS_CACHE_TTL` (1h) — `get_dispatched_observation` needs
  the stored `external_id` to resolve `related_to` for attachments.
```

Add to `### Key modules`:

```markdown
- `core/batch_progress.py` — per-envelope redelivery-dedup records (pure
  encode/decode plus never-raising Redis accessors)
```

Add to `## Key env vars`:

```markdown
- `DISPATCHED_BATCH_PROGRESS_CACHE_TTL` (default 90000) — batch dedup record
  lifetime; must exceed `MAX_EVENT_AGE_SECONDS`
- `BATCH_DEDUP_LEGACY_FALLBACK_ENABLED` (default true) — transitional; read
  legacy per-item keys when an envelope has no progress record
```

- [ ] **Step 2: Write the rollout runbook**

Create `docs/rollout-envelope-dedup.md`:

```markdown
# Rollout: envelope-scoped batch dedup

Ordering matters. Purging `dispatched_observation.*` before step 4 breaks the
legacy fallback and recreates the duplicate burst it exists to prevent.

1. **Scale prod Redis up.** Independent of this change and more urgent:
   `sintegrate-4bb07e9c` (project `cdip-prod1-78ca`, region `us-central1`) is a
   10 GB instance that hit 100%. Size for peak observation rate x 25h until this
   ships, then it can come back down.
2. **Deploy** with `BATCH_DEDUP_LEGACY_FALLBACK_ENABLED=true` (the default).
   Check `dedup_source` in traces, but expect `none` to dominate — a brand-new
   envelope's *first* delivery has no progress record yet, so `none` is the
   healthy steady-state value, not a sign of failure. `batch_progress` appears
   only on redelivery of an envelope that already flushed progress once, and
   `unusable_record` only on a fingerprint mismatch or truncated record (rare;
   worth investigating if it is not). `legacy` should appear only for
   envelopes published before the deploy. The signal that actually matters
   here is **`legacy` decaying to zero over the 25h window** — that is what
   confirms every pre-deploy envelope has drained, not the presence of
   `batch_progress`.
3. **Wait at least 25h** — longer than `MAX_EVENT_AGE_SECONDS` (86400), so no
   envelope predating the deploy can still be redelivered.
4. **Set `BATCH_DEDUP_LEGACY_FALLBACK_ENABLED=false`** and redeploy. Confirm
   `dedup_source` is never `legacy`.
5. **Optionally purge leftovers.** `dispatched_observation.*` keys expire on
   their own within 25h of their last write; `scripts/redis_stale_key_cleaner.py`
   in the `cdip` repo (branch `backup/redis-prod-cleanup-prerebase`) can UNLINK
   them sooner.
6. **Scale Redis back down** once db3 is at its new steady state (~0.1-0.2 GB).
7. **Follow-up PR:** delete `_legacy_delivered_indices`,
   `BATCH_DEDUP_LEGACY_FALLBACK_ENABLED`, `utils.mark_observation_dispatched`,
   and `DISPATCHED_OBSERVATIONS_BATCH_CACHE_TTL`.

## Verifying in prod

Profile the keyspace read-only from a portal pod (Memorystore has no public
route and `CONFIG GET` is disabled):

    kubectl --context gundi-prod -n application exec -i <admin-portal-pod> \
      -c admin-portal -- python3 - < script.py

db3 is the ER dispatcher's keyspace. Expect `batch_progress.*` to dominate and
`dispatched_observation.*` to shrink to zero over 25h.
```

- [ ] **Step 3: Commit**

```bash
git add CLAUDE.md docs/rollout-envelope-dedup.md
git commit -m "docs: document envelope-scoped batch dedup and its rollout"
```

---

## Self-Review

**Spec coverage:** Problem/Goals → Tasks 1-4. Non-goals respected: no task touches the single-item path, events, or attachments. Design §1 module → Task 1+2. §2 key → Task 1 (`progress_key`, with the provider_key rationale in tests and comments). §3 value → Task 1 (`encode`/`decode`). §4 read path → Task 4 Step 5, including the `dedup_source` span attribute and the preserved "everything already delivered" publish. §5 write path → Task 4 Step 6, including the no-op-when-empty rule (Task 2) and per-chunk flush. §6 rollout → Task 3 (flag), Task 4 Step 4 (`_legacy_delivered_indices`), Task 5 Step 2 (runbook with the purge-ordering warning). §7 config → Task 3. §8 impact → Task 5 docs. Accepted trade-offs: fingerprint mismatch, Redis errors, missing/truncated values, empty batch, and 4xx fallback each have a named test in Task 1 or 4. The concurrency race is documented as accepted, with the Lua OR-merge listed as a follow-up — no task, by design. Testing section → every listed case maps to a test in Task 1, 2, or 4.

**Placeholder scan:** No TBD/TODO. Every code step contains runnable code, and the one fragile coupling in the test helpers (`_progress_value`'s gundi_ids must match `_make_batch_request`'s) is called out explicitly where it matters, because drift there degrades tests silently instead of failing loudly.

**Type consistency:** `fingerprint`/`encode`/`decode`/`progress_key` signatures are identical across Tasks 1, 2, and 4. `write_progress` is called with keyword args matching its definition (`fp`, `delivered`, `n`, `ttl`). `chunk` becomes `list[tuple[int, item]]` in Task 4 and every consumer (`dispatcher.send`, the fallback loop, the success loop) is updated to unpack it. `delivered` is a `set[int]` throughout. `_flush_progress` and `_legacy_delivered_indices` are used only where defined.

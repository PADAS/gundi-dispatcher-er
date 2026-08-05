# Batch Envelope Redelivery Dedup: O(envelopes) Instead of O(observations)

Status: approved, ready for implementation planning
Date: 2026-08-05

## Problem

The batch delivery path writes one Redis key per observation per destination:

```
dispatched_observation.{gundi_id}.{destination_id}   ->  "1"   TTL 90000s (25h)
```

Resident keys are therefore `observation_rate x 25h`, which has no bounded
steady state that fits the instance. On 2026-08-05 prod Redis
(`sintegrate-4bb07e9c`, 10 GB, project `cdip-prod1-78ca`) filled completely:

- db3 held **33.7M keys / 10.00 GB of 10 GB**, 99.9% `dispatched_observation.*`,
  averaging 223 bytes per key, TTL histogram peaking in the 24-26h bucket.
- **96% of those keys belonged to one destination** — `89647e08-fd7a-4e97-9059-d0a75b672d56`
  ("Wildlife Dynamics - Gundi Service Account"), whose topic went from ~400 to
  ~800K messages/day on 2026-08-04.
- Growth ran at ~1.7M keys/hour (~470/s) with no plateau; projected steady state
  is ~42M keys / ~9.4 GB for db3 alone.
- At 100% with `maxmemory-policy=volatile-lru`, Redis evicts the very
  idempotency keys the 25h TTL exists to protect — so dedup is already
  degraded during saturation, and the dispatcher's ER token cache (also db3,
  also volatile) is evictable too.

The 25h TTL itself is correct and must not be shortened: it has to exceed
prod's `MAX_EVENT_AGE_SECONDS=86400`, or a PubSub redelivery arriving after
expiry re-posts delivered items (ER 409s plus throttling distress). The defect
is the *cardinality*, not the lifetime.

Two things are explicitly **not** the cause, and were ruled out by inspection:
the `ef7e398` TTL-env-var fix (prod sets neither `PORTAL_CONFIG_OBJECT_CACHE_TTL`
nor `DISPATCHED_OBSERVATIONS_CACHE_TTL`, so the value was the 3600 default both
before and after), and ER token caching (one key per destination, ~100 total).
The `5b93c84` sentinel fix did help — 223 B observed versus ~380 B for the full
`DispatchedObservation` JSON — but a 2-2.5x saving cannot absorb a 25x TTL.

## Goals

- Make redelivery dedup cost `O(envelopes)` in both memory and Redis commands.
- Preserve the existing idempotency guarantee for redelivery of the *same*
  envelope, which is what PubSub's at-least-once actually produces.
- Keep the 25h lifetime and its relationship to `MAX_EVENT_AGE_SECONDS`.
- Deploy without a duplicate burst from envelopes in flight at deploy time.
- Never let the dedup layer raise; a broken cache must not stall the stream.

## Non-goals

- The single-item path. `cache_dispatched_observation` keeps writing
  per-observation keys at the 1h TTL: `get_dispatched_observation` needs the
  stored `external_id` to resolve `related_to` for attachment delivery. Events
  and attachments are untouched.
- Globally-scoped per-observation dedup. Suppressing a duplicate that arrives
  in a *different* envelope is dropped as a requirement (see "Accepted
  trade-offs").
- Today's saturation. Scaling the instance is a separate, more urgent operational
  action; this design fixes the steady state.
- The Wildlife Dynamics volume itself. Resolved 2026-08-05: it is intentional,
  and it is a large backfill rather than sustained traffic — so ~470 keys/s is a
  recurring-but-transient peak, not a new baseline. Nothing to chase.

## Design

### 1. New module `core/batch_progress.py`

`core/utils.py` is already ~450 lines with mixed responsibilities, so the new
logic lands in its own module. The encode/decode/fingerprint functions are pure
so the interesting logic is testable without touching Redis.

```
progress_key(batch_id, destination_id, provider_key) -> str
fingerprint(items) -> bytes                       # pure
encode(fingerprint, delivered, n) -> bytes        # pure
decode(raw, expected_fingerprint, n) -> set[int]  # pure
read_progress(batch_id, destination_id, provider_key) -> bytes | None
write_progress(batch_id, destination_id, provider_key, fingerprint, delivered, n, ttl) -> None
```

`read_progress` / `write_progress` wrap `utils._cache_db` and swallow every
exception (logging a warning), mirroring `is_observation_dispatched`.

### 2. The key

```
batch_progress.{batch_id}.{destination_id}.{sha256(provider_key)[:8]}
```

`provider_key` **must** participate: `cdip-routing` groups transformed items per
`(destination, effective_provider_key)` because field mappings can override the
provider key per item and one ER bulk post allows exactly one key in its URL
path. One `batch_id` + `destination_id` therefore legitimately yields several
envelopes with disjoint item sets, and omitting `provider_key` would collide
them.

It is hashed rather than embedded literally because it is a free-form string
that may contain `.` or `:`, which would break prefix-bucketed key analysis
(`scripts/redis_ops_common.key_prefix` splits on exactly those characters) and
make the key length unbounded.

### 3. The value

A single binary string written with `SET ... EX <ttl>`:

```
bytes 0..7   fingerprint = sha256(len(items) || len(id_0) || id_0 || len(id_1) || id_1 || ...)[:8]
bytes 8..    bitmap; bit i set  <=>  batch.items[i] was delivered
```

The fingerprint is length-prefixed, not delimiter-joined: the item count as a
4-byte big-endian integer, then each `gundi_id`'s byte length as a 4-byte
big-endian integer followed by its bytes. Every length field is fixed-width,
including the count — a decimal count would itself be variable-length and would
leave injectivity resting on a side argument about the maximum size of a single
`gundi_id`.

`gundi_id` is `Union[UUID, str]` in `gundi_core`, so a non-UUID string is
schema-legal and may itself contain `|` — a delimiter join lets two different
item lists (e.g. `["a|b", "c"]` and `["a", "b|c"]`) hash identically, and a
collision here means `decode()` reports a match against the wrong item list:
a bit gets read as "delivered" for an observation that was never sent. That is
the one outcome this design forbids. Fixed-width length prefixing is a uniquely
decodable code, so two different ordered `gundi_id` sequences never produce the
same bytes to hash.

An injective encoding is not a collision-free digest, though: the fingerprint
truncates SHA-256 to 8 bytes, so distinct inputs can still collide at ~2^-64.
Why 8 bytes is enough is a matter of **scope**, not of digest width — a
fingerprint is only ever compared against records stored under the same
`(batch_id, destination_id, provider_key)` key, and only a handful of item lists
ever exist for one key inside its 25h lifetime. The exposure is therefore ~2^-64
per comparison, not a birthday problem across ~800K envelopes/day. Widen
`FINGERPRINT_BYTES` if that scoping assumption ever stops holding.

Bit `i` indexes into the **received** `batch.items` list. The fingerprint binds
the bitmap to the exact item-identity sequence it was computed against, which is
what makes positional indexing safe (see "Accepted trade-offs").

Size for a ~50-item envelope: 8 + 7 = 15 bytes of value, ~85 bytes of key name,
~50 bytes of Redis overhead — about **150 B per envelope**, against
50 x 223 B = ~11 KB today.

### 4. Read path

Replaces the N-way `is_observation_dispatched` filter in
`dispatch_observations_batch_v2` with one `GET`:

```python
if not batch.items:
    return  # empty batches parse as a no-op (see gundi_core/events/batches.py)

fp = batch_progress.fingerprint(batch.items)
raw = batch_progress.read_progress(batch.batch_id, destination_id, batch.provider_key)
delivered = batch_progress.decode(raw, fp, n=len(batch.items))
if not delivered and settings.BATCH_DEDUP_LEGACY_FALLBACK_ENABLED:
    delivered = _legacy_delivered_indices(batch, destination_id)   # see section 6
pending = [(i, item) for i, item in enumerate(batch.items) if i not in delivered]
```

`current_span` gains a `dedup_source` attribute — `batch_progress` (record
present and usable), `unusable_record` (a record was present but `decode()`
couldn't use it: fingerprint mismatch or truncation — the one condition that
causes a full-envelope duplicate re-post), `legacy` (no usable record, legacy
per-item keys found one), or `none` (no record at all — the common case for a
brand-new envelope's first delivery) — so the rollout can be observed in
traces. `raw` is kept around specifically so `unusable_record` can be told
apart from `none`; without it, decode's empty set alone can't distinguish
"nothing was ever recorded" from "something was recorded but is unusable."

The existing "everything already delivered" branch is preserved verbatim: when
`pending` is empty it still publishes `ObservationsBatchDelivered` for **all**
items, because the original attempt may have died after recording progress and
before publishing.

### 5. Write path

Progress flushes **once per chunk**, never per item. This is load-bearing: the
transient-error branch raises to nack the envelope, so progress for
already-delivered chunks must be durable *before* that raise, or redelivery
re-posts them.

```python
for chunk in _chunked(pending, settings.ER_BULK_SIZE):
    await dispatcher.send([item.observation for _, item in chunk])
    delivered.update(i for i, _ in chunk)
    batch_progress.write_progress(..., fp, delivered, len(batch.items),
                                  ttl=settings.DISPATCHED_BATCH_PROGRESS_CACHE_TTL)
```

The per-item 4xx fallback loop sets bits only for items that succeeded
individually, then flushes once after the loop. Items that failed individually
keep their bit unset — identical to today, since those items are not cached
now either. This is *not* "retried on redelivery": the 4xx fallback path acks
the envelope and publishes `ObservationDeliveryFailed` for each item that
failed individually, so there is no redelivery for the unset bit to be
retried on. The bit is simply accurate bookkeeping of what was never
delivered.

`write_progress` is a no-op when `delivered` is empty. An all-zero record
carries no information and would be indistinguishable from a missing one, so
never writing it keeps `decode`'s "empty set" result unambiguous: it always
means "nothing known to be delivered", whether the record is absent or
unusable. The fallback loop can reach its flush having delivered nothing (every
item failed individually), which is the case this rule covers.

`_cache_item_as_dispatched` is deleted. `mark_observation_dispatched` in
`core/utils.py` becomes unused once the legacy fallback is removed (section 6)
and is deleted with it.

### 6. Rollout: dual-read for one 25h window

At deploy there are up to `MAX_EVENT_AGE_SECONDS` (24h) of in-flight envelopes
whose items were already delivered and recorded as per-item keys. New code would
find no `batch_progress` key and re-post all of them — a duplicate burst exactly
when the system is shedding load.

For one window, a missing progress record falls back to the legacy per-item
check:

```python
def _legacy_delivered_indices(batch, destination_id):
    return {
        i for i, item in enumerate(batch.items)
        if is_observation_dispatched(gundi_id=str(item.gundi_id),
                                     destination_id=destination_id)
    }
```

The batch path **only ever writes the new format**. No new per-item key is
created, so the existing 33.7M keys drain by TTL and the population strictly
shrinks.

Cost during the window: envelopes without a progress record still pay N GETs,
including on the *first* delivery of a brand-new envelope. That is exactly
today's read cost, so the window is neutral-to-better, and the memory win starts
immediately.

Sequence, and the ordering matters:

1. Scale the instance up (independent of this change; do it now).
2. Deploy with `BATCH_DEDUP_LEGACY_FALLBACK_ENABLED=true`.
3. Wait at least 25h.
4. Set `BATCH_DEDUP_LEGACY_FALLBACK_ENABLED=false`.
5. Optionally `UNLINK` leftover `dispatched_observation.*` keys.
6. Scale the instance back down.
7. Follow-up PR: delete the fallback, `mark_observation_dispatched`, and
   `DISPATCHED_OBSERVATIONS_BATCH_CACHE_TTL`.

**Do not purge `dispatched_observation.*` before step 4** — the fallback reads
those keys, and purging early reintroduces the duplicate burst it exists to
prevent.

### 7. Configuration

| Setting | Default | Notes |
|---|---|---|
| `DISPATCHED_BATCH_PROGRESS_CACHE_TTL` | `90000` | Must exceed `MAX_EVENT_AGE_SECONDS` (86400). Same rationale as the TTL it replaces. |
| `BATCH_DEDUP_LEGACY_FALLBACK_ENABLED` | `true` | Flip to `false` after >=25h in prod, then delete. |
| `DISPATCHED_OBSERVATIONS_BATCH_CACHE_TTL` | `90000` | Becomes unused the moment this ships — the fallback only *reads* legacy keys and writes none. Deleted in the follow-up PR alongside the fallback. |

### 8. Expected impact

| | Today | After |
|---|---|---|
| Keys resident | 33.7M (heading for ~42M) | ~830K |
| db3 memory | ~9.4 GB projected steady state | ~0.1-0.2 GB |
| Redis reads per envelope | N `GET` | 1 `GET` |
| Redis writes per envelope | N `SETEX` | ~1 `SET` per chunk |

At the ~50 items per envelope implied by current traffic (~40M keys/day against
~800K envelopes/day), that is a ~50x reduction per envelope in both keys and
commands, a ~40x drop in resident keys against today's saturated figure, and a
~50-90x reduction in memory.

## Accepted trade-offs

Every failure mode fails open — `delivered` becomes empty, items are re-posted,
and nothing raises. Duplicates are the failure currency; lost observations are
never acceptable.

| Condition | Behaviour |
|---|---|
| Fingerprint mismatch | `delivered = {}`, re-post all N. Duplicates, no loss. |
| Same envelope delivered concurrently to two workers | Both read-modify-write; last write wins, some bits lost, those items re-posted later. Duplicates, no loss. |
| Redis unavailable or any exception | `delivered = {}`. Matches `is_observation_dispatched`, which returns `False` on error. |
| Value missing, truncated, or shorter than `ceil(n/8)` | Absent bits count as not delivered. |
| `batch.items == []` | Early return, nothing written. |
| Item fails individually in the 4xx fallback | Bit unset. The envelope is acked and `ObservationDeliveryFailed` is published for that item — not retried on redelivery. Unchanged from today. |

Two of these deserve explicit justification.

**Why a fingerprint is required.** `transform_and_route_observations_batch` can
publish some groups and *then* raise, retrying the whole
`ObservationsBatchReceived`; and per-item transform failures `continue` to shrink
the batch rather than abort it. A transform failure that is transient can
therefore succeed on the retry, so the same `batch_id` may be re-published with
a *different item list or order*. Without the fingerprint, bit `i` from the first
attempt would be read as authoritative for a different observation in the
second — silently skipping an undelivered item. That is data loss, the one
outcome ruled out. The fingerprint converts it into a detectable mismatch that
fails open.

**Why the concurrency race is tolerated.** Today each item's `SETEX` is
independent, so nothing can be clobbered; a shared read-modify-write value
introduces a lost-update window that did not previously exist. A Lua OR-merge
would close it, but it only converts rare duplicates into rarer duplicates, and
duplicates on mismatch are already accepted. Noted as a follow-up should the
duplicate rate prove material in practice.

**Why per-envelope scoping is acceptable.** A `gundi_id` is assigned when Gundi
ingests an observation, so a re-pull produces new ids rather than resurfacing
old ones; the realistic duplicate scenario is redelivery or re-publication of
the same envelope, which keys on `batch_id` and is still covered. If a single
`gundi_id` ever did reach one destination through two distinct envelopes, it
would now be posted twice.

## Testing

The suite mocks Redis with `MagicMock` (`tests/conftest.py`'s `mock_cache_*`
fixtures); there is no `fakeredis` in this repo. Keeping encode/decode/fingerprint
pure means the bit-level logic needs no mocking at all.

**Pure functions**

- `fingerprint` is stable across identical id sequences, and differs on reorder,
  on an added item, and on a removed item.
- `encode` -> `decode` round-trips for n = 0, 1, 7, 8, 9, 200 (byte-boundary cases).
- `decode` returns `{}` for `None`, for a value shorter than 8 bytes, and for a
  mismatched fingerprint.
- `decode` treats a bitmap shorter than `ceil(n/8)` as having only the bits present.

**Handler behaviour**

- First delivery: one read, all bits set, TTL applied.
- Redelivery with a full bitmap: no ER post, but `ObservationsBatchDelivered` is
  still published for all items.
- Redelivery with a partial bitmap: only unset items posted.
- Transient failure mid-envelope: progress for completed chunks is persisted
  *before* the exception propagates.
- 4xx fallback: bits set only for individually-successful items.
- Fingerprint mismatch: every item re-posted.
- Legacy fallback enabled, no progress record, legacy per-item keys present:
  `pending` excludes those items; `dedup_source` is `legacy`.
- Legacy fallback disabled: no per-item reads occur.
- Redis raising on read, and on write: never propagates, delivery proceeds.

## Follow-ups

- Optional Lua OR-merge for the concurrent-redelivery race.
- Remove the legacy fallback, `mark_observation_dispatched`, and
  `DISPATCHED_OBSERVATIONS_BATCH_CACHE_TTL` after the window closes.
- Land `redis_memory_profiler.py` / `redis_stale_key_cleaner.py` on main in the
  `cdip` repo (currently only on `backup/redis-prod-cleanup-prerebase`).
- ~~Determine whether the Wildlife Dynamics volume is intentional or a runaway
  backfill.~~ Resolved 2026-08-05: intentional, and a large backfill rather than
  sustained volume. The sizing rule that follows is to size for peak *backfill*
  rate x TTL, not for average traffic — which is exactly what this design makes
  cheap: the same backfill costs ~0.12 GB here versus ~9.4 GB projected under the
  per-observation scheme, so a backfill of this size stops being a capacity event
  and the instance needs no permanent oversizing.

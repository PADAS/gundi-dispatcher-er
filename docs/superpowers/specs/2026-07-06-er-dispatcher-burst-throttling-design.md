# Design: Per-destination burst throttling in the shared ER dispatcher

**Date:** 2026-07-06
**Status:** Approved for planning
**Repos affected:** `gundi-dispatcher-er` (primary), `er-client` (companion), routing publisher (recommendation only)

## Problem

The deployment scheme for ER dispatchers changed: instead of one small Cloud Function
per destination (`max-instances=1`, `concurrency=4`), a **limited number of shared
dispatchers** now consume **shared PubSub topics** carrying traffic for many
destinations, via **push subscriptions**, with much higher concurrency limits.

Consequences:

1. **Per-destination isolation is gone.** A burst for one destination competes with
   all others inside the same subscription, and nothing bounds how much of the shared
   pool's concurrency lands on a single EarthRanger site at once.
2. **The old implicit rate cap is gone.** `max-instances=1 × concurrency=4` used to
   crudely bound per-site request rates; the shared pool can now hammer a small
   self-hosted ER site with the full pool's parallelism.
3. Nothing honors ER's distress signals: a 429/`Retry-After` or a run of 502/503/504
   today just produces per-message PubSub retries at whatever rate the backlog drives.

Goal: be kind to destinations (respect standing rate caps and back off when a site
signals distress) while draining streams efficiently and without recreating the
false-alarm problem solved in `2026-07-05-transient-er-delivery-errors-design.md`.

## Decision

**Approach A — Redis admission gate + fast-nack** (chosen over hold-and-pace and
Cloud Tasks; see Alternatives), **scoped per destination AND per stream-type
family** — events are the primary rate-limit culprit, and a blended budget would
let an event burst starve observations and messages that hit different, cheaper
ER endpoints:

- A **per-destination, per-family admission gate** (cooldown check + fixed-window
  token bucket in Redis) runs before any ER work; over-cap or cooling-down messages
  are **nacked** (HTTP 429 → PubSub redelivers with its existing 10s–600s backoff).
- A **distress cooldown** is set when ER returns 429/502/503/504 or is unreachable,
  with exponential TTL, honoring `Retry-After` once erclient exposes it. A 429 cools
  only the family that triggered it; 5xx/transport failures cool the whole site.
- A **grace-wait hybrid** sleeps briefly instead of nacking when the rate window
  opens within ~2s, reducing redelivery churn for near-cap traffic.
- **Deferrals are silent** — no failure events, no activity logs — preserving the
  transient-error/alarm work.

## Stream-type families

Throttle state is keyed by `(destination_id, family)`, where family is derived from
the message's `stream_type` attribute (available at the gate without any lookup):

| Family | Stream types | ER surface | Default cap |
|---|---|---|---|
| `events` | `ev`, `evu`, `att` | activity/events API (heavy: event creation, updates, attachments post to events) | 120/min |
| `observations` | `obv` | sensors API (cheap, bulk-tolerant) | 300/min |
| `messages` | `txt` | messages API (low volume) | 60/min |

Unknown or missing `stream_type` maps to `events` (the conservative family); such
messages are typically dead-lettered downstream as unknown types anyway.

## Component 1 — Admission gate

**Where:** new module `core/throttling.py`; invoked in
`core/services.py::process_request` for v2 messages that carry a
`destination_id` attribute, **after the age gate and before any portal config is
fetched or dispatch begins**. The order matters: the age check must run first so a
message that exhausts its 24h budget while its destination is throttled still
dead-letters (with the ERROR event) instead of being nacked until PubSub's 7-day
retention drops it silently. (v1 messages bypass the gate — deprecated path, low
volume.)

**Logic (single atomic Redis Lua script, one round trip; `family` derived from
`attributes.stream_type` per the table above):**

1. If `throttle:cooldown:{destination_id}:site` exists (site-wide distress) OR
   `throttle:cooldown:{destination_id}:{family}` exists → **DEFER** with
   `retry_after = TTL(key)`. This is the fast path that protects a struggling
   site: ~1ms, ER never touched.
2. Else `INCR throttle:rate:{destination_id}:{family}:{epoch_minute}` (set
   `EXPIRE 120` on first increment). If the counter ≤ the family's cap → **ADMIT**;
   else → **DEFER** with `retry_after = seconds until next minute window`.

Families are independent at the window level: an event burst exhausting the
`events` budget never defers observations or messages for the same destination.

**Grace-wait hybrid:** if DEFER came from the rate window (not a cooldown) and
`retry_after ≤ THROTTLE_GRACE_WAIT_MAX_SECONDS` (default 2), `asyncio.sleep(retry_after)`
and re-run the script once. Admitted on the retry → proceed; still over → DEFER.
Never grace-wait on cooldown deferrals.

**Nack semantics:** on DEFER, `main.py::main` returns **HTTP 429** with a short body
(e.g. `{"status": "throttled", "destination_id": ...}`). Any non-2xx nacks the push
message; 429 is chosen so the intent is self-documenting in request logs. PubSub
redelivers per the subscription's retry policy (min 10s, max 600s backoff).

**Deferrals are silent:** log at INFO with destination_id and reason
(`cooldown`/`rate`), no `ObservationDeliveryFailed`, no activity log. Repeated
deferral consumes the message's `MAX_EVENT_AGE_SECONDS` (24h) budget; a destination
that stays saturated or down for 24h dead-letters through the existing path,
which now publishes an ERROR-level event — that remains the "we gave up" alarm.

**Cap resolution (hot path must never call the portal):**

- Defaults per family: `DEFAULT_MAX_EVENT_DELIVERIES_PER_MINUTE` (**120**),
  `DEFAULT_MAX_OBSERVATION_DELIVERIES_PER_MINUTE` (**300**),
  `DEFAULT_MAX_MESSAGE_DELIVERIES_PER_MINUTE` (**60**).
- Per-destination, per-family override: an optional integer read from the destination
  integration's config **only via the existing Redis config cache** (the dispatcher
  already caches integration details with a 60s TTL). Cache miss → use the env
  default for this message; the normal processing path refreshes the cache. The
  exact config field/location on the Integration record is settled at planning time
  (it must be editable in the portal admin, near the ER `AUTHENTICATE` config).

**Kill switch:** `THROTTLING_ENABLED` env var, default **false**. First deploy ships
dark; enable per environment after observing gate metrics in logs.

## Component 2 — Distress cooldown

**Where:** the exception handler in
`core/event_handlers.py::dispatch_transformed_observation_v2` (the same place that
publishes `ObservationDeliveryFailed`/`ObservationUpdateFailed`), plus the success
path for reset.

**Trigger and scope — the two distress signals differ in blast radius:**

- HTTP **429** (`status_code == 429`) is endpoint-level rate limiting → set
  `throttle:cooldown:{destination_id}:{family}` for the family of the failed
  message only. Events keep backing off while observations and messages flow.
- HTTP **502/503/504** (`ERClientServiceUnreachable`) or a connection/timeout
  failure (erclient wraps `httpx.RequestError` as `ERClientException` with message
  prefix `"Request to ER failed"` and no status) is gateway/site-wide distress →
  set `throttle:cooldown:{destination_id}:site`, which the gate checks for every
  family.

**Explicitly excluded: 409.** erclient maps 409 → `ERClientRateLimitExceeded`, but ER
uses 409 as a *per-source* rate limit ("one observation per second per source").
Pausing an entire destination because one source is hot would over-throttle; 409s
keep today's behavior (PubSub retry; WARNING in the portal). Discriminate by
`status_code`, not exception class.

**TTL — exponential with reset (levels tracked per scope):**

- Level keys `throttle:cooldown_level:{destination_id}:{family}` and
  `...:site` (each with its own TTL ~15 min).
- Cooldown TTL = `min(THROTTLE_COOLDOWN_BASE_SECONDS × 2^level, THROTTLE_COOLDOWN_MAX_SECONDS)`
  (defaults 30s base, 600s max); each trigger increments its scope's level.
- A **successful delivery** deletes the `site` cooldown/level keys (the site is
  demonstrably reachable) plus the delivered message's own family keys. Other
  families' cooldowns are left to expire on their own — a flowing observation says
  nothing about the events endpoint's rate limiter.

**`Retry-After` (companion change in er-client) — superseded, see the
SUPERSEDED DETAILS addendum at the top (shipped as er-client 1.16.0):** verified that erclient
**v1.15.0 (latest, released 2026-03-20) does not capture the header** — exceptions
carry only `message`/`status_code`/`response_body`. Companion PR to er-client: parse
`Retry-After` (both seconds and HTTP-date forms) in `_handle_http_status_error` and
attach it as `retry_after: Optional[int]` on `ERClientRateLimitExceeded` and
`ERClientServiceUnreachable`. When present, the dispatcher uses it as the cooldown
TTL (clamped to `THROTTLE_COOLDOWN_MAX_SECONDS`) instead of the exponential default.
The dispatcher change is written defensively (`getattr(e, "retry_after", None)`) so
it works with or without the upgraded client.

**The attempt that trips a cooldown still publishes its failure event** exactly as
today (recorded as WARNING in the portal per the transient-errors work). The
cooldown only prevents *subsequent* attempts from reaching ER.

## Component 3 — Visibility without alarms

When a destination **enters** cooldown in any scope (SETNX on a notify key with
~5 min TTL to rate-limit, one notify key per destination), publish one
`DispatcherCustomLog` event (INFO level) to `DISPATCHER_EVENTS_TOPIC`, naming the
scope: "Event deliveries to this destination are temporarily deferred (rate
limited)" / "Deliveries to this destination are temporarily deferred (destination
unreachable or overloaded)". The portal already handles
`DispatcherCustomLog` and INFO never counts toward health thresholds. This answers
"why is my data delayed" in the activity log without tripping alarms or email.

## Component 4 — erclient pin upgrade (superseded: pin became `earthranger-client==1.16.0` from PyPI, see addendum)

Bump `requirements.in` from the v1.8.0 wheel to **v1.15.0** (and regenerate
`requirements.txt`). Verify at planning/implementation time that the 1.8.0→1.15.0
changes (API-version support, added methods) don't alter the call signatures the
dispatcher uses (`post_report`, `patch_report`, `post_report_attachment`,
`post_sensor_observation`, `post_message`, `close`). Once the companion er-client
PR ships a release with `retry_after`, a later pin bump picks it up — not a blocker
for this feature.

## Ordering recommendation (recorded here; implemented in the routing publisher, not this repo)

For the shared topic(s): if ordering is desired, use **ordering keys per `gundi_id`**.
Semantic ordering only matters within one object's lifecycle (event → update →
attachment); such keys are tiny, so PubSub's redeliver-everything-behind-a-nacked-key
behavior is harmless. Destination- or source-level ordering keys would turn every
throttle-nack into head-of-line blocking for an entire stream — do not use them with
this design. Ordering-off is also acceptable: early arrivals already self-heal via
`ReferenceDataError` retries.

## Failure modes

| Failure | Behavior |
|---|---|
| Redis unavailable | Gate **fails open** (admit; log warning) — matches the existing `*_cache_safe` pattern; system degrades to today's behavior |
| Lua/script error | Same fail-open path |
| `THROTTLING_ENABLED=false` | Gate is a no-op; cooldown hook still records nothing (fully dark) |
| Destination saturated > 24h | Messages age out → DLQ + ERROR event (existing backstop; guaranteed by running the age gate before the admission gate) |
| Clock-window edge (bursts at minute boundary) | Fixed windows admit ≤ 2× cap across a boundary worst-case — acceptable; caps are kindness bounds, not SLAs |

## Settings summary (all env vars, `core/settings.py`)

| Variable | Default | Purpose |
|---|---|---|
| `THROTTLING_ENABLED` | `false` | Kill switch; ship dark |
| `DEFAULT_MAX_EVENT_DELIVERIES_PER_MINUTE` | `120` | Standing cap, `events` family (`ev`/`evu`/`att`) |
| `DEFAULT_MAX_OBSERVATION_DELIVERIES_PER_MINUTE` | `300` | Standing cap, `observations` family (`obv`) |
| `DEFAULT_MAX_MESSAGE_DELIVERIES_PER_MINUTE` | `60` | Standing cap, `messages` family (`txt`) |
| `THROTTLE_GRACE_WAIT_MAX_SECONDS` | `2` | Max in-request sleep before nacking |
| `THROTTLE_COOLDOWN_BASE_SECONDS` | `30` | First cooldown TTL |
| `THROTTLE_COOLDOWN_MAX_SECONDS` | `600` | Cooldown ceiling (also clamps Retry-After) |

## Testing

All Redis and PubSub mocked, matching suite conventions (`tests/conftest.py`):

- Gate: admit under cap; defer over cap; defer immediately when a family or site
  cooldown key exists; grace-wait sleeps and admits when the window is near;
  grace-wait not applied to cooldowns; fail-open when Redis raises; no-op when
  `THROTTLING_ENABLED=false`.
- Family isolation: events over cap does NOT defer observations/messages for the
  same destination; `ev`/`evu`/`att` share the `events` budget; unknown
  stream_type maps to `events`.
- `main.py`: DEFER → HTTP 429 response; no failure event published; message not
  processed.
- Cooldown hook: 429 sets the failed message's family cooldown only (other
  families still admitted); 503 and "Request to ER failed" set the site cooldown
  (all families deferred); **not** set on 409 or 400; TTL escalates across
  consecutive failures and is clamped; a successful delivery clears the site scope
  and its own family scope but not other families; `retry_after` attribute wins
  when present.
- Custom log: published once on cooldown entry, rate-limited on repeat entries.
- Regression: admitted messages flow through the existing dispatch path unchanged
  (existing suite must stay green).

## Rollout

1. Land dispatcher changes; deploy with `THROTTLING_ENABLED=false` (behavior
   identical to today).
2. Enable in a staging/low-traffic environment; watch INFO gate logs and PubSub
   redelivery metrics; tune the three per-family default caps (events first — it's
   the culprit family).
3. Enable in production; set per-destination overrides for known-small ER sites.
4. er-client `Retry-After` PR proceeds in parallel; pin bump when released.

## Alternatives considered

- **Hold-and-pace (sleep instead of nack):** smoother, near-FIFO, but holds shared
  instance capacity during exactly the bursts being managed, pressures the
  autoscaler, and risks ack-deadline expiry → duplicates. Rejected as the primary
  mechanism; its cheap half is kept as the ≤2s grace-wait.
- **Cloud Tasks queue per destination:** native rate/concurrency limits and precise
  drain, but new infrastructure (queue lifecycle per destination, quotas, second
  delivery path). Rejected per Redis-only constraint.
- **Delay topic + re-publish:** keeps PubSub-only but means owning a custom
  scheduler; more moving parts than nacking for little gain at current scale.
- **Destination-level ordering keys as a throttle:** superficially attractive
  (PubSub serializes per key) but converts every nack into stream-wide head-of-line
  blocking and redelivery storms. Rejected; see Ordering recommendation.

## Open items for the implementation plan

- Exact portal-side location of the per-destination, per-family cap overrides
  (integration config section/field names) and whether they ship in phase 1 or
  env-defaults-only.
- Confirm `main.py`'s functions-framework response plumbing for returning a 429
  (flask-style tuple) and how `process_request`'s DEFER result propagates.
- Confirm the Redis client in use (`walrus` wraps redis-py) exposes `eval`/
  `register_script` for the Lua gate; else fall back to a WATCH/MULTI or plain
  INCR+GET sequence with documented race tolerance.
- Verify erclient 1.8.0 → 1.15.0 upgrade compatibility for the six client methods
  the dispatchers call.

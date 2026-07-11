# Design: Don't alarm on transient EarthRanger delivery failures

**Date:** 2026-07-05
**Status:** Approved for planning
**Repos affected:** `cdip` (portal), `gundi-dispatcher-er` (this repo)

## Problem

EarthRanger's API often returns errors that are transient — server overload, temporary
outages, gateway timeouts. The ER dispatcher publishes an `ObservationDeliveryFailed`
event for every failed attempt and re-raises so GCP PubSub retries the message. The
retry usually succeeds, but each failed attempt has already been recorded as an
ERROR-level `ActivityLog` entry in the portal.

Gundi's health calculator (`cdip/cdip_admin/integrations/models/v2/services.py::calculate_integration_status`)
marks an integration UNHEALTHY when it sees ≥ `error_count_threshold` (default 3) ERROR
logs within `time_window_minutes` (default 60). It never considers whether delivery
eventually succeeded. The unhealthy status surfaces in the portal UI and in the
scheduled alert email (`integrations/tasks.py::send_unhealthy_connections_email`).

Two amplifiers make false alarms common:

1. **Retries are invisible.** The dispatcher's push subscriptions (created in
   `cdip/cdip_admin/deployments/tasks.py`) have a retry policy (10s–600s backoff) but no
   dead-letter policy, so PubSub does not populate `deliveryAttempt` and the dispatcher
   cannot distinguish a retry from a first attempt. Every redelivery of the *same*
   failing message emits a fresh failure event — up to ~6 ERROR logs per hour from a
   single stuck message, which alone crosses the default threshold.
2. **Reference-data waits are logged as failures.** Event updates and attachments that
   arrive before their parent observation raise `ReferenceDataError` ("Will retry
   later") and publish failure events, even though this is normal out-of-order delivery.

## Decision

Classify retriable failures at the **portal event consumer** so they are logged as
WARNING instead of ERROR (generalizing the existing ER-409 precedent), add a
**WARNING-volume safety net** to the health calculator so sustained outages still
alarm, and make the dispatcher emit a **final ERROR when it gives up** and
dead-letters a message.

Scope: the EarthRanger dispatcher path only (`origin=DISPATCHER` activity logs for ER
destinations). Other dispatcher types and provider-side (pull action) errors are
unchanged; the pattern can be extended later.

## Component 1 — Portal consumer: classify retriable failures as WARNING

**Where:** `cdip/cdip_admin/event_consumers/dispatcher_events_consumer.py`

Replace the current special case (`"pamdas.org" in error and status == 409` → WARNING)
with a shared helper applied in both `handle_observation_delivery_failed_event` and
`handle_observation_update_failed_event`.

A failure is **retriable** (log as WARNING) when the destination integration is an
EarthRanger type and either:

- `server_response_status ∈ {409, 429, 502, 503, 504}`, or
- `server_response_status` is absent and the error string matches a transient pattern.
  The dispatcher formats errors as `f"{type(e).__name__}: {e}"`, so exception class
  names are reliable markers:
  - erclient transient exceptions: `ERClientRateLimitExceeded`, `ERClientServiceUnreachable`
  - raw transport failures: `ClientConnectorError`, `ServerTimeoutError`,
    `ServerDisconnectedError`, `ClientOSError`, `TimeoutError`, `ConnectionResetError`
  - the dispatcher's reference-data messages containing `"Will retry later"`
    (an event update arriving before its parent event was delivered; early-arriving
    attachments raise `ReferenceDataError` without publishing a portal event, so they
    need no classification)

Everything else stays ERROR, deliberately including:

- **Plain 500 (`ERClientInternalError`)** — a deterministic 500 caused by a specific
  payload should still alarm. (Revisit if ER overload proves to surface as 500s.)
- **400/401/403/404** — bad request/auth/config problems are real and actionable.
- **v1 legacy events** — they carry no status code; current behavior is preserved.
- **Retries-exhausted events from Component 3.**

ER scoping uses the destination integration's type (via the `GundiTrace` destination),
not a URL substring, so self-hosted ER sites are covered. The activity-log `value`
fields (`observation_delivery_failed`, `observation_update_failed`) and log payloads
are unchanged — only `log_level` varies.

## Component 2 — Health calculator: sustained-outage safety net

**Where:** `cdip/cdip_admin/integrations/models/v2/services.py::calculate_integration_status`
and `integrations/models/v2/models.py::HealthCheckSettings` (+ one migration)

With Component 1 alone, an ER site that is hard-down for hours would produce only
WARNINGs and never go unhealthy — silencing the one alarm that matters. Add a third
condition to the calculator:

- Count WARNING-level logs with `origin=DISPATCHER` and
  `value ∈ {observation_delivery_failed, observation_update_failed}` in the same time
  window. If the count ≥ a new `HealthCheckSettings.retriable_error_count_threshold`
  (PositiveIntegerField, **default 30**), set UNHEALTHY with details like
  `"Sustained delivery errors — destination may be down or overloaded"`.

Calibration: one stuck message retrying at max backoff (600s) generates ~6 failure
events per hour, so 30/hour ≈ 5+ messages failing concurrently — a real incident, not
a blip. Tunable per integration like the existing settings.

Ordering of checks: disabled → dispatcher deployment error → ERROR threshold →
INTEGRATION-origin ERROR threshold (existing) → new WARNING threshold → healthy.

## Component 3 — Dispatcher: ERROR when retries are exhausted

**Where:** `gundi-dispatcher-er/core/services.py::process_request` (too-old branch,
currently lines ~327-331)

Messages that keep failing are retried until their original publish time exceeds
`MAX_EVENT_AGE_SECONDS` (default 24h), at which point the dispatcher itself publishes
them to the per-stream-type DLQ topic and acks — today **silently**, with no portal
event.

Change: when dead-lettering a too-old **v2** message, also publish a failure event to
`DISPATCHER_EVENTS_TOPIC`:

- `ObservationUpdateFailed` for `stream_type == event_update`, else
  `ObservationDeliveryFailed`
- error text: `"Delivery retries exhausted (message older than MAX_EVENT_AGE_SECONDS);
  sent to dead-letter queue"`, no `server_response_status`

The portal classifier (Component 1) will not match this as retriable, so it lands as
one ERROR-level activity log — "we gave up on this message" becomes visible and counts
toward the unhealthy threshold. No gundi-core schema change and no new event type.

Out of scope for this component: the other DLQ paths (unsupported schema version,
unknown event type) are code/config defects rather than delivery failures and keep
current behavior; v1 messages keep current behavior. Failure to publish the event must
not prevent the DLQ send/ack (wrap and log).

## Behavior summary

| Scenario | Today | After |
|---|---|---|
| ER returns 503/502/504/429/409, retry succeeds later | ERROR per attempt → unhealthy + email | WARNINGs; stays healthy |
| Event update arrives before parent event ("Will retry later") | ERROR per attempt | WARNINGs; stays healthy |
| ER hard-down for hours | unhealthy | unhealthy (≥30 WARNINGs in window) |
| Message retried for 24h, then dead-lettered | silent (errors aged out of window) | one ERROR at DLQ time |
| Bad credentials / 400 / 404 / plain 500 | ERROR → unhealthy | unchanged |
| Non-ER destinations, v1 legacy events | ERROR | unchanged |

## Testing

**Portal (`cdip`):**
- Consumer tests: each retriable status code → WARNING; each transient string pattern
  → WARNING; "Will retry later" update-failed → WARNING; plain 500 / 401 / 400 → ERROR;
  non-ER destination with 503 → ERROR (scoping); v1 event → ERROR; retries-exhausted
  message → ERROR.
- Health-calc tests: WARNINGs below threshold → healthy; ≥ threshold → unhealthy with
  the new details string; interplay with the existing ERROR threshold; per-integration
  override of the new setting.

**Dispatcher (this repo):**
- Extend `tests/test_dead_lettering.py`: too-old v2 message publishes both the DLQ
  message and an `ObservationDeliveryFailed`; event-update variant publishes
  `ObservationUpdateFailed`; publish failure of the event does not block the DLQ send;
  v1 too-old message does not publish a failure event.

## Rollout

1. Portal deploy activates Components 1 and 2 immediately for **all existing ER
   dispatcher functions** — no fleet redeploy (this is why classification lives in the
   consumer rather than the dispatcher: there is one Cloud Function per destination).
2. Component 3 ships with the next dispatcher release cycle.
3. Tuning knobs, per integration, no code change: `error_count_threshold`,
   `time_window_minutes`, new `retriable_error_count_threshold`.

## Alternatives considered

- **Dispatcher-side classification** (add `is_retriable` to `DeliveryErrorDetails` in
  gundi-core): cleanest contract, but requires a gundi-core release plus redeploying
  every per-destination function before it has any effect. Noted as a future contract
  improvement.
- **Retry-aware health calculator** (correlate failures with later successes via
  `GundiTrace`): most faithful, but per-integration query complexity on a fleet-wide
  scheduled task, and a success for one observation doesn't prove another failing one
  is fine. Rejected for now.
- **ERROR only when retries exhausted** (suppress intermediate attempts entirely):
  requires the dispatcher to know the attempt number, which PubSub only exposes
  (`deliveryAttempt`) when a dead-letter policy is set on the subscription. Adding a
  dead-letter policy in `deployments/tasks.py` is a worthwhile future direction (native
  retry caps + retry awareness) but changes retry semantics fleet-wide.

## Implementation notes / verifications for the plan

- Verify the ER integration type slug used for scoping (e.g. `earth_ranger`) against
  the `Integration.type` model in cdip.
- Verify the exact `value` field written by `handle_observation_update_failed_event`
  (assumed `observation_update_failed`).
- Verify which exception types `AsyncERClient` lets propagate for timeouts/connection
  errors (the transport-pattern list above) before finalizing the pattern list.
- The v2 attributes dict on the dispatcher side carries `gundi_id`, `related_to`,
  `data_provider_id`, `destination_id`, `stream_type` — sufficient to build the
  Component 3 event payloads without parsing the message body.

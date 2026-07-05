# Transient ER Delivery Errors Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop transient EarthRanger delivery failures (503/429/timeouts that succeed on retry) from marking Gundi connections unhealthy, while keeping real alarms: sustained outages and retries-exhausted messages.

**Architecture:** Three independent changes. (1) The portal's dispatcher-events consumer classifies retriable ER failures as WARNING instead of ERROR, generalizing an existing 409 special case. (2) The health calculator gains a high-threshold WARNING-volume check so a hard-down ER site still goes unhealthy. (3) The ER dispatcher publishes a final ERROR-level failure event when it dead-letters a message whose retries are exhausted (older than 24h).

**Tech Stack:** Django + Celery + pytest (`cdip` repo), functions-framework + pytest-asyncio (`gundi-dispatcher-er` repo), gundi-core event schemas, erclient.

**Spec:** `docs/superpowers/specs/2026-07-05-transient-er-delivery-errors-design.md` (in `gundi-dispatcher-er`)

## Global Constraints

- Two repos: Tasks 1–4 in `/Users/chrisdo/padas/cdip` (working dir `/Users/chrisdo/padas/cdip/cdip_admin`), Task 5 in `/Users/chrisdo/padas/gundi-dispatcher-er`.
- `cdip` work happens on branch `feature/transient-er-delivery-alarms` created from up-to-date `main` (run `git fetch origin && git checkout -b feature/transient-er-delivery-alarms origin/main` in `/Users/chrisdo/padas/cdip` before Task 1). `gundi-dispatcher-er` work continues on the existing `spec/transient-er-delivery-errors` branch.
- Retriable status codes are exactly `{409, 429, 502, 503, 504}`. Plain 500 stays ERROR (deliberate — see spec).
- The ER integration type slug is `earth_ranger` (verified in `cdip_admin/conftest.py:390`).
- New health-check setting name: `retriable_error_count_threshold`, default `30`.
- New status details string (verbatim): `Sustained delivery errors - destination may be down or overloaded`
- cdip tests run from `/Users/chrisdo/padas/cdip/cdip_admin` with plain `pytest` (pytest.ini sets `DJANGO_SETTINGS_MODULE=cdip_admin.local_settings`, `--reuse-db`). Requires the local dev database used by the existing suite.
- Existing behavior that must not change: v1 legacy events stay ERROR; non-ER destinations stay ERROR; 400/401/403/404/500 stay ERROR.

---

### Task 1: Portal — retriable-error classifier + delivery-failed handler

**Files:**
- Modify: `/Users/chrisdo/padas/cdip/cdip_admin/event_consumers/dispatcher_events_consumer.py` (helper near top after imports; classification at lines ~181-185)
- Test: `/Users/chrisdo/padas/cdip/cdip_admin/event_consumers/tests/test_dispatcher_events_consumer.py`

**Interfaces:**
- Consumes: existing `Integration` model import (already imported in this module — used by `handle_dispatcher_log_event`), `ActivityLog.LogLevels`.
- Produces: `is_retriable_er_error(error: str | None, server_response_status: int | None, destination: Integration | None) -> bool`, plus module constants `RETRIABLE_ER_STATUS_CODES: set[int]` and `TRANSIENT_ER_ERROR_MARKERS: tuple[str, ...]`. Task 2 reuses all three.

- [ ] **Step 1: Write the failing tests**

Add to `event_consumers/tests/test_dispatcher_events_consumer.py`. Add `from unittest.mock import MagicMock` to the imports at the top of the file (`json`, `pytest`, `ActivityLog`, `process_event`, etc. are already imported there). Add a module-level helper and tests at the end of the file:

```python
def _make_er_delivery_failed_message(trace, destination, error, server_response_status=None,
                                     server_response_body=""):
    message = MagicMock()
    event_dict = {
        "event_id": "605535df-1b9b-412b-9fd5-e29b09582999",
        "timestamp": "2023-07-11 18:19:19.215459+00:00",
        "schema_version": "v2",
        "event_type": "ObservationDeliveryFailed",
        "payload": {
            "error": error,
            "error_traceback": "Traceback (most recent call last): ...",
            "server_response_status": server_response_status,
            "server_response_body": server_response_body,
            "observation": {
                "gundi_id": str(trace.object_id),
                "related_to": None,
                "data_provider_id": str(trace.data_provider.id),
                "destination_id": str(destination.id),
                "delivered_at": "2025-01-23 16:54:19.215015+00:00",
            },
        }
    }
    message.data = json.dumps(event_dict).encode("utf-8")
    return message


@pytest.mark.parametrize("server_response_status", [409, 429, 502, 503, 504])
def test_retriable_status_delivery_errors_are_logged_as_warnings(
        lotek_observation_trace, integrations_list_er, server_response_status
):
    message = _make_er_delivery_failed_message(
        trace=lotek_observation_trace,
        destination=integrations_list_er[0],
        error=f"ERClientException: ER error ON POST https://fake-site.pamdas.org/api/v1.0/sensors/",
        server_response_status=server_response_status,
        server_response_body='{"status": {"message": "err"}}',
    )
    process_event(message)
    activity_log = ActivityLog.objects.filter(
        integration_id=str(lotek_observation_trace.data_provider.id)
    ).first()
    assert activity_log
    assert activity_log.log_level == ActivityLog.LogLevels.WARNING
    assert activity_log.value == "observation_delivery_failed"


@pytest.mark.parametrize("error", [
    # erclient wraps httpx.RequestError (timeouts, connection errors) into an
    # ERClientException with no status code and the "Request to ER failed" prefix.
    "ERClientException: Request to ER failed: Connection timeout to host https://fake-site.pamdas.org",
    "ERClientServiceUnreachable: ER Service Unavailable ON POST https://fake-site.pamdas.org/api/v1.0/sensors/",
    "ERClientRateLimitExceeded: ER Too Many Requests ON POST https://fake-site.pamdas.org/api/v1.0/sensors/",
    "ClientConnectorError: Cannot connect to host fake-site.pamdas.org:443 ssl:default",
    "ServerTimeoutError: Timeout on reading data from socket",
    "ServerDisconnectedError: Server disconnected",
    "ClientOSError: [Errno 104] Connection reset by peer",
    "ConnectionResetError: [Errno 104] Connection reset by peer",
    "TimeoutError: Request timed out",
])
def test_transient_errors_without_status_are_logged_as_warnings(
        lotek_observation_trace, integrations_list_er, error
):
    message = _make_er_delivery_failed_message(
        trace=lotek_observation_trace,
        destination=integrations_list_er[0],
        error=error,
        server_response_status=None,
    )
    process_event(message)
    activity_log = ActivityLog.objects.filter(
        integration_id=str(lotek_observation_trace.data_provider.id)
    ).first()
    assert activity_log
    assert activity_log.log_level == ActivityLog.LogLevels.WARNING


def test_retries_exhausted_failures_are_logged_as_errors(
        lotek_observation_trace, integrations_list_er
):
    # The dispatcher publishes this event when it dead-letters a message whose
    # retries are exhausted (Task 5). It must land as ERROR: "we gave up" is
    # the real alarm.
    message = _make_er_delivery_failed_message(
        trace=lotek_observation_trace,
        destination=integrations_list_er[0],
        error="Delivery retries exhausted (message older than 86400 seconds). Message sent to dead-letter queue.",
        server_response_status=None,
    )
    process_event(message)
    activity_log = ActivityLog.objects.filter(
        integration_id=str(lotek_observation_trace.data_provider.id)
    ).first()
    assert activity_log
    assert activity_log.log_level == ActivityLog.LogLevels.ERROR


@pytest.mark.parametrize("server_response_status,error", [
    (500, "ERClientInternalError: ER Internal Server Error ON POST https://fake-site.pamdas.org/api/v1.0/sensors/"),
    (401, "ERClientBadCredentials: ER Unauthorized ON POST https://fake-site.pamdas.org/api/v1.0/sensors/"),
    (400, "ERClientBadRequest: ER Bad Request ON POST https://fake-site.pamdas.org/api/v1.0/sensors/"),
    (404, "ERClientNotFound: ER Not Found ON POST https://fake-site.pamdas.org/api/v1.0/sensors/"),
])
def test_non_retriable_delivery_errors_are_logged_as_errors(
        lotek_observation_trace, integrations_list_er, server_response_status, error
):
    message = _make_er_delivery_failed_message(
        trace=lotek_observation_trace,
        destination=integrations_list_er[0],
        error=error,
        server_response_status=server_response_status,
    )
    process_event(message)
    activity_log = ActivityLog.objects.filter(
        integration_id=str(lotek_observation_trace.data_provider.id)
    ).first()
    assert activity_log
    assert activity_log.log_level == ActivityLog.LogLevels.ERROR


def test_retriable_errors_from_non_er_destinations_stay_errors(
        trap_tagger_to_movebank_observation_trace, destination_movebank
):
    # Classification is scoped to EarthRanger destinations only.
    message = _make_er_delivery_failed_message(
        trace=trap_tagger_to_movebank_observation_trace,
        destination=destination_movebank,
        error="Exception: Service Unavailable",
        server_response_status=503,
    )
    process_event(message)
    activity_log = ActivityLog.objects.filter(
        integration_id=str(trap_tagger_to_movebank_observation_trace.data_provider.id)
    ).first()
    assert activity_log
    assert activity_log.log_level == ActivityLog.LogLevels.ERROR
```

- [ ] **Step 2: Run the new tests to verify they fail**

Run:
```bash
cd /Users/chrisdo/padas/cdip/cdip_admin && pytest event_consumers/tests/test_dispatcher_events_consumer.py -v -k "retriable or transient or retries_exhausted"
```
Expected: the 409 parametrized case PASSES (existing behavior — its error string contains "pamdas.org"); the 429/502/503/504 cases and all `test_transient_errors_without_status...` cases FAIL with `assert 'error' == 'warning'`-style assertion errors; the non-retriable, non-ER, and retries-exhausted tests PASS (current behavior is already ERROR). This confirms the tests measure the right thing.

- [ ] **Step 3: Implement the classifier and use it in the delivery-failed handler**

In `event_consumers/dispatcher_events_consumer.py`, add after the imports/`data_type_str_map` section near the top:

```python
# HTTP statuses from EarthRanger that indicate a transient condition. The
# dispatcher re-raises on failure so PubSub retries the message; these
# failures usually resolve on retry and must not count toward the
# unhealthy threshold in the health calculator.
RETRIABLE_ER_STATUS_CODES = {409, 429, 502, 503, 504}

# Markers of transient failures that carry no HTTP status code. The dispatcher
# formats errors as f"{type(e).__name__}: {e}", so exception class names appear
# in the error string. "Request to ER failed" is the message erclient uses when
# wrapping network errors and timeouts (httpx.RequestError). "Will retry later"
# is the dispatcher's message when an event update arrives before its parent
# event was delivered.
TRANSIENT_ER_ERROR_MARKERS = (
    "Request to ER failed",
    "ERClientServiceUnreachable",
    "ERClientRateLimitExceeded",
    "ClientConnectorError",
    "ServerTimeoutError",
    "ServerDisconnectedError",
    "ClientOSError",
    "ConnectionResetError",
    "TimeoutError",
    "Will retry later",
)


def is_retriable_er_error(error, server_response_status, destination):
    # Retriable failures against EarthRanger destinations are logged as WARNING
    # so temporary outages don't mark the connection unhealthy. Sustained
    # volumes of these warnings are caught by a separate threshold in
    # calculate_integration_status.
    if not destination or destination.type.value != "earth_ranger":
        return False
    if server_response_status in RETRIABLE_ER_STATUS_CODES:
        return True
    if not server_response_status and error:
        return any(marker in error for marker in TRANSIENT_ER_ERROR_MARKERS)
    return False
```

In `handle_observation_delivery_failed_event`, replace:

```python
    # Flag the error as a warning if ER returns a 409 conflict error. Those are retried.
    if "pamdas.org" in event.payload.error and event.payload.server_response_status == 409:
        level = ActivityLog.LogLevels.WARNING
    else:
        level = ActivityLog.LogLevels.ERROR
```

with:

```python
    destination = Integration.objects.filter(id=str(observation.destination_id)).first()
    if is_retriable_er_error(
        error=event.payload.error,
        server_response_status=event.payload.server_response_status,
        destination=destination,
    ):
        level = ActivityLog.LogLevels.WARNING
    else:
        level = ActivityLog.LogLevels.ERROR
```

Note: v1 legacy events keep ERROR automatically — their synthesized error text ("Delivery Failed at the Dispatcher. Please update this dispatcher...") matches no marker and has no status code.

- [ ] **Step 4: Run the consumer test module to verify everything passes**

Run:
```bash
cd /Users/chrisdo/padas/cdip/cdip_admin && pytest event_consumers/tests/test_dispatcher_events_consumer.py -v
```
Expected: ALL PASS, including the pre-existing `test_409_delivery_errors_are_logged_as_warnings` (its fixture is an ER destination with status 409 → still WARNING) and `test_process_observation_delivery_failed_event` (its v2 fixture is a 400 `ERClientBadRequest` → still ERROR).

- [ ] **Step 5: Commit**

```bash
cd /Users/chrisdo/padas/cdip && git add cdip_admin/event_consumers/dispatcher_events_consumer.py cdip_admin/event_consumers/tests/test_dispatcher_events_consumer.py && git commit -m "Log retriable ER delivery failures as warnings

Generalizes the ER-409 special case: 409/429/502/503/504 responses and
status-less network/timeout failures against EarthRanger destinations are
transient and retried by PubSub, so they are recorded as WARNING instead
of ERROR and no longer trip the unhealthy threshold."
```

---

### Task 2: Portal — classify update-failed events with the same helper

**Files:**
- Modify: `/Users/chrisdo/padas/cdip/cdip_admin/event_consumers/dispatcher_events_consumer.py` (`handle_observation_update_failed_event`, ActivityLog creation at lines ~292-301)
- Test: `/Users/chrisdo/padas/cdip/cdip_admin/event_consumers/tests/test_dispatcher_events_consumer.py` (modify `test_process_observation_update_failed_event`)

**Interfaces:**
- Consumes: `is_retriable_er_error(error, server_response_status, destination)` from Task 1 (same module).
- Produces: nothing new — update-failed activity logs now carry `log_level=WARNING` when retriable. Task 4 counts these via `value="observation_update_failed"`.

- [ ] **Step 1: Update the existing test to expect WARNING for the v2 case**

The existing v2 fixture `trap_tagger_observation_update_failed_schema_v2_event` (conftest.py:2755) has error `"Event ... wasn't delivered yet. Will retry later."`, `server_response_status: None`, and an ER destination — under the new classification it becomes WARNING. The v1 case stays ERROR. In `test_process_observation_update_failed_event`, replace:

```python
    assert activity_log.log_level == ActivityLog.LogLevels.ERROR
```

with:

```python
    if schema_version == "v1":
        # v1 events carry no status/error detail; current behavior is preserved
        assert activity_log.log_level == ActivityLog.LogLevels.ERROR
    else:
        # The v2 fixture is a "Will retry later" reference-data wait — retriable
        assert activity_log.log_level == ActivityLog.LogLevels.WARNING
```

Also add a new test at the end of the file for a non-retriable update failure staying ERROR:

```python
def test_non_retriable_update_failures_are_logged_as_errors(
        trap_tagger_event_update_trace, integrations_list_er
):
    message = MagicMock()
    event_dict = {
        "event_id": "605535df-1b9b-412b-9fd5-e29b09582999",
        "timestamp": "2023-07-11 18:19:19.215459+00:00",
        "schema_version": "v2",
        "event_type": "ObservationUpdateFailed",
        "payload": {
            "error": "ERClientBadRequest: ER Bad Request ON PATCH https://fake-site.pamdas.org/api/v1.0/activity/event/1234",
            "error_traceback": "",
            "server_response_status": 400,
            "server_response_body": '{"status": {"code": 400}}',
            "observation": {
                "gundi_id": str(trap_tagger_event_update_trace.object_id),
                "related_to": None,
                "data_provider_id": str(trap_tagger_event_update_trace.data_provider.id),
                "destination_id": str(integrations_list_er[0].id),
                "updated_at": "2024-07-25 12:25:44.442696+00:00",
            },
        },
    }
    message.data = json.dumps(event_dict).encode("utf-8")
    process_event(message)
    activity_log = ActivityLog.objects.filter(
        integration_id=str(trap_tagger_event_update_trace.data_provider.id)
    ).first()
    assert activity_log
    assert activity_log.log_level == ActivityLog.LogLevels.ERROR
    assert activity_log.value == "observation_update_failed"
```

- [ ] **Step 2: Run to verify the updated test fails**

Run:
```bash
cd /Users/chrisdo/padas/cdip/cdip_admin && pytest event_consumers/tests/test_dispatcher_events_consumer.py -v -k "update_failed"
```
Expected: `test_process_observation_update_failed_event[schema_v2]` FAILS (log is still ERROR, test now expects WARNING); `[schema_v1]` and the new non-retriable test PASS.

- [ ] **Step 3: Apply the classifier in the update-failed handler**

In `handle_observation_update_failed_event`, replace the unconditional ERROR:

```python
    ActivityLog.objects.create(
        log_level=ActivityLog.LogLevels.ERROR,
```

with:

```python
    destination = Integration.objects.filter(id=destination_id).first()
    if is_retriable_er_error(
        error=event_data.error,
        server_response_status=getattr(event_data, "server_response_status", None),
        destination=destination,
    ):
        level = ActivityLog.LogLevels.WARNING
    else:
        level = ActivityLog.LogLevels.ERROR
    ActivityLog.objects.create(
        log_level=level,
```

(`destination_id` is already a local variable in this handler. `getattr` guards the v1-built payload, which may omit the field depending on gundi-core defaults.)

- [ ] **Step 4: Run the full consumer test module**

Run:
```bash
cd /Users/chrisdo/padas/cdip/cdip_admin && pytest event_consumers/tests/test_dispatcher_events_consumer.py -v
```
Expected: ALL PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/chrisdo/padas/cdip && git add cdip_admin/event_consumers/dispatcher_events_consumer.py cdip_admin/event_consumers/tests/test_dispatcher_events_consumer.py && git commit -m "Log retriable ER update failures as warnings

Applies the same retriable-error classification to ObservationUpdateFailed
events, covering 'Will retry later' reference-data waits (event updates
arriving before their parent event) and transient ER responses."
```

---

### Task 3: Portal — `retriable_error_count_threshold` setting + migration

**Files:**
- Modify: `/Users/chrisdo/padas/cdip/cdip_admin/integrations/models/v2/models.py` (`HealthCheckSettings`, lines ~593-608)
- Create: `/Users/chrisdo/padas/cdip/cdip_admin/integrations/migrations/0116_healthchecksettings_retriable_error_count_threshold.py` (generated)
- Test: `/Users/chrisdo/padas/cdip/cdip_admin/integrations/tests/test_calc_integration_status.py`

**Interfaces:**
- Consumes: existing `HealthCheckSettings` model (fields `error_count_threshold`, `time_window_minutes`).
- Produces: `HealthCheckSettings.retriable_error_count_threshold: PositiveIntegerField(default=30)`. Task 4 reads it in `calculate_integration_status`.

- [ ] **Step 1: Write the failing test**

Add to `integrations/tests/test_calc_integration_status.py`:

```python
def test_health_check_settings_have_retriable_error_threshold(provider_lotek_panthera):
    settings = provider_lotek_panthera.health_check_settings
    assert settings.retriable_error_count_threshold == 30
```

- [ ] **Step 2: Run to verify it fails**

Run:
```bash
cd /Users/chrisdo/padas/cdip/cdip_admin && pytest integrations/tests/test_calc_integration_status.py::test_health_check_settings_have_retriable_error_threshold -v
```
Expected: FAIL with `AttributeError: 'HealthCheckSettings' object has no attribute 'retriable_error_count_threshold'`.

- [ ] **Step 3: Add the field and generate the migration**

In `integrations/models/v2/models.py`, inside `HealthCheckSettings` after `time_window_minutes`:

```python
    retriable_error_count_threshold = models.PositiveIntegerField(
        default=30,
        help_text=(
            "Number of retriable (warning-level) delivery failures within the time window "
            "before the integration is marked unhealthy. Retriable failures are transient "
            "destination errors that PubSub retries; a sustained volume indicates an outage."
        ),
    )
```

Generate the migration:
```bash
cd /Users/chrisdo/padas/cdip/cdip_admin && python manage.py makemigrations integrations
```
Expected output: `integrations/migrations/0116_healthchecksettings_retriable_error_count_threshold.py` with a single `AddField` operation. (If `manage.py` needs the settings module explicitly, prefix with `DJANGO_SETTINGS_MODULE=cdip_admin.local_settings`.)

- [ ] **Step 4: Run the test to verify it passes**

Run:
```bash
cd /Users/chrisdo/padas/cdip/cdip_admin && pytest integrations/tests/test_calc_integration_status.py::test_health_check_settings_have_retriable_error_threshold -v
```
Expected: PASS (pytest applies the new migration to the test DB; if `--reuse-db` skips it, run once with `pytest --create-db` for this module).

- [ ] **Step 5: Commit**

```bash
cd /Users/chrisdo/padas/cdip && git add cdip_admin/integrations/models/v2/models.py cdip_admin/integrations/migrations/ cdip_admin/integrations/tests/test_calc_integration_status.py && git commit -m "Add retriable_error_count_threshold to health check settings"
```

---

### Task 4: Portal — WARNING-volume safety net in the health calculator

**Files:**
- Modify: `/Users/chrisdo/padas/cdip/cdip_admin/integrations/models/v2/services.py` (`calculate_integration_status`, lines ~102-119)
- Test: `/Users/chrisdo/padas/cdip/cdip_admin/integrations/tests/test_calc_integration_status.py`

**Interfaces:**
- Consumes: `HealthCheckSettings.retriable_error_count_threshold` (Task 3); WARNING logs with `value in ("observation_delivery_failed", "observation_update_failed")` produced by Tasks 1–2.
- Produces: new UNHEALTHY branch with `status_details = "Sustained delivery errors - destination may be down or overloaded"`.

- [ ] **Step 1: Write the failing tests**

Add to `integrations/tests/test_calc_integration_status.py`. Add `from activity_log.models import ActivityLog` to the imports, then:

```python
def _make_retriable_delivery_warning_logs(integration, count, value="observation_delivery_failed"):
    return [
        ActivityLog.objects.create(
            log_level=ActivityLog.LogLevels.WARNING,
            log_type=ActivityLog.LogTypes.EVENT,
            origin=ActivityLog.Origin.DISPATCHER,
            integration=integration,
            value=value,
            title="Error Delivering Observation to 'https://fake-site.pamdas.org'",
            details={"server_response_status": 503},
            is_reversible=False,
        )
        for _ in range(count)
    ]


def test_sustained_retriable_delivery_errors_mark_integration_unhealthy(provider_lotek_panthera):
    provider_lotek_panthera.health_check_settings.retriable_error_count_threshold = 3
    provider_lotek_panthera.health_check_settings.save()
    _make_retriable_delivery_warning_logs(provider_lotek_panthera, count=3)

    calculate_integration_status(integration_id=provider_lotek_panthera.id)

    provider_lotek_panthera.status.refresh_from_db()
    assert provider_lotek_panthera.status.status == IntegrationStatus.Status.UNHEALTHY
    assert provider_lotek_panthera.status.status_details == (
        "Sustained delivery errors - destination may be down or overloaded"
    )


def test_few_retriable_delivery_errors_keep_integration_healthy(provider_lotek_panthera):
    # Below the threshold (default 30), retriable warnings must not alarm
    _make_retriable_delivery_warning_logs(provider_lotek_panthera, count=5)

    calculate_integration_status(integration_id=provider_lotek_panthera.id)

    provider_lotek_panthera.status.refresh_from_db()
    assert provider_lotek_panthera.status.status == IntegrationStatus.Status.HEALTHY


def test_unrelated_warnings_do_not_count_toward_sustained_errors(provider_lotek_panthera):
    provider_lotek_panthera.health_check_settings.retriable_error_count_threshold = 3
    provider_lotek_panthera.health_check_settings.save()
    _make_retriable_delivery_warning_logs(provider_lotek_panthera, count=3, value="custom_dispatcher_log")

    calculate_integration_status(integration_id=provider_lotek_panthera.id)

    provider_lotek_panthera.status.refresh_from_db()
    assert provider_lotek_panthera.status.status == IntegrationStatus.Status.HEALTHY


def test_error_threshold_takes_precedence_over_warning_threshold(
        provider_lotek_panthera,
        pull_observations_action_started_activity_log,
        pull_observations_action_failed_activity_log,
        pull_observations_action_failed_activity_log_2,
        pull_observations_action_failed_activity_log_3
):
    # When both thresholds are crossed, the ERROR branch runs first and its
    # status_details wins (the elif chain in calculate_integration_status).
    provider_lotek_panthera.health_check_settings.retriable_error_count_threshold = 3
    provider_lotek_panthera.health_check_settings.save()
    _make_retriable_delivery_warning_logs(provider_lotek_panthera, count=3)

    calculate_integration_status(integration_id=provider_lotek_panthera.id)

    provider_lotek_panthera.status.refresh_from_db()
    assert provider_lotek_panthera.status.status == IntegrationStatus.Status.UNHEALTHY
    assert provider_lotek_panthera.status.status_details != (
        "Sustained delivery errors - destination may be down or overloaded"
    )


def test_retriable_update_failures_count_toward_sustained_errors(provider_lotek_panthera):
    provider_lotek_panthera.health_check_settings.retriable_error_count_threshold = 4
    provider_lotek_panthera.health_check_settings.save()
    _make_retriable_delivery_warning_logs(provider_lotek_panthera, count=2)
    _make_retriable_delivery_warning_logs(provider_lotek_panthera, count=2, value="observation_update_failed")

    calculate_integration_status(integration_id=provider_lotek_panthera.id)

    provider_lotek_panthera.status.refresh_from_db()
    assert provider_lotek_panthera.status.status == IntegrationStatus.Status.UNHEALTHY
```

- [ ] **Step 2: Run to verify they fail**

Run:
```bash
cd /Users/chrisdo/padas/cdip/cdip_admin && pytest integrations/tests/test_calc_integration_status.py -v -k "sustained or unrelated_warnings or retriable_update"
```
Expected: `test_sustained_...` and `test_retriable_update_...` FAIL (status stays healthy); the two negative tests PASS.

- [ ] **Step 3: Add the WARNING-volume branch**

In `integrations/models/v2/services.py::calculate_integration_status`, after the existing `elif` for DISPATCHER-origin ERROR logs (the block ending `"Errors were detected while pushing data to the destination"`), add a further `elif`:

```python
    elif ActivityLog.objects.filter(
        origin=ActivityLog.Origin.DISPATCHER,
        integration=integration,
        log_level=ActivityLog.LogLevels.WARNING,
        value__in=("observation_delivery_failed", "observation_update_failed"),
        created_at__gte=time_window
    ).count() >= healthcheck_settings.retriable_error_count_threshold:
        # Retriable (transient) delivery failures are logged as warnings and don't
        # count toward the error threshold above. But a sustained volume of them
        # means the destination is down or overloaded, which must still alarm.
        integration_status.status = IntegrationStatus.Status.UNHEALTHY
        integration_status.status_details = "Sustained delivery errors - destination may be down or overloaded"
```

- [ ] **Step 4: Run the health-calc test module**

Run:
```bash
cd /Users/chrisdo/padas/cdip/cdip_admin && pytest integrations/tests/test_calc_integration_status.py -v
```
Expected: ALL PASS (existing threshold tests unaffected — they use ERROR-level logs).

- [ ] **Step 5: Commit**

```bash
cd /Users/chrisdo/padas/cdip && git add cdip_admin/integrations/models/v2/services.py cdip_admin/integrations/tests/test_calc_integration_status.py && git commit -m "Mark integrations unhealthy on sustained retriable delivery errors

Retriable ER failures are now warnings and skip the error threshold, so a
hard-down destination would otherwise never alarm. Count delivery/update
failure warnings against retriable_error_count_threshold (default 30) in
the same time window."
```

---

### Task 5: Dispatcher — publish ERROR event when dead-lettering a too-old v2 message

**Files:**
- Modify: `/Users/chrisdo/padas/gundi-dispatcher-er/core/services.py` (imports at top; new function after `send_observation_to_dead_letter_topic`; too-old branch in `process_request`, lines ~327-331)
- Modify: `/Users/chrisdo/padas/gundi-dispatcher-er/tests/conftest.py` (three too-old request fixtures)
- Test: `/Users/chrisdo/padas/gundi-dispatcher-er/tests/test_dead_lettering.py`

**Interfaces:**
- Consumes: `publish_event(event, topic_name)` and `is_null(value)` from `core/utils.py`; `system_events.ObservationDeliveryFailed` / `ObservationUpdateFailed`, `DeliveryErrorDetails` / `UpdateErrorDetails` from `gundi_core.events`; `gundi_schemas_v2.DispatchedObservation` / `UpdatedObservation`.
- Produces: `publish_retries_exhausted_event(attributes: dict) -> None` (async) in `core/services.py`, called from `process_request` for v2 messages only. The portal (Task 1 classifier) records it as ERROR: no status code and no transient marker in the text.

- [ ] **Step 1: Add too-old request fixtures**

In `/Users/chrisdo/padas/gundi-dispatcher-er/tests/conftest.py`, add (near the existing `*_as_pubsub_request` fixtures around line 841; `datetime`, `json`, and `settings` are already imported in this module):

```python
def _make_request_too_old(request_fixture):
    # Rewind publish_time so is_too_old() routes the message to the DLQ
    json_data = request_fixture.get_json.return_value
    old_publish_time = (
        datetime.datetime.now(datetime.timezone.utc)
        - datetime.timedelta(seconds=settings.MAX_EVENT_AGE_SECONDS + 3600)
    ).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    json_data["message"]["publish_time"] = old_publish_time
    json_data["message"]["publishTime"] = old_publish_time
    request_fixture.data = json.dumps(json_data)
    return request_fixture


@pytest.fixture
def event_v2_as_pubsub_request_too_old(event_v2_as_pubsub_request):
    return _make_request_too_old(event_v2_as_pubsub_request)


@pytest.fixture
def event_update_v2_as_pubsub_request_too_old(event_update_v2_as_pubsub_request):
    return _make_request_too_old(event_update_v2_as_pubsub_request)


@pytest.fixture
def position_as_request_too_old(position_as_request):
    return _make_request_too_old(position_as_request)
```

(If `from core import settings` is not already among conftest imports, add it.)

- [ ] **Step 2: Write the failing tests**

In `/Users/chrisdo/padas/gundi-dispatcher-er/tests/test_dead_lettering.py`, extend the imports:

```python
from gundi_core import events as system_events
from core.services import send_observation_to_dead_letter_topic, process_request
```

and add:

```python
@pytest.mark.asyncio
async def test_too_old_v2_event_publishes_retries_exhausted_error(
        mocker, mock_pubsub_client, mock_publish_event, event_v2_as_pubsub_request_too_old
):
    mocker.patch("core.services.pubsub", mock_pubsub_client)
    mocker.patch("core.services.publish_event", mock_publish_event)

    await process_request(event_v2_as_pubsub_request_too_old)

    # The message was sent to the events DLQ topic
    publish_calls = [c for c in mock_pubsub_client.PublisherClient.mock_calls if c[0] == "().publish"]
    assert len(publish_calls) == 1
    assert publish_calls[0].args[0] == f"projects/{settings.GCP_PROJECT_ID}/topics/{settings.EVENTS_DEAD_LETTER_TOPIC}"
    # And a retries-exhausted failure event was published for the portal
    assert mock_publish_event.called
    call_kwargs = mock_publish_event.call_args.kwargs
    event = call_kwargs["event"]
    assert isinstance(event, system_events.ObservationDeliveryFailed)
    assert "retries exhausted" in event.payload.error.lower()
    assert event.payload.server_response_status is None
    assert str(event.payload.observation.gundi_id) == "23ca4b15-18b6-4cf4-9da6-36dd69c6f638"
    assert call_kwargs["topic_name"] == settings.DISPATCHER_EVENTS_TOPIC


@pytest.mark.asyncio
async def test_too_old_v2_event_update_publishes_update_failed_error(
        mocker, mock_pubsub_client, mock_publish_event, event_update_v2_as_pubsub_request_too_old
):
    mocker.patch("core.services.pubsub", mock_pubsub_client)
    mocker.patch("core.services.publish_event", mock_publish_event)

    await process_request(event_update_v2_as_pubsub_request_too_old)

    assert mock_publish_event.called
    event = mock_publish_event.call_args.kwargs["event"]
    assert isinstance(event, system_events.ObservationUpdateFailed)
    assert "retries exhausted" in event.payload.error.lower()


@pytest.mark.asyncio
async def test_too_old_v1_message_does_not_publish_failure_event(
        mocker, mock_pubsub_client, mock_publish_event, position_as_request_too_old
):
    mocker.patch("core.services.pubsub", mock_pubsub_client)
    mocker.patch("core.services.publish_event", mock_publish_event)

    await process_request(position_as_request_too_old)

    # v1 messages go to the legacy DLQ with no portal event (deprecated path)
    publish_calls = [c for c in mock_pubsub_client.PublisherClient.mock_calls if c[0] == "().publish"]
    assert len(publish_calls) == 1
    assert publish_calls[0].args[0] == f"projects/{settings.GCP_PROJECT_ID}/topics/{settings.LEGACY_DEAD_LETTER_TOPIC}"
    assert not mock_publish_event.called


@pytest.mark.asyncio
async def test_failure_publishing_retries_exhausted_event_does_not_raise(
        mocker, mock_pubsub_client, mock_publish_event, event_v2_as_pubsub_request_too_old
):
    mocker.patch("core.services.pubsub", mock_pubsub_client)
    mock_publish_event.side_effect = Exception("PubSub is down")
    mocker.patch("core.services.publish_event", mock_publish_event)

    # Must not raise: the DLQ send already succeeded and the message must be acked
    await process_request(event_v2_as_pubsub_request_too_old)
```

- [ ] **Step 3: Run to verify they fail**

Run:
```bash
cd /Users/chrisdo/padas/gundi-dispatcher-er && pytest tests/test_dead_lettering.py -v
```
Expected: the three new v2/v1 tests FAIL — the first two with `AttributeError: <module 'core.services'> does not have the attribute 'publish_event'` (patch target doesn't exist yet) or `assert mock_publish_event.called`; the existing parametrized DLQ tests PASS.

- [ ] **Step 4: Implement `publish_retries_exhausted_event`**

In `/Users/chrisdo/padas/gundi-dispatcher-er/core/services.py`:

Extend the imports at the top:

```python
from gundi_core import events as system_events
from gundi_core.events import UpdateErrorDetails, DeliveryErrorDetails
from gundi_core.schemas import v2 as gundi_schemas_v2
from core.utils import (
    extract_fields_from_message,
    get_inbound_integration_detail,
    get_outbound_config_detail,
    is_null,
    publish_event,
    ExtraKeys,
)
```

Add after `send_observation_to_dead_letter_topic`:

```python
async def publish_retries_exhausted_event(attributes: dict):
    # Notify the portal that we gave up on this message so it's recorded as an
    # ERROR in the activity log. Intermediate retriable failures are recorded
    # as warnings, so without this event a message that never delivers would
    # be dead-lettered silently.
    gundi_id = attributes.get("gundi_id")
    destination_id = attributes.get("destination_id")
    if not gundi_id or not destination_id:
        logger.warning(
            "Cannot publish retries-exhausted event without gundi_id and destination_id.",
            extra={"attributes": attributes},
        )
        return
    data_provider_id = attributes.get("data_provider_id")
    related_to = attributes.get("related_to")
    related_to = None if is_null(related_to) else related_to
    error_msg = (
        f"Delivery retries exhausted (message older than {settings.MAX_EVENT_AGE_SECONDS} seconds). "
        "Message sent to dead-letter queue."
    )
    if attributes.get("stream_type") == StreamPrefixEnum.event_update.value:
        event = system_events.ObservationUpdateFailed(
            payload=UpdateErrorDetails(
                error=error_msg,
                observation=gundi_schemas_v2.UpdatedObservation(
                    gundi_id=gundi_id,
                    related_to=related_to,
                    data_provider_id=data_provider_id,
                    destination_id=destination_id,
                    updated_at=datetime.now(timezone.utc),
                ),
            )
        )
    else:
        event = system_events.ObservationDeliveryFailed(
            payload=DeliveryErrorDetails(
                error=error_msg,
                observation=gundi_schemas_v2.DispatchedObservation(
                    gundi_id=gundi_id,
                    related_to=related_to,
                    external_id=None,
                    data_provider_id=data_provider_id,
                    destination_id=destination_id,
                    delivered_at=datetime.now(timezone.utc),
                ),
            )
        )
    try:
        await publish_event(event=event, topic_name=settings.DISPATCHER_EVENTS_TOPIC)
    except Exception as e:
        logger.exception(
            f"Error publishing retries-exhausted event for gundi_id {gundi_id}: {e}. "
            "The message was still sent to the dead-letter topic."
        )
```

In `process_request`, change the too-old branch:

```python
        if is_too_old(timestamp):
            logger.warning(f"Event is too old (timestamp = {timestamp}) and will be sent to dead-letter.")
            current_span.set_attribute("is_too_old", True)
            await send_observation_to_dead_letter_topic(transformed_observation, attributes)
            if attributes.get("gundi_version", "v1") == "v2":
                await publish_retries_exhausted_event(attributes)
            return  # Skip the event
```

(`datetime`/`timezone` and `StreamPrefixEnum` are already imported in this module.)

- [ ] **Step 5: Run the dead-lettering tests, then the full suite**

Run:
```bash
cd /Users/chrisdo/padas/gundi-dispatcher-er && pytest tests/test_dead_lettering.py -v && pytest
```
Expected: ALL PASS.

- [ ] **Step 6: Commit**

```bash
cd /Users/chrisdo/padas/gundi-dispatcher-er && git add core/services.py tests/conftest.py tests/test_dead_lettering.py && git commit -m "Publish delivery-failed event when dead-lettering too-old v2 messages

Retriable failures are recorded as warnings in the portal, so a message
whose retries are exhausted (older than MAX_EVENT_AGE_SECONDS) would be
dead-lettered silently. Publish an ObservationDeliveryFailed /
ObservationUpdateFailed with no status code so the portal records one
ERROR-level activity log at the moment we give up."
```

---

### Task 6: Full verification

**Files:** none (verification only)

- [ ] **Step 1: Run the affected cdip test modules**

Run:
```bash
cd /Users/chrisdo/padas/cdip/cdip_admin && pytest event_consumers/ integrations/tests/test_calc_integration_status.py integrations/tests/test_email_alerts.py -v
```
Expected: ALL PASS. (`test_email_alerts.py` is included because it exercises statuses produced by `calculate_integration_status`.)

- [ ] **Step 2: Run the full dispatcher suite**

Run:
```bash
cd /Users/chrisdo/padas/gundi-dispatcher-er && pytest
```
Expected: ALL PASS.

- [ ] **Step 3: Report**

Summarize for the user: branches (`feature/transient-er-delivery-alarms` in cdip, `spec/transient-er-delivery-errors` in gundi-dispatcher-er), commits, and the deploy note from the spec — the portal deploy activates classification + safety net fleet-wide immediately; the dispatcher change ships with the next dispatcher release; thresholds are tunable per integration via `HealthCheckSettings`.

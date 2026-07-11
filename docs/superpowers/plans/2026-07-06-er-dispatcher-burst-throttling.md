# ER Dispatcher Burst Throttling Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **SUPERSEDED DETAILS (2026-07-11):** Task 6's er-client change shipped as
> PADAS/er-client#53 and was released as **1.16.0** (first release through the
> new trusted-publishing workflow), so Task 5's pin target became
> `earthranger-client==1.16.0` from PyPI instead of the v1.15.0 GitHub wheel,
> and `retry_after` is exercised end-to-end in this repo's suite. Version
> numbers, wheel URLs, and "not yet exposed" comments below are retained as
> written history.

**Goal:** Per-destination, per-stream-family burst throttling in the shared ER dispatcher: a Redis admission gate that nacks over-cap/cooling-down messages with HTTP 429, distress cooldowns scoped by failure type, and a companion er-client change exposing `Retry-After`.

**Architecture:** A new `core/throttling.py` module owns all throttle state in Redis (via the existing `core.utils._cache_db` handle): fixed-window rate counters and cooldown keys, both keyed by `(destination_id, family)` plus a site-wide cooldown scope. `process_request` checks admission after the age gate; `main.py` converts a `ThrottledMessage` into an HTTP 429 (PubSub nack). The v2 dispatch exception/success paths record distress and recovery. Everything ships dark behind `THROTTLING_ENABLED=false`.

**Tech Stack:** functions-framework (flask-style responses), redis-py commands via the existing `walrus.Database` handle, gundi-core event schemas, pytest + pytest-asyncio + pytest-mock (dispatcher), pytest + respx (er-client).

**Spec:** `docs/superpowers/specs/2026-07-06-er-dispatcher-burst-throttling-design.md`

## Global Constraints

- Two repos: Tasks 1–5 and 7 in `/Users/chrisdo/padas/gundi-dispatcher-er` (branch `spec/burst-throttling`, already checked out); Task 6 in `/Users/chrisdo/padas/er-client` (create branch `feature/retry-after-header` from up-to-date `main`).
- Dispatcher tests: `cd /Users/chrisdo/padas/gundi-dispatcher-er && pytest <target>` (no containers; everything mocked).
- Families (exact): `events` ← `ev`,`evu`,`att`; `observations` ← `obv`,`obvu`; `messages` ← `txt`; **unknown/missing stream_type → `events`**. (`obvu` = `StreamPrefixEnum.observation_update`, present in gundi-core though not dispatched today — mapped for completeness.)
- Redis key shapes (exact): `throttle:cooldown:{destination_id}:site`, `throttle:cooldown:{destination_id}:{family}`, `throttle:cooldown_level:{destination_id}:{scope}`, `throttle:rate:{destination_id}:{family}:{epoch_minute}`, `throttle:notify:{destination_id}`.
- Cooldown scoping (exact): status **429 → family scope**; status **502/503/504 → site scope**; **no status + error contains `"Request to ER failed"` → site scope**; anything else (incl. 409, 400) → no cooldown.
- Settings and defaults (exact, all via `environs`): `THROTTLING_ENABLED=false`, `DEFAULT_MAX_EVENT_DELIVERIES_PER_MINUTE=120`, `DEFAULT_MAX_OBSERVATION_DELIVERIES_PER_MINUTE=300`, `DEFAULT_MAX_MESSAGE_DELIVERIES_PER_MINUTE=60`, `THROTTLE_GRACE_WAIT_MAX_SECONDS=2`, `THROTTLE_COOLDOWN_BASE_SECONDS=30`, `THROTTLE_COOLDOWN_MAX_SECONDS=600`, `THROTTLE_COOLDOWN_LEVEL_TTL_SECONDS=900`, `THROTTLE_NOTIFY_TTL_SECONDS=300`.
- **Age gate runs before the admission gate** (a too-old message must DLQ, never throttle-nack).
- Deferrals are silent: INFO logs only — no `ObservationDeliveryFailed`, no activity log.
- Redis failures fail open (admit); `THROTTLING_ENABLED=false` means no Redis calls at all.
- **Plan decision (resolving spec open items):** (a) per-destination cap overrides are **deferred to a follow-up** — env defaults only in this phase (the spec's rollout tunes env caps first); (b) plain redis-py commands instead of a Lua script — the spec sanctions this fallback; `INCR` is atomic and the check-then-increment race admits at most a few extra messages, acceptable for a kindness cap, and it keeps the module testable against the suite's MagicMock Redis.
- er-client v1.15.0 signatures verified identical to the pinned 1.8.0 for everything the dispatcher calls (`AsyncERClient.__init__`, `post_sensor_observation`, `post_report`, `post_camera_trap_report`, `patch_report`, `post_report_attachment`, `post_message`, `close`). Wheel asset name verified: `earthranger_client-1.15.0-py3-none-any.whl`.

---

### Task 1: `core/throttling.py` — families, settings, admission check

**Files:**
- Create: `core/throttling.py`
- Modify: `core/settings.py` (append settings)
- Modify: `.env.yaml.template` (document new vars)
- Test: `tests/test_throttling.py` (new)

**Interfaces:**
- Consumes: `core.settings` (environs pattern: `env.bool/env.int`), `core.utils._cache_db` (module attribute — reference it as `utils._cache_db` at call time so tests patching `core.utils._cache_db` take effect).
- Produces (used by Tasks 2–4):
  - `get_family(stream_type: str | None) -> str`
  - `ThrottledMessage(Exception)` with attributes `destination_id`, `family`, `reason` (`"cooldown" | "rate"`), `retry_after: int | None`
  - `async check_admission(destination_id, stream_type) -> None` (raises `ThrottledMessage` on defer)
  - module constants `EVENTS_FAMILY`, `OBSERVATIONS_FAMILY`, `MESSAGES_FAMILY`, `SITE_SCOPE`, key helpers `_cooldown_key/_level_key/_rate_key`

- [ ] **Step 1: Add settings**

Append to `core/settings.py` (it already has `env = Env()`):

```python
# Per-destination burst throttling (see docs/superpowers/specs/2026-07-06-er-dispatcher-burst-throttling-design.md)
THROTTLING_ENABLED = env.bool("THROTTLING_ENABLED", False)
DEFAULT_MAX_EVENT_DELIVERIES_PER_MINUTE = env.int("DEFAULT_MAX_EVENT_DELIVERIES_PER_MINUTE", 120)
DEFAULT_MAX_OBSERVATION_DELIVERIES_PER_MINUTE = env.int("DEFAULT_MAX_OBSERVATION_DELIVERIES_PER_MINUTE", 300)
DEFAULT_MAX_MESSAGE_DELIVERIES_PER_MINUTE = env.int("DEFAULT_MAX_MESSAGE_DELIVERIES_PER_MINUTE", 60)
THROTTLE_GRACE_WAIT_MAX_SECONDS = env.int("THROTTLE_GRACE_WAIT_MAX_SECONDS", 2)
THROTTLE_COOLDOWN_BASE_SECONDS = env.int("THROTTLE_COOLDOWN_BASE_SECONDS", 30)
THROTTLE_COOLDOWN_MAX_SECONDS = env.int("THROTTLE_COOLDOWN_MAX_SECONDS", 600)
THROTTLE_COOLDOWN_LEVEL_TTL_SECONDS = env.int("THROTTLE_COOLDOWN_LEVEL_TTL_SECONDS", 900)
THROTTLE_NOTIFY_TTL_SECONDS = env.int("THROTTLE_NOTIFY_TTL_SECONDS", 300)
```

Append the same nine variable names with their defaults as comments/entries to `.env.yaml.template` following that file's existing format.

- [ ] **Step 2: Write the failing tests**

Create `tests/test_throttling.py`:

```python
import pytest
from redis import exceptions as redis_exceptions

from core import settings, throttling
from core.throttling import ThrottledMessage


@pytest.fixture
def mock_throttle_db(mocker):
    db = mocker.MagicMock()
    db.ttl.return_value = -2  # no cooldown keys by default
    db.incr.return_value = 1  # first message in the window by default
    mocker.patch("core.utils._cache_db", db)
    return db


@pytest.fixture
def throttling_enabled(mocker):
    mocker.patch.object(settings, "THROTTLING_ENABLED", True)


@pytest.mark.parametrize("stream_type,expected_family", [
    ("ev", "events"),
    ("evu", "events"),
    ("att", "events"),
    ("obv", "observations"),
    ("obvu", "observations"),
    ("txt", "messages"),
    ("something_new", "events"),  # unknown maps to the conservative family
    (None, "events"),
])
def test_get_family_mapping(stream_type, expected_family):
    assert throttling.get_family(stream_type) == expected_family


@pytest.mark.asyncio
async def test_admits_message_under_cap(mock_throttle_db, throttling_enabled):
    await throttling.check_admission(destination_id="dest-1", stream_type="ev")

    rate_call = mock_throttle_db.incr.call_args.args[0]
    assert rate_call.startswith("throttle:rate:dest-1:events:")
    mock_throttle_db.expire.assert_called_once()


@pytest.mark.asyncio
async def test_defers_message_over_cap(mock_throttle_db, throttling_enabled):
    mock_throttle_db.incr.return_value = settings.DEFAULT_MAX_EVENT_DELIVERIES_PER_MINUTE + 1

    with pytest.raises(ThrottledMessage) as exc_info:
        await throttling.check_admission(destination_id="dest-1", stream_type="ev")

    assert exc_info.value.reason == "rate"
    assert exc_info.value.family == "events"
    assert exc_info.value.destination_id == "dest-1"


@pytest.mark.asyncio
async def test_defers_immediately_on_site_cooldown(mock_throttle_db, throttling_enabled):
    # First ttl call is the site scope; a positive TTL means cooling down
    mock_throttle_db.ttl.side_effect = [42]

    with pytest.raises(ThrottledMessage) as exc_info:
        await throttling.check_admission(destination_id="dest-1", stream_type="obv")

    assert exc_info.value.reason == "cooldown"
    assert exc_info.value.retry_after == 42
    mock_throttle_db.incr.assert_not_called()  # never counts against the window


@pytest.mark.asyncio
async def test_defers_on_family_cooldown(mock_throttle_db, throttling_enabled):
    # site scope clear (-2), family scope cooling (45)
    mock_throttle_db.ttl.side_effect = [-2, 45]

    with pytest.raises(ThrottledMessage) as exc_info:
        await throttling.check_admission(destination_id="dest-1", stream_type="ev")

    assert exc_info.value.reason == "cooldown"
    assert exc_info.value.retry_after == 45
    ttl_keys = [c.args[0] for c in mock_throttle_db.ttl.call_args_list]
    assert ttl_keys == ["throttle:cooldown:dest-1:site", "throttle:cooldown:dest-1:events"]


@pytest.mark.asyncio
async def test_families_have_independent_rate_windows(mock_throttle_db, throttling_enabled):
    # events over cap, observations under cap — observations must be admitted
    mock_throttle_db.incr.side_effect = [settings.DEFAULT_MAX_EVENT_DELIVERIES_PER_MINUTE + 1, 1]
    mock_throttle_db.ttl.return_value = -2

    with pytest.raises(ThrottledMessage):
        await throttling.check_admission(destination_id="dest-1", stream_type="ev")
    await throttling.check_admission(destination_id="dest-1", stream_type="obv")

    rate_keys = [c.args[0] for c in mock_throttle_db.incr.call_args_list]
    assert ":events:" in rate_keys[0]
    assert ":observations:" in rate_keys[1]


@pytest.mark.asyncio
async def test_grace_wait_sleeps_then_admits_when_window_is_near(
        mocker, mock_throttle_db, throttling_enabled
):
    # 59s into the minute -> next window opens in 1s (<= grace of 2s)
    mocker.patch("core.throttling.time").time.return_value = 119  # 119 % 60 == 59
    mock_sleep = mocker.patch("core.throttling.asyncio.sleep")
    mock_throttle_db.incr.side_effect = [settings.DEFAULT_MAX_EVENT_DELIVERIES_PER_MINUTE + 1, 1]

    await throttling.check_admission(destination_id="dest-1", stream_type="ev")

    mock_sleep.assert_awaited_once_with(1)
    assert mock_throttle_db.incr.call_count == 2


@pytest.mark.asyncio
async def test_grace_wait_not_applied_to_cooldowns(mocker, mock_throttle_db, throttling_enabled):
    mock_sleep = mocker.patch("core.throttling.asyncio.sleep")
    mock_throttle_db.ttl.side_effect = [1]  # site cooldown with tiny TTL

    with pytest.raises(ThrottledMessage):
        await throttling.check_admission(destination_id="dest-1", stream_type="ev")

    mock_sleep.assert_not_awaited()


@pytest.mark.asyncio
async def test_fails_open_when_redis_unavailable(mock_throttle_db, throttling_enabled):
    mock_throttle_db.ttl.side_effect = redis_exceptions.ConnectionError("boom")

    # Must not raise — throttling is a kindness, not a correctness requirement
    await throttling.check_admission(destination_id="dest-1", stream_type="ev")


@pytest.mark.asyncio
async def test_noop_when_throttling_disabled(mock_throttle_db):
    # THROTTLING_ENABLED defaults to False — no Redis traffic at all
    await throttling.check_admission(destination_id="dest-1", stream_type="ev")

    mock_throttle_db.ttl.assert_not_called()
    mock_throttle_db.incr.assert_not_called()


@pytest.mark.asyncio
async def test_noop_without_destination_id(mock_throttle_db, throttling_enabled):
    await throttling.check_admission(destination_id=None, stream_type="ev")

    mock_throttle_db.ttl.assert_not_called()
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cd /Users/chrisdo/padas/gundi-dispatcher-er && pytest tests/test_throttling.py -v`
Expected: FAIL at import time with `ModuleNotFoundError: No module named 'core.throttling'` (settings attribute errors also acceptable until Step 1 is done — do Step 1 first, then this fails only on the missing module).

- [ ] **Step 4: Implement `core/throttling.py`**

```python
import asyncio
import logging
import time

from redis import exceptions as redis_exceptions

from core import settings
from core import utils

logger = logging.getLogger(__name__)

EVENTS_FAMILY = "events"
OBSERVATIONS_FAMILY = "observations"
MESSAGES_FAMILY = "messages"
SITE_SCOPE = "site"

FAMILY_BY_STREAM_TYPE = {
    "ev": EVENTS_FAMILY,
    "evu": EVENTS_FAMILY,
    "att": EVENTS_FAMILY,  # attachments post to events
    "obv": OBSERVATIONS_FAMILY,
    "obvu": OBSERVATIONS_FAMILY,
    "txt": MESSAGES_FAMILY,
}


class ThrottledMessage(Exception):
    # Raised by check_admission when a message must be deferred. main.py turns
    # this into an HTTP 429 so PubSub nacks and redelivers later.
    def __init__(self, destination_id, family, reason, retry_after=None):
        super().__init__(
            f"Delivery throttled for destination {destination_id} ({family}): {reason}"
        )
        self.destination_id = destination_id
        self.family = family
        self.reason = reason  # "cooldown" | "rate"
        self.retry_after = retry_after


def get_family(stream_type):
    # Unknown stream types map to the most conservative family
    return FAMILY_BY_STREAM_TYPE.get(stream_type, EVENTS_FAMILY)


def _cap_for_family(family):
    return {
        EVENTS_FAMILY: settings.DEFAULT_MAX_EVENT_DELIVERIES_PER_MINUTE,
        OBSERVATIONS_FAMILY: settings.DEFAULT_MAX_OBSERVATION_DELIVERIES_PER_MINUTE,
        MESSAGES_FAMILY: settings.DEFAULT_MAX_MESSAGE_DELIVERIES_PER_MINUTE,
    }[family]


def _cooldown_key(destination_id, scope):
    return f"throttle:cooldown:{destination_id}:{scope}"


def _level_key(destination_id, scope):
    return f"throttle:cooldown_level:{destination_id}:{scope}"


def _rate_key(destination_id, family, window):
    return f"throttle:rate:{destination_id}:{family}:{window}"


def _evaluate(destination_id, family):
    # Returns (admitted, reason, retry_after). Plain commands instead of a Lua
    # script: INCR is atomic, and the check-then-increment race admits at most
    # a few extra messages — acceptable for a kindness cap, and it keeps this
    # module testable against the suite's MagicMock Redis.
    db = utils._cache_db
    for scope in (SITE_SCOPE, family):
        ttl = db.ttl(_cooldown_key(destination_id, scope))
        if ttl and ttl > 0:
            return False, "cooldown", ttl
    now = int(time.time())
    rate_key = _rate_key(destination_id, family, now // 60)
    count = db.incr(rate_key)
    if count == 1:
        # Two windows so a straggler INCR never resurrects an expired key
        db.expire(rate_key, 120)
    if count <= _cap_for_family(family):
        return True, None, None
    return False, "rate", 60 - (now % 60)


async def check_admission(destination_id, stream_type):
    # Raises ThrottledMessage when the message must be deferred (nacked).
    if not settings.THROTTLING_ENABLED or not destination_id:
        return
    family = get_family(stream_type)
    try:
        admitted, reason, retry_after = _evaluate(destination_id, family)
        if admitted:
            return
        if reason == "rate" and retry_after <= settings.THROTTLE_GRACE_WAIT_MAX_SECONDS:
            # The window opens soon: wait it out instead of paying a redelivery
            await asyncio.sleep(retry_after)
            admitted, reason, retry_after = _evaluate(destination_id, family)
            if admitted:
                return
    except redis_exceptions.RedisError as e:
        # Fail open: throttling is a kindness, not a correctness requirement
        logger.warning(f"Throttle gate unavailable, admitting message: {e}")
        return
    logger.info(
        f"Message deferred by throttle gate. destination_id={destination_id}, "
        f"family={family}, reason={reason}, retry_after={retry_after}"
    )
    raise ThrottledMessage(
        destination_id=destination_id, family=family, reason=reason, retry_after=retry_after
    )
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest tests/test_throttling.py -v`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add core/throttling.py core/settings.py .env.yaml.template tests/test_throttling.py
git commit -m "Add per-destination, per-family throttle admission gate

Redis fixed-window counters and cooldown checks keyed by destination and
stream-type family (events/observations/messages), with a short grace
wait near window boundaries, fail-open on Redis errors, and a
THROTTLING_ENABLED kill switch (default off)."
```

---

### Task 2: Distress cooldowns, success reset, notify flag

**Files:**
- Modify: `core/throttling.py` (append)
- Test: `tests/test_throttling.py` (append)

**Interfaces:**
- Consumes: Task 1's key helpers, `get_family`, settings.
- Produces (used by Task 4):
  - `record_distress(destination_id, stream_type, status_code=None, error=None, retry_after=None) -> str | None` — sets/extends the cooldown; returns the scope string (`"site"` or a family name) **only when the portal should be notified** (first cooldown in the notify window), else `None`.
  - `record_success(destination_id, stream_type) -> None` — clears site scope + the delivered family's scope.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_throttling.py`:

```python
def test_429_sets_family_scoped_cooldown(mock_throttle_db, throttling_enabled):
    mock_throttle_db.incr.return_value = 1  # first level
    mock_throttle_db.set.return_value = True  # notify SETNX succeeds

    scope = throttling.record_distress(
        destination_id="dest-1", stream_type="ev", status_code=429,
        error="ERClientRateLimitExceeded: ER Too Many Requests ON POST ...",
    )

    assert scope == "events"
    mock_throttle_db.setex.assert_called_once_with(
        "throttle:cooldown:dest-1:events", settings.THROTTLE_COOLDOWN_BASE_SECONDS, "events"
    )


@pytest.mark.parametrize("status_code,error", [
    (502, "ERClientServiceUnreachable: ER Bad Gateway ON POST ..."),
    (503, "ERClientServiceUnreachable: ER Service Unavailable ON POST ..."),
    (504, "ERClientServiceUnreachable: ER Gateway Timeout ON POST ..."),
    (None, "ERClientException: Request to ER failed: Connection timeout ..."),
])
def test_5xx_and_transport_failures_set_site_cooldown(
        mock_throttle_db, throttling_enabled, status_code, error
):
    mock_throttle_db.incr.return_value = 1
    mock_throttle_db.set.return_value = True

    scope = throttling.record_distress(
        destination_id="dest-1", stream_type="ev", status_code=status_code, error=error,
    )

    assert scope == "site"
    assert mock_throttle_db.setex.call_args.args[0] == "throttle:cooldown:dest-1:site"


@pytest.mark.parametrize("status_code,error", [
    (409, "ERClientRateLimitExceeded: ER Conflict ON POST ..."),  # per-source limit, not site distress
    (400, "ERClientBadRequest: ER Bad Request ON POST ..."),
    (401, "ERClientBadCredentials: ER Unauthorized ON POST ..."),
    (None, "SomeOtherError: unrelated"),
])
def test_non_distress_failures_do_not_set_cooldowns(
        mock_throttle_db, throttling_enabled, status_code, error
):
    scope = throttling.record_distress(
        destination_id="dest-1", stream_type="ev", status_code=status_code, error=error,
    )

    assert scope is None
    mock_throttle_db.setex.assert_not_called()


def test_cooldown_ttl_escalates_and_clamps(mock_throttle_db, throttling_enabled):
    mock_throttle_db.set.return_value = None  # already notified
    base = settings.THROTTLE_COOLDOWN_BASE_SECONDS
    # level (INCR return) -> expected TTL: 1->30, 2->60, 5->480, 6->600 (clamped)
    for level, expected_ttl in [(1, base), (2, base * 2), (5, base * 16),
                                (6, settings.THROTTLE_COOLDOWN_MAX_SECONDS)]:
        mock_throttle_db.reset_mock()
        mock_throttle_db.incr.return_value = level

        throttling.record_distress(
            destination_id="dest-1", stream_type="ev", status_code=429, error="...",
        )

        assert mock_throttle_db.setex.call_args.args[1] == expected_ttl


def test_retry_after_overrides_exponential_ttl_and_is_clamped(
        mock_throttle_db, throttling_enabled
):
    mock_throttle_db.incr.return_value = 1
    mock_throttle_db.set.return_value = None

    throttling.record_distress(
        destination_id="dest-1", stream_type="ev", status_code=429, error="...",
        retry_after=90,
    )
    assert mock_throttle_db.setex.call_args.args[1] == 90

    throttling.record_distress(
        destination_id="dest-1", stream_type="ev", status_code=429, error="...",
        retry_after=9000,  # clamped to the ceiling
    )
    assert mock_throttle_db.setex.call_args.args[1] == settings.THROTTLE_COOLDOWN_MAX_SECONDS


def test_notify_flag_rate_limited_by_setnx(mock_throttle_db, throttling_enabled):
    mock_throttle_db.incr.return_value = 1
    mock_throttle_db.set.return_value = None  # SETNX lost: already notified recently

    scope = throttling.record_distress(
        destination_id="dest-1", stream_type="ev", status_code=429, error="...",
    )

    assert scope is None  # cooldown still set, but no notification due
    mock_throttle_db.setex.assert_called_once()


def test_success_clears_site_and_own_family_only(mock_throttle_db, throttling_enabled):
    throttling.record_success(destination_id="dest-1", stream_type="obv")

    deleted = mock_throttle_db.delete.call_args.args
    assert set(deleted) == {
        "throttle:cooldown:dest-1:site",
        "throttle:cooldown_level:dest-1:site",
        "throttle:cooldown:dest-1:observations",
        "throttle:cooldown_level:dest-1:observations",
    }


def test_record_functions_are_noop_when_disabled(mock_throttle_db):
    assert throttling.record_distress(
        destination_id="dest-1", stream_type="ev", status_code=429, error="...",
    ) is None
    throttling.record_success(destination_id="dest-1", stream_type="ev")

    mock_throttle_db.setex.assert_not_called()
    mock_throttle_db.delete.assert_not_called()


def test_record_functions_tolerate_redis_errors(mock_throttle_db, throttling_enabled):
    mock_throttle_db.incr.side_effect = redis_exceptions.ConnectionError("boom")
    mock_throttle_db.delete.side_effect = redis_exceptions.ConnectionError("boom")

    assert throttling.record_distress(
        destination_id="dest-1", stream_type="ev", status_code=429, error="...",
    ) is None
    throttling.record_success(destination_id="dest-1", stream_type="ev")  # must not raise
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_throttling.py -v -k "record or cooldown_ttl or retry_after or notify or success"`
Expected: FAIL with `AttributeError: module 'core.throttling' has no attribute 'record_distress'`.

- [ ] **Step 3: Implement**

Append to `core/throttling.py`:

```python
SITE_DISTRESS_STATUSES = {502, 503, 504}
TRANSPORT_ERROR_MARKER = "Request to ER failed"  # erclient's wrapper for httpx.RequestError


def _scope_for_failure(status_code, error):
    # 429 is endpoint-level rate limiting -> cool down the family only.
    # 5xx / transport failures are site-wide distress -> cool down everything.
    # 409 is ER's per-source limit ("one obs/sec/source") and must NOT pause
    # the whole destination; other errors are not distress signals.
    if status_code == 429:
        return "family"
    if status_code in SITE_DISTRESS_STATUSES:
        return SITE_SCOPE
    if not status_code and error and TRANSPORT_ERROR_MARKER in error:
        return SITE_SCOPE
    return None


def record_distress(destination_id, stream_type, status_code=None, error=None, retry_after=None):
    # Set/extend a cooldown after a failed delivery. Returns the scope string
    # ("site" or a family name) when the portal should be notified (first
    # cooldown within the notify window), else None.
    if not settings.THROTTLING_ENABLED or not destination_id:
        return None
    scope = _scope_for_failure(status_code, error)
    if not scope:
        return None
    scope_key = get_family(stream_type) if scope == "family" else SITE_SCOPE
    db = utils._cache_db
    try:
        level = db.incr(_level_key(destination_id, scope_key))
        db.expire(
            _level_key(destination_id, scope_key),
            settings.THROTTLE_COOLDOWN_LEVEL_TTL_SECONDS,
        )
        if retry_after:
            ttl = min(int(retry_after), settings.THROTTLE_COOLDOWN_MAX_SECONDS)
        else:
            ttl = min(
                settings.THROTTLE_COOLDOWN_BASE_SECONDS * (2 ** (level - 1)),
                settings.THROTTLE_COOLDOWN_MAX_SECONDS,
            )
        db.setex(_cooldown_key(destination_id, scope_key), ttl, scope_key)
        # One notification per destination per notify window
        notify = db.set(
            f"throttle:notify:{destination_id}", "1",
            ex=settings.THROTTLE_NOTIFY_TTL_SECONDS, nx=True,
        )
        return scope_key if notify else None
    except redis_exceptions.RedisError as e:
        logger.warning(f"Could not record destination distress: {e}")
        return None


def record_success(destination_id, stream_type):
    # A successful delivery proves the site is reachable: clear the site scope
    # and the delivered family's scope. Other families' cooldowns are left to
    # expire — a flowing observation says nothing about the events endpoint.
    if not settings.THROTTLING_ENABLED or not destination_id:
        return
    family = get_family(stream_type)
    db = utils._cache_db
    try:
        db.delete(
            _cooldown_key(destination_id, SITE_SCOPE),
            _level_key(destination_id, SITE_SCOPE),
            _cooldown_key(destination_id, family),
            _level_key(destination_id, family),
        )
    except redis_exceptions.RedisError as e:
        logger.warning(f"Could not clear throttle state after successful delivery: {e}")
```

- [ ] **Step 4: Run the full throttling module tests**

Run: `pytest tests/test_throttling.py -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add core/throttling.py tests/test_throttling.py
git commit -m "Record destination distress cooldowns and success resets

429s cool down only the failing stream family; 5xx and transport
failures cool down the whole site. Exponential TTL (30s..600s) unless
the destination provides Retry-After. Success clears the site scope and
the delivered family; the notify flag is SETNX-rate-limited."
```

---

### Task 3: Wire the gate into `process_request` and return 429 from `main`

**Files:**
- Modify: `core/services.py` (imports + the v2 branch of `process_request`, currently lines ~332-336)
- Modify: `main.py`
- Test: `tests/test_throttling.py` (append integration tests)

**Interfaces:**
- Consumes: `throttling.check_admission(destination_id, stream_type)`, `ThrottledMessage` (Task 1).
- Produces: `main(request)` returns `({"status": "throttled", ...}, 429)` on deferral; `process_request` raises `ThrottledMessage` through to `main`. Age gate ordering guaranteed (admission runs only in the v2 branch, which is after the `is_too_old` early return).

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_throttling.py`:

```python
from core.services import process_request
import main as main_module


@pytest.mark.asyncio
async def test_process_request_defers_v2_message_on_cooldown(
        mocker, mock_throttle_db, throttling_enabled,
        mock_erclient_class, mock_pubsub_client, event_v2_as_pubsub_request
):
    mock_throttle_db.ttl.return_value = 60  # site cooldown active
    mocker.patch("core.dispatchers.AsyncERClient", mock_erclient_class)
    mocker.patch("core.utils.pubsub", mock_pubsub_client)

    with pytest.raises(ThrottledMessage):
        await process_request(event_v2_as_pubsub_request)

    # ER never touched; nothing published (deferrals are silent)
    assert not mock_erclient_class.return_value.post_report.called
    assert not mock_pubsub_client.PublisherClient.return_value.publish.called


@pytest.mark.asyncio
async def test_process_request_admits_v2_message_under_cap(
        mocker, mock_throttle_db, throttling_enabled,
        mock_gundi_client_v2_class, mock_erclient_class,
        mock_pubsub_client, event_v2_as_pubsub_request
):
    # Admission gate uses the same patched _cache_db as the config cache:
    # ttl -> -2 (no cooldown), incr -> 1 (under cap), get -> None (cache miss)
    mock_throttle_db.get.return_value = None
    mocker.patch("core.utils.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("core.dispatchers.AsyncERClient", mock_erclient_class)
    mocker.patch("core.utils.pubsub", mock_pubsub_client)

    await process_request(event_v2_as_pubsub_request)

    assert mock_erclient_class.return_value.post_report.called


@pytest.mark.asyncio
async def test_v1_messages_bypass_the_gate(
        mocker, mock_throttle_db, throttling_enabled,
        mock_cache, mock_gundi_client, mock_erclient_class, mock_pubsub_client,
        position_as_request
):
    mocker.patch("core.utils.portal_v1", mock_gundi_client)
    mocker.patch("core.dispatchers.AsyncERClient", mock_erclient_class)
    mocker.patch("core.utils.pubsub", mock_pubsub_client)

    await process_request(position_as_request)

    mock_throttle_db.ttl.assert_not_called()


@pytest.mark.asyncio
async def test_too_old_messages_dead_letter_before_the_gate(
        mocker, mock_throttle_db, throttling_enabled,
        mock_pubsub_client, mock_publish_event, event_v2_as_pubsub_request_too_old
):
    mock_throttle_db.ttl.return_value = 60  # destination cooling down
    mocker.patch("core.services.pubsub", mock_pubsub_client)
    mocker.patch("core.services.publish_event", mock_publish_event)

    # Must NOT raise ThrottledMessage: the age gate runs first and dead-letters
    await process_request(event_v2_as_pubsub_request_too_old)

    publish_calls = [c for c in mock_pubsub_client.PublisherClient.mock_calls if c[0] == "().publish"]
    assert len(publish_calls) == 1  # DLQ publish happened
    mock_throttle_db.ttl.assert_not_called()


def test_main_returns_429_on_throttled_message(mocker):
    mocker.patch.object(
        main_module, "process_request",
        side_effect=ThrottledMessage(
            destination_id="dest-1", family="events", reason="cooldown", retry_after=42
        ),
    )
    request = mocker.MagicMock()
    request.data = b"{}"
    request.headers = {}

    body, status = main_module.main(request)

    assert status == 429
    assert body["status"] == "throttled"
    assert body["destination_id"] == "dest-1"
    assert body["family"] == "events"
```

Note: `mock_cache` and `mock_gundi_client` are existing v1 fixtures in `tests/conftest.py`; if the v1 test's patch targets differ from the ones used by existing v1 tests in `tests/test_process_observations.py`, copy the patch lines from the first passing v1 test in that file instead — the assertion that matters is `mock_throttle_db.ttl.assert_not_called()`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_throttling.py -v -k "process_request or main_returns or bypass or too_old"`
Expected: the cooldown/admit/v1/too-old tests FAIL (gate not wired, so no `ThrottledMessage` raised / ttl never called is trivially true for v1 — that one may pass; fine). `test_main_returns_429...` FAILS because `main` doesn't catch `ThrottledMessage`.

- [ ] **Step 3: Implement the wiring**

In `core/services.py`, add to the imports block:

```python
from core import throttling
```

Change the version dispatch at the bottom of `process_request` from:

```python
        # Process the event according to the gundi version
        if attributes.get("gundi_version", "v1") == "v2":
            await process_transformer_event_v2(transformed_observation, attributes)
        else:  # Default to v1
            await process_transformed_observation(transformed_observation, attributes)
```

to:

```python
        # Process the event according to the gundi version
        if attributes.get("gundi_version", "v1") == "v2":
            # Admission gate: defer over-cap / cooling-down destinations.
            # Runs after the too-old check above so exhausted messages always
            # dead-letter instead of being nacked past PubSub retention.
            await throttling.check_admission(
                destination_id=attributes.get("destination_id"),
                stream_type=attributes.get("stream_type"),
            )
            await process_transformer_event_v2(transformed_observation, attributes)
        else:  # Default to v1
            await process_transformed_observation(transformed_observation, attributes)
```

Replace `main.py` with:

```python
import asyncio
import logging
from functions_framework import http
from core import tracing
from core.services import process_request
from core.throttling import ThrottledMessage

logger = logging.getLogger(__name__)


@http
def main(request):
    logger.info(f"Request received:\n{request}")
    body = request.data
    headers = request.headers
    print(f"Message Received.\n RAW body: {body}\n headers: {headers}")
    logger.debug(f"Request received:\n{request}")
    try:
        asyncio.run(process_request(request))
    except ThrottledMessage as e:
        # Deferral, not failure: 429 nacks the push message so PubSub
        # redelivers it later. Deliberately no failure event, no activity log.
        logger.info(f"Message deferred by throttle gate, returning 429: {e}")
        return {
            "status": "throttled",
            "destination_id": str(e.destination_id),
            "family": e.family,
        }, 429
    logger.info(f"Request processed successfully.")
    return {}
```

- [ ] **Step 4: Run the module tests, then the full suite**

Run: `pytest tests/test_throttling.py -v && pytest -q`
Expected: throttling module all PASS; full suite passes (existing tests are unaffected because `THROTTLING_ENABLED` defaults to False, making the gate a no-op).

- [ ] **Step 5: Commit**

```bash
git add core/services.py main.py tests/test_throttling.py
git commit -m "Defer throttled v2 messages with an HTTP 429 nack

The admission gate runs after the age gate (exhausted messages must
dead-letter, never nack past retention) and only for v2 messages.
Deferrals are silent: no failure events, no activity logs."
```

---

### Task 4: Distress/success hooks and the cooldown-entry notice

**Files:**
- Modify: `core/event_handlers.py` (exception branch ~line 141 and success branch ~line 193 of `dispatch_transformed_observation_v2`; new helper at module level)
- Test: `tests/test_throttling.py` (append wiring tests)

**Interfaces:**
- Consumes: `throttling.record_distress(...) -> str | None`, `throttling.record_success(...)` (Task 2); existing `publish_event`, `system_events`, `gundi_schemas_v2`, `settings` imports in `event_handlers.py`.
- Produces: `publish_throttling_notice(attributes, scope)` (async, module-level in `event_handlers.py`) publishing a `DispatcherCustomLog` (INFO) to `DISPATCHER_EVENTS_TOPIC`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_throttling.py`:

```python
from erclient import er_errors
from gundi_core import events as system_events


def _make_erclient_raising(mocker, mock_erclient_class, error):
    mock_erclient_class.return_value.post_report.side_effect = error
    return mock_erclient_class


@pytest.mark.asyncio
async def test_429_failure_records_family_distress_and_notifies(
        mocker, mock_throttle_db, throttling_enabled,
        mock_gundi_client_v2_class, mock_erclient_class,
        mock_pubsub_client, mock_publish_event, event_v2_as_pubsub_request
):
    mock_throttle_db.get.return_value = None
    _make_erclient_raising(mocker, mock_erclient_class, er_errors.ERClientRateLimitExceeded(
        message="ER Too Many Requests ON POST https://fake-site.pamdas.org/api/v1.0/activity/events",
        status_code=429, response_body="{}",
    ))
    mock_record = mocker.patch("core.throttling.record_distress", return_value="events")
    mocker.patch("core.utils.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("core.dispatchers.AsyncERClient", mock_erclient_class)
    mocker.patch("core.utils.pubsub", mock_pubsub_client)
    mocker.patch("core.event_handlers.publish_event", mock_publish_event)

    from core.errors import DispatcherException
    with pytest.raises(DispatcherException):
        await process_request(event_v2_as_pubsub_request)

    kwargs = mock_record.call_args.kwargs
    assert kwargs["status_code"] == 429
    assert kwargs["stream_type"] == "ev"
    assert kwargs["retry_after"] is None  # erclient 1.15.0 doesn't expose it yet
    # A cooldown-entry notice AND the regular failure event were published
    published_events = [c.kwargs["event"] for c in mock_publish_event.call_args_list]
    assert any(isinstance(ev, system_events.DispatcherCustomLog) for ev in published_events)
    assert any(isinstance(ev, system_events.ObservationDeliveryFailed) for ev in published_events)


@pytest.mark.asyncio
async def test_no_notice_published_when_already_notified(
        mocker, mock_throttle_db, throttling_enabled,
        mock_gundi_client_v2_class, mock_erclient_class,
        mock_pubsub_client, mock_publish_event, event_v2_as_pubsub_request
):
    mock_throttle_db.get.return_value = None
    _make_erclient_raising(mocker, mock_erclient_class, er_errors.ERClientServiceUnreachable(
        message="ER Service Unavailable ON POST ...", status_code=503, response_body="",
    ))
    mocker.patch("core.throttling.record_distress", return_value=None)  # SETNX lost
    mocker.patch("core.utils.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("core.dispatchers.AsyncERClient", mock_erclient_class)
    mocker.patch("core.utils.pubsub", mock_pubsub_client)
    mocker.patch("core.event_handlers.publish_event", mock_publish_event)

    from core.errors import DispatcherException
    with pytest.raises(DispatcherException):
        await process_request(event_v2_as_pubsub_request)

    published_events = [c.kwargs["event"] for c in mock_publish_event.call_args_list]
    assert not any(isinstance(ev, system_events.DispatcherCustomLog) for ev in published_events)


@pytest.mark.asyncio
async def test_successful_delivery_records_success(
        mocker, mock_throttle_db, throttling_enabled,
        mock_gundi_client_v2_class, mock_erclient_class,
        mock_pubsub_client, event_v2_as_pubsub_request
):
    mock_throttle_db.get.return_value = None
    mock_record_success = mocker.patch("core.throttling.record_success")
    mocker.patch("core.utils.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("core.dispatchers.AsyncERClient", mock_erclient_class)
    mocker.patch("core.utils.pubsub", mock_pubsub_client)

    await process_request(event_v2_as_pubsub_request)

    kwargs = mock_record_success.call_args.kwargs
    assert kwargs["stream_type"] == "ev"
    assert str(kwargs["destination_id"]) == "338225f3-91f9-4fe1-b013-353a229ce504"
```

(The destination UUID is the one in the `event_v2_as_pubsub_request` fixture's attributes.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_throttling.py -v -k "distress or notice or records_success"`
Expected: FAIL — `record_distress`/`record_success` never called (`call_args` is None), and no `DispatcherCustomLog` published.

- [ ] **Step 3: Implement the hooks**

In `core/event_handlers.py`, add to imports:

```python
from core import throttling
from gundi_core.schemas.v2 import LogLevel
```

Add a module-level helper (after the imports, before `dispatch_transformed_observation_v2`):

```python
async def publish_throttling_notice(attributes: dict, scope: str):
    # One INFO-level breadcrumb in the portal activity log when a destination
    # enters cooldown ("why is my data delayed"). INFO never counts toward
    # health thresholds. Failures here must not affect delivery handling.
    if scope == throttling.SITE_SCOPE:
        title = "Deliveries to this destination are temporarily deferred (destination unreachable or overloaded)"
    else:
        title = f"{scope.capitalize()} deliveries to this destination are temporarily deferred (rate limited)"
    try:
        await publish_event(
            event=system_events.DispatcherCustomLog(
                payload=gundi_schemas_v2.CustomDispatcherLog(
                    gundi_id=attributes.get("gundi_id"),
                    related_to=attributes.get("related_to"),
                    data_provider_id=attributes.get("data_provider_id"),
                    destination_id=attributes.get("destination_id"),
                    title=title,
                    level=LogLevel.INFO,
                )
            ),
            topic_name=settings.DISPATCHER_EVENTS_TOPIC,
        )
    except Exception as e:
        logger.exception(f"Error publishing throttling notice: {e}")
```

In `dispatch_transformed_observation_v2`, in the `except Exception as e:` branch, right after `error_msg = f"Exception occurred dispatching observation {gundi_id}: {error}"` (before the failure events are published):

```python
                notify_scope = throttling.record_distress(
                    destination_id=destination_id,
                    stream_type=stream_type,
                    status_code=getattr(e, "status_code", None),
                    error=error,
                    retry_after=getattr(e, "retry_after", None),
                )
                if notify_scope:
                    await publish_throttling_notice(attributes=attributes, scope=notify_scope)
```

In the success `else:` branch, right after `logger.debug(f"Observation {gundi_id} delivered with success. ...")`:

```python
                throttling.record_success(
                    destination_id=destination_id, stream_type=stream_type
                )
```

Note: `dispatch_transformed_observation_v2` doesn't currently receive `attributes` as a local by that name inside the span — it does (`attributes` is the function parameter); use it directly.

- [ ] **Step 4: Run the module tests, then the full suite**

Run: `pytest tests/test_throttling.py -v && pytest -q`
Expected: all PASS (existing failure-path tests unaffected — with `THROTTLING_ENABLED=false`, `record_distress` returns `None` and no notice is published).

- [ ] **Step 5: Commit**

```bash
git add core/event_handlers.py tests/test_throttling.py
git commit -m "Record destination distress and recovery from delivery outcomes

429s cool down the failing stream family, 5xx/transport failures cool
down the site, successes clear the state. On cooldown entry, publish a
rate-limited INFO DispatcherCustomLog so the portal shows why data is
delayed without tripping health alarms."
```

---

### Task 5: Bump erclient pin to v1.15.0

**Files:**
- Modify: `requirements.in:3`
- Modify: `requirements.txt:58`

**Interfaces:**
- Consumes: nothing from other tasks (independent).
- Produces: dispatcher runs against erclient 1.15.0; `getattr(e, "retry_after", None)` in Task 4 starts returning real values only after the Task 6 er-client change is released and pinned (a later, separate bump).

- [ ] **Step 1: Update both pin lines**

In `requirements.in` line 3 and `requirements.txt` line 58, replace:
`https://github.com/PADAS/er-client/releases/download/v1.8.0/earthranger_client-1.8.0-py3-none-any.whl`
with:
`https://github.com/PADAS/er-client/releases/download/v1.15.0/earthranger_client-1.15.0-py3-none-any.whl`
(Asset name verified to exist on the v1.15.0 release. Do not regenerate the rest of `requirements.txt` — a full `pip-compile` refresh is a separate maintenance task.)

- [ ] **Step 2: Install the new client and run the full suite against it**

Run:
```bash
pip install https://github.com/PADAS/er-client/releases/download/v1.15.0/earthranger_client-1.15.0-py3-none-any.whl
pytest -q
```
Expected: install succeeds; full suite passes. (Signatures for the six client methods + `close()` and the `AsyncERClient` constructor were verified identical between 1.8.0 and 1.15.0 during planning; this run is the executable proof.)

- [ ] **Step 3: Commit**

```bash
git add requirements.in requirements.txt
git commit -m "Bump earthranger-client pin to v1.15.0"
```

---

### Task 6: er-client companion — expose Retry-After on transient exceptions

**Files (repo `/Users/chrisdo/padas/er-client`, branch `feature/retry-after-header` created from up-to-date `main`):**
- Modify: `erclient/er_errors.py` (base class `__init__`)
- Modify: `erclient/client.py` (`AsyncERClient._handle_http_status_error`, ~line 1847, plus a module-level parser)
- Create: `tests/async_client/test_retry_after.py`

**Interfaces:**
- Consumes: existing `er_client` fixture (`tests/async_client/conftest.py:22`, service root `https://fake-site.erdomain.org/api/v1.0`); respx test style modeled on `tests/async_client/test_get_source_by_manufacturer_id.py:207` (the existing 429 test).
- Produces: `ERClientException.retry_after: Optional[int]` (None when absent/unparseable); populated by the async status-error handler for all mapped exceptions.

- [ ] **Step 0: Create the branch**

```bash
cd /Users/chrisdo/padas/er-client && git fetch origin main && git checkout -b feature/retry-after-header origin/main
```
If the repo needs a test env: `python3.11 -m venv .venv && .venv/bin/pip install -e . pytest pytest-asyncio respx` and use `.venv/bin/pytest` below.

- [ ] **Step 1: Write the failing tests**

Create `tests/async_client/test_retry_after.py`:

```python
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime

import httpx
import pytest
import respx

from erclient.er_errors import ERClientRateLimitExceeded, ERClientServiceUnreachable


@pytest.mark.asyncio
async def test_retry_after_seconds_on_429(er_client):
    async with respx.mock(base_url=er_client.service_root) as respx_mock:
        respx_mock.post("/activity/events").mock(
            return_value=httpx.Response(429, headers={"Retry-After": "60"}, json={})
        )
        er_client.auth_headers = _fake_auth_headers(er_client)

        with pytest.raises(ERClientRateLimitExceeded) as exc_info:
            await er_client.post_report({"title": "t", "event_type": "x"})

    assert exc_info.value.retry_after == 60


@pytest.mark.asyncio
async def test_retry_after_http_date_on_503(er_client):
    retry_at = datetime.now(timezone.utc) + timedelta(seconds=120)
    async with respx.mock(base_url=er_client.service_root) as respx_mock:
        respx_mock.post("/activity/events").mock(
            return_value=httpx.Response(
                503, headers={"Retry-After": format_datetime(retry_at, usegmt=True)}, json={}
            )
        )
        er_client.auth_headers = _fake_auth_headers(er_client)

        with pytest.raises(ERClientServiceUnreachable) as exc_info:
            await er_client.post_report({"title": "t", "event_type": "x"})

    assert exc_info.value.retry_after is not None
    assert 100 <= exc_info.value.retry_after <= 120


@pytest.mark.asyncio
@pytest.mark.parametrize("headers", [{}, {"Retry-After": "soonish"}, {"Retry-After": "-5"}])
async def test_retry_after_absent_or_unparseable_is_none(er_client, headers):
    async with respx.mock(base_url=er_client.service_root) as respx_mock:
        respx_mock.post("/activity/events").mock(
            return_value=httpx.Response(429, headers=headers, json={})
        )
        er_client.auth_headers = _fake_auth_headers(er_client)

        with pytest.raises(ERClientRateLimitExceeded) as exc_info:
            await er_client.post_report({"title": "t", "event_type": "x"})

    assert exc_info.value.retry_after is None


def _fake_auth_headers(client):
    async def auth_headers():
        return {"Authorization": "Bearer fake"}
    return auth_headers
```

Adaptation note: if existing tests in this suite stub authentication differently (e.g. by mocking the token endpoint with respx instead of replacing `auth_headers`), copy the auth-stubbing pattern from `tests/async_client/test_get_source_by_manufacturer_id.py` — the assertions on `retry_after` are what this task is about. Similarly, if `post_report` hits a different path than `/activity/events` in respx's eyes, mirror whatever route that existing 429 test mocks.

- [ ] **Step 2: Run to verify they fail**

Run: `cd /Users/chrisdo/padas/er-client && pytest tests/async_client/test_retry_after.py -v`
Expected: FAIL with `AttributeError: 'ERClientRateLimitExceeded' object has no attribute 'retry_after'`.

- [ ] **Step 3: Implement**

In `erclient/er_errors.py`, change the base class:

```python
class ERClientException(Exception):

    def __init__(self, message=None, status_code=None, response_body=None, retry_after=None):
        super().__init__(message)
        self.status_code = status_code
        self.response_body = response_body
        # Seconds the server asked us to wait (parsed from the Retry-After
        # header); None when the header was absent or unparseable.
        self.retry_after = retry_after
```

In `erclient/client.py`, add near the other module-level imports:

```python
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
```

(Check for existing imports first — `datetime` may already be imported; don't duplicate.)

Add a module-level helper above `class ERClient`:

```python
def parse_retry_after_header(value):
    """Parse a Retry-After header (delta-seconds or HTTP-date) into seconds, or None."""
    if not value:
        return None
    try:
        seconds = int(value)
        return seconds if seconds >= 0 else None
    except ValueError:
        pass
    try:
        retry_at = parsedate_to_datetime(value)
        seconds = int((retry_at - datetime.now(timezone.utc)).total_seconds())
        return seconds if seconds >= 0 else None
    except (TypeError, ValueError):
        return None
```

In `AsyncERClient._handle_http_status_error` (~line 1847), compute the value once and pass it to both raise sites:

```python
        retry_after = parse_retry_after_header(e.response.headers.get("Retry-After"))

        if e.response.status_code in exception_map:
            raise exception_map[e.response.status_code](
                message=error_details,
                status_code=e.response.status_code,
                response_body=e.response.text,
                retry_after=retry_after,
            )

        raise ERClientException(
            message=error_details,
            status_code=e.response.status_code,
            response_body=e.response.text,
            retry_after=retry_after,
        )
```

Scope note: only the **async** handler changes (it's what the Gundi dispatcher uses). The sync client's inline raises are untouched. Do not bump `erclient/version.py` — releases are the maintainer's process; the PR description should request a release.

- [ ] **Step 4: Run the new tests, then the whole async suite**

Run: `pytest tests/async_client/test_retry_after.py -v && pytest tests/async_client -q`
Expected: new tests PASS; existing async suite stays green (the new constructor kwarg has a default, so all existing raise sites remain valid).

- [ ] **Step 5: Commit**

```bash
git add erclient/er_errors.py erclient/client.py tests/async_client/test_retry_after.py
git commit -m "Expose Retry-After header on ER client exceptions

Parse delta-seconds and HTTP-date forms into retry_after on
ERClientException (async status-error handler), so consumers like the
Gundi ER dispatcher can honor the destination's own backoff guidance."
```

---

### Task 7: Full verification

**Files:** none (verification only)

- [ ] **Step 1: Dispatcher full suite**

Run: `cd /Users/chrisdo/padas/gundi-dispatcher-er && pytest -q`
Expected: ALL PASS (89 pre-existing + ~30 new).

- [ ] **Step 2: er-client async suite**

Run: `cd /Users/chrisdo/padas/er-client && pytest tests/async_client -q`
Expected: ALL PASS.

- [ ] **Step 3: Report**

Summarize: branches (`spec/burst-throttling` in gundi-dispatcher-er, `feature/retry-after-header` in er-client), commits, the dark-launch note (`THROTTLING_ENABLED=false` — enabling is a config action per environment), and the two follow-ups deferred by design: per-destination cap overrides from portal config, and a later erclient pin bump once a release containing `retry_after` exists.

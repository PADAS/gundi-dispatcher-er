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

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

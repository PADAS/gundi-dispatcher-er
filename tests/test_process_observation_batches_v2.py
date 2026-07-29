import base64
import datetime
import json

import pytest

from core import settings
from core.services import process_request
from erclient import ERClientException


def _dispatched_observation_setex_calls(mock_cache):
    # core.utils._cache_db is shared between the dispatched-observation cache
    # (this helper) and get_integration_details' own config-object cache
    # (positional-arg `_cache_db.setex(key, ttl, config.json())` call). Filter
    # to the dispatched-observation writes (kwarg `name="dispatched_observation.*"`)
    # so assertions about per-item caching aren't thrown off by the unrelated
    # integration-details cache write that happens once per envelope.
    return [
        call for call in mock_cache.setex.call_args_list
        if call.kwargs.get("name", "").startswith("dispatched_observation.")
    ]


def _make_batch_request(mocker, items_count=3, provider_key="gundi_movebank_abc123"):
    destination_id = "338225f3-91f9-4fe1-b013-353a229ce504"
    data_provider_id = "ddd0946d-15b0-4308-b93d-e0470b6d33b6"
    items = [
        {
            "gundi_id": f"23ca4b15-18b6-4cf4-9da6-36dd69c6f63{i}",
            "observation": {
                "manufacturer_id": f"device-{i}",
                "source_type": "tracking-device",
                "subject_name": f"subject-{i}",
                "recorded_at": "2026-07-22 11:51:05+00:00",
                "location": {"lon": -72.7, "lat": -51.6},
                "additional": {"speed_kmph": 30},
            },
        }
        for i in range(items_count)
    ]
    envelope = {
        "event_id": "48bd073a-8e35-43cf-91c2-c7b4b87a26d7",
        "timestamp": "2026-07-29 13:23:43.952056+00:00",
        "schema_version": "v1",
        "event_type": "ObservationsBatchTransformedER",
        "payload": {
            "batch_id": "8a5535df-1b9b-412b-9fd5-e29b09582222",
            "data_provider_id": data_provider_id,
            "destination_id": destination_id,
            "provider_key": provider_key,
            "items": items,
        },
    }
    publish_time = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    json_data = {
        "message": {
            "data": base64.b64encode(json.dumps(envelope).encode("utf-8")).decode("utf-8"),
            "attributes": {
                "gundi_version": "v2",
                "batch": "true",
                "batch_count": str(items_count),
                "provider_key": provider_key,
                "stream_type": "obv",
                "destination_id": destination_id,
                "data_provider_id": data_provider_id,
                "tracing_context": "{}",
            },
            "messageId": "11937923011474847",
            "message_id": "11937923011474847",
            "publishTime": publish_time,
            "publish_time": publish_time,
        },
        "subscription": "projects/MY-PROJECT/subscriptions/MY-SUB",
    }
    mock_request = mocker.MagicMock()
    mock_request.headers = {}
    mock_request.data = json.dumps(json_data)
    mock_request.get_json.return_value = json_data
    return mock_request


@pytest.mark.asyncio
async def test_process_observations_batch_posts_one_bulk_request(
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

    await process_request(_make_batch_request(mocker, items_count=3))

    # ONE bulk post for the whole envelope
    post_mock = mock_erclient_class.return_value.post_sensor_observation
    assert post_mock.call_count == 1
    (posted,) = post_mock.call_args.args
    assert isinstance(posted, list)
    assert len(posted) == 3
    # Every item cached as dispatched
    assert len(_dispatched_observation_setex_calls(mock_cache_empty)) == 3
    # One ObservationsBatchDelivered event published
    publish_mock = mock_pubsub_client.PublisherClient.return_value.publish
    assert publish_mock.called


@pytest.mark.asyncio
async def test_batch_respects_er_bulk_size(
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

    await process_request(_make_batch_request(mocker, items_count=3))

    post_mock = mock_erclient_class.return_value.post_sensor_observation
    assert post_mock.call_count == 2  # 2 + 1


@pytest.mark.asyncio
async def test_batch_skips_already_delivered_items(
    mocker,
    mock_gundi_client_v2_class,
    mock_erclient_class,
    mock_pubsub_client,
    dispatched_event,
):
    # First item is a cache hit (already delivered); the other two must post.
    # The leading None accounts for get_integration_details' own cache-miss
    # read on the shared _cache_db mock, which runs before any per-item check.
    mock_cache = mocker.MagicMock()
    mock_cache.get.side_effect = (None, dispatched_event.json(), None, None)
    mocker.patch("core.utils._cache_db", mock_cache)
    mocker.patch("core.utils.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("core.dispatchers.TokenCachingAsyncERClient", mock_erclient_class)
    mocker.patch("core.utils.pubsub", mock_pubsub_client)

    await process_request(_make_batch_request(mocker, items_count=3))

    post_mock = mock_erclient_class.return_value.post_sensor_observation
    (posted,) = post_mock.call_args.args
    assert len(posted) == 2


@pytest.mark.asyncio
async def test_batch_transient_error_raises_for_redelivery(
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
    err = ERClientException("ER error ON POST: service unavailable")
    err.status_code = 503
    mock_erclient_class.return_value.post_sensor_observation.side_effect = err

    with pytest.raises(Exception):
        await process_request(_make_batch_request(mocker, items_count=3))
    # Nothing was cached as delivered
    assert not _dispatched_observation_setex_calls(mock_cache_empty)


@pytest.mark.asyncio
async def test_batch_400_falls_back_to_per_item_and_acks(
    mocker,
    mock_cache_empty,
    mock_gundi_client_v2_class,
    mock_erclient_class,
    mock_pubsub_client,
    post_sensor_observation_response,
):
    mocker.patch("core.utils._cache_db", mock_cache_empty)
    mocker.patch("core.utils.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("core.dispatchers.TokenCachingAsyncERClient", mock_erclient_class)
    mocker.patch("core.utils.pubsub", mock_pubsub_client)

    from tests.conftest import async_return
    bulk_err = ERClientException("ER error ON POST: bad payload")
    bulk_err.status_code = 400
    item_err = ERClientException("ER error ON POST: bad payload")
    item_err.status_code = 400
    post_mock = mock_erclient_class.return_value.post_sensor_observation
    # Bulk call fails with 400; per-item fallback: item0 ok, item1 fails, item2 ok
    post_mock.side_effect = [
        bulk_err,
        async_return(post_sensor_observation_response),
        item_err,
        async_return(post_sensor_observation_response),
    ]

    # Must NOT raise: poison items are individually failed, envelope acks
    await process_request(_make_batch_request(mocker, items_count=3))

    assert post_mock.call_count == 4  # 1 bulk + 3 singles
    assert len(_dispatched_observation_setex_calls(mock_cache_empty)) == 2  # only the two successes cached

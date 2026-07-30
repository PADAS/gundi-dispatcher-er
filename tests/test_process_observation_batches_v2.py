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

    # ONE bulk post for the whole envelope, sent via _post (not
    # post_sensor_observation - see C1 fix note in core/dispatchers.py)
    post_mock = mock_erclient_class.return_value._post
    assert post_mock.call_count == 1
    posted = post_mock.call_args.kwargs["payload"]
    assert isinstance(posted, list)
    assert len(posted) == 3
    assert not mock_erclient_class.return_value.post_sensor_observation.called
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

    post_mock = mock_erclient_class.return_value._post
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

    post_mock = mock_erclient_class.return_value._post
    posted = post_mock.call_args.kwargs["payload"]
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
    mock_erclient_class.return_value._post.side_effect = err

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
    # Bulk call (via _post) fails with 400; per-item fallback (via
    # post_sensor_observation, ERObservationDispatcher's single-item path):
    # item0 ok, item1 fails, item2 ok
    bulk_post_mock = mock_erclient_class.return_value._post
    bulk_post_mock.side_effect = bulk_err
    item_post_mock = mock_erclient_class.return_value.post_sensor_observation
    item_post_mock.side_effect = [
        async_return(post_sensor_observation_response),
        item_err,
        async_return(post_sensor_observation_response),
    ]

    # Must NOT raise: poison items are individually failed, envelope acks
    await process_request(_make_batch_request(mocker, items_count=3))

    assert bulk_post_mock.call_count == 1
    assert item_post_mock.call_count == 3
    assert len(_dispatched_observation_setex_calls(mock_cache_empty)) == 2  # only the two successes cached


class _CloseOnceERClient:
    """Mimics httpx.AsyncClient/erclient's AsyncERClient: once __aexit__ has
    run on an instance, re-entering it (`async with` again) raises
    RuntimeError, exactly like httpx 0.24.1's
    "Cannot reopen a client instance, once it has been closed." This is used
    to prove C2's fix (a fresh dispatcher/client per chunk and per fallback
    item) rather than a permissive MagicMock whose no-op __aexit__ hides the
    reuse-after-close bug entirely.
    """

    def __init__(self, **kwargs):
        self.provider_key = kwargs.get("provider_key")
        self.service_root = "https://fake-site.pamdas.org/api/v1.0"
        self.username = None
        self._closed = False
        self.posted_payloads = []

    async def __aenter__(self):
        if self._closed:
            raise RuntimeError("Cannot reopen a client instance, once it has been closed.")
        return self

    async def __aexit__(self, *args):
        self._closed = True
        return False

    def _clean_observation(self, observation):
        return observation

    async def _post(self, path, payload, params=None):
        self.posted_payloads.append(payload)
        return {"result": "ok"}

    async def post_sensor_observation(self, observation, sensor_type="generic"):
        self.posted_payloads.append(observation)
        return {"result": "ok"}


@pytest.mark.asyncio
async def test_batch_uses_a_fresh_client_per_chunk(
    mocker,
    mock_cache_empty,
    mock_gundi_client_v2_class,
    mock_pubsub_client,
):
    # C2 regression test: a client double that raises if reused after being
    # closed (like real httpx). A fresh ERObservationsBatchDispatcher (and
    # thus a fresh client) must be built per chunk, so chunk 2 must succeed
    # rather than raise RuntimeError on the reused, already-closed client.
    created_clients = []

    def _make_client(**kwargs):
        client = _CloseOnceERClient(**kwargs)
        created_clients.append(client)
        return client

    mocker.patch("core.utils._cache_db", mock_cache_empty)
    mocker.patch("core.utils.GundiClient", mock_gundi_client_v2_class)
    mocker.patch(
        "core.dispatchers.TokenCachingAsyncERClient",
        mocker.MagicMock(side_effect=_make_client),
    )
    mocker.patch("core.utils.pubsub", mock_pubsub_client)
    mocker.patch.object(settings, "ER_BULK_SIZE", 2)

    # 3 items, ER_BULK_SIZE=2 -> 2 chunks (sizes 2 and 1)
    await process_request(_make_batch_request(mocker, items_count=3))

    assert len(created_clients) == 2
    all_posted = [item for client in created_clients for item in client.posted_payloads]
    assert len(all_posted) == 2  # one post per chunk
    assert sum(len(payload) for payload in all_posted) == 3  # 2 + 1 items total
    # Every item cached as dispatched proves both chunks succeeded
    assert len(_dispatched_observation_setex_calls(mock_cache_empty)) == 3


@pytest.mark.asyncio
async def test_batch_400_fallback_uses_a_fresh_client_per_item(
    mocker,
    mock_cache_empty,
    mock_gundi_client_v2_class,
    mock_pubsub_client,
):
    # C2 regression test for the per-item 400 fallback path: each fallback
    # single-item dispatcher must also get a fresh client, otherwise every
    # item after the first raises RuntimeError on the closed client and is
    # wrongly treated as a permanent per-item failure (silent data loss).
    created_clients = []

    def _make_client(**kwargs):
        client = _CloseOnceERClient(**kwargs)
        if not created_clients:
            # First client (the bulk attempt) fails with a permanent 400.
            async def _bulk_post(path, payload, params=None):
                err = ERClientException("ER error ON POST: bad payload")
                err.status_code = 400
                raise err
            client._post = _bulk_post
        created_clients.append(client)
        return client

    mocker.patch("core.utils._cache_db", mock_cache_empty)
    mocker.patch("core.utils.GundiClient", mock_gundi_client_v2_class)
    mocker.patch(
        "core.dispatchers.TokenCachingAsyncERClient",
        mocker.MagicMock(side_effect=_make_client),
    )
    mocker.patch("core.utils.pubsub", mock_pubsub_client)

    # Must NOT raise: all 3 items succeed individually after the bulk 400.
    await process_request(_make_batch_request(mocker, items_count=3))

    # 1 client for the failed bulk attempt + 1 fresh client per fallback item
    assert len(created_clients) == 4
    assert len(_dispatched_observation_setex_calls(mock_cache_empty)) == 3


@pytest.mark.asyncio
async def test_batch_all_cached_redelivery_still_publishes_delivered_event(
    mocker,
    mock_gundi_client_v2_class,
    mock_erclient_class,
    mock_pubsub_client,
    dispatched_event,
):
    # I-trace-stamp-loss regression test: if every item in the batch is
    # already cached as dispatched (e.g. the original attempt died after
    # caching but before publishing ObservationsBatchDelivered), the
    # redelivery must still publish the delivered event for all of them -
    # otherwise the traces never get stamped, even though the data is safely
    # in ER.
    mock_cache = mocker.MagicMock()
    mock_cache.get.return_value = dispatched_event.json()  # every item is a cache hit
    mocker.patch("core.utils._cache_db", mock_cache)
    mocker.patch("core.utils.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("core.dispatchers.TokenCachingAsyncERClient", mock_erclient_class)
    mocker.patch("core.utils.pubsub", mock_pubsub_client)

    await process_request(_make_batch_request(mocker, items_count=3))

    # No ER post at all - everything was skipped as already delivered.
    assert not mock_erclient_class.return_value._post.called
    assert not mock_erclient_class.return_value.post_sensor_observation.called
    # But the delivered event WAS published, for all 3 gundi_ids.
    publish_mock = mock_pubsub_client.PublisherClient.return_value.publish
    assert publish_mock.called
    (binary_payload,), _ = mock_pubsub_client.PubsubMessage.call_args
    published_payload = json.loads(binary_payload)
    assert published_payload["event_type"] == "ObservationsBatchDelivered"
    assert len(published_payload["payload"]["gundi_ids"]) == 3


@pytest.mark.asyncio
async def test_batch_403_is_treated_as_transient_not_permanent(
    mocker,
    mock_cache_empty,
    mock_gundi_client_v2_class,
    mock_erclient_class,
    mock_pubsub_client,
):
    # I-403 regression test: a 403 from ER is typically a recoverable
    # auth/permission condition, not a bad payload. It must be treated like
    # any other transient error (record distress, publish partial progress,
    # raise for redelivery) rather than falling back to per-item posts and
    # permanently failing every item in the chunk.
    mocker.patch("core.utils._cache_db", mock_cache_empty)
    mocker.patch("core.utils.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("core.dispatchers.TokenCachingAsyncERClient", mock_erclient_class)
    mocker.patch("core.utils.pubsub", mock_pubsub_client)
    err = ERClientException("ER error ON POST: forbidden")
    err.status_code = 403
    mock_erclient_class.return_value._post.side_effect = err

    with pytest.raises(Exception):
        await process_request(_make_batch_request(mocker, items_count=3))

    # No per-item fallback attempted, nothing cached as delivered
    assert not mock_erclient_class.return_value.post_sensor_observation.called
    assert not _dispatched_observation_setex_calls(mock_cache_empty)


@pytest.mark.asyncio
async def test_batch_items_cached_with_batch_ttl_not_single_item_ttl(
    mocker,
    mock_cache_empty,
    mock_gundi_client_v2_class,
    mock_erclient_class,
    mock_pubsub_client,
):
    # I-cache-TTL regression test: batch-delivered items must be cached with
    # DISPATCHED_OBSERVATIONS_BATCH_CACHE_TTL (>= the 24h PubSub retry
    # window), not the 1h single-item TTL - otherwise a redelivered envelope
    # can silently re-post already-delivered items once the cache expires
    # mid-retry-window.
    mocker.patch("core.utils._cache_db", mock_cache_empty)
    mocker.patch("core.utils.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("core.dispatchers.TokenCachingAsyncERClient", mock_erclient_class)
    mocker.patch("core.utils.pubsub", mock_pubsub_client)

    await process_request(_make_batch_request(mocker, items_count=3))

    setex_calls = _dispatched_observation_setex_calls(mock_cache_empty)
    assert len(setex_calls) == 3
    for call in setex_calls:
        assert call.kwargs["time"] == settings.DISPATCHED_OBSERVATIONS_BATCH_CACHE_TTL
        assert settings.DISPATCHED_OBSERVATIONS_BATCH_CACHE_TTL >= settings.MAX_EVENT_AGE_SECONDS


@pytest.mark.asyncio
async def test_too_old_batch_publishes_dead_lettered_notice(
    mocker,
    mock_pubsub_client,
    mock_publish_event,
):
    # I-failure-invisibility regression test: publish_retries_exhausted_event
    # requires a gundi_id, which batch envelope attributes don't carry, so it
    # bails out with just a warning log. Without a batch-specific notice, a
    # batch dying at age-out is completely silent - no activity-log entry
    # anywhere, even though up to hundreds of observations were dropped.
    from tests.conftest import _make_request_too_old
    from gundi_core import events as system_events

    mocker.patch("core.services.pubsub", mock_pubsub_client)
    # publish_batch_dead_lettered_notice lives in core.event_handlers and
    # calls the publish_event it imported there - a different name binding
    # than core.services.publish_event, so it must be patched separately.
    mocker.patch("core.event_handlers.publish_event", mock_publish_event)

    request = _make_request_too_old(_make_batch_request(mocker, items_count=5))
    await process_request(request)

    # The envelope was dead-lettered
    publish_calls = [c for c in mock_pubsub_client.PublisherClient.mock_calls if c[0] == "().publish"]
    assert len(publish_calls) == 1
    assert publish_calls[0].args[0] == (
        f"projects/{settings.GCP_PROJECT_ID}/topics/{settings.OBSERVATIONS_DEAD_LETTER_TOPIC}"
    )
    # And a DispatcherCustomLog ERROR notice was published so it's visible
    # in the portal activity log.
    assert mock_publish_event.called
    call_kwargs = mock_publish_event.call_args.kwargs
    event = call_kwargs["event"]
    assert isinstance(event, system_events.DispatcherCustomLog)
    assert event.payload.gundi_id is None
    assert "batch_count=5" in event.payload.title
    from gundi_core.schemas.v2 import LogLevel
    assert event.payload.level == LogLevel.ERROR
    assert call_kwargs["topic_name"] == settings.DISPATCHER_EVENTS_TOPIC

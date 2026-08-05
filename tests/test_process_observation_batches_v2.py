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
    # Delivery recorded in ONE per-envelope progress record, not per-item keys
    assert _dispatched_observation_setex_calls(mock_cache_empty) == []
    progress_calls = _progress_setex_calls(mock_cache_empty)
    assert len(progress_calls) == 1
    assert progress_calls[0].kwargs["value"][8:] == bytes([0b00000111])
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
    # First item is a cache hit via the legacy per-item key (already
    # delivered); the other two must post. Order of gets: config cache,
    # progress record (miss), then one per item for the legacy sweep.
    mock_cache = mocker.MagicMock()
    mock_cache.get.side_effect = (None, None, dispatched_event.json(), None, None)
    mocker.patch("core.utils._cache_db", mock_cache)
    mocker.patch("core.utils.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("core.dispatchers.TokenCachingAsyncERClient", mock_erclient_class)
    mocker.patch("core.utils.pubsub", mock_pubsub_client)

    await process_request(_make_batch_request(mocker, items_count=3))

    post_mock = mock_erclient_class.return_value._post
    posted = post_mock.call_args.kwargs["payload"]
    assert len(posted) == 2
    # The envelope migrates to the new progress-record format, including the
    # legacy-derived bit for item 0.
    assert _progress_setex_calls(mock_cache)[-1].kwargs["value"][8:] == bytes([0b00000111])


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
    assert _dispatched_observation_setex_calls(mock_cache_empty) == []
    # Items 0 and 2 succeeded individually; item 1 failed and keeps its bit
    # unset in the progress record.
    progress_calls = _progress_setex_calls(mock_cache_empty)
    assert len(progress_calls) == 1
    assert progress_calls[-1].kwargs["value"][8:] == bytes([0b00000101])


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
    # One progress flush per chunk, and the final record proves both chunks
    # succeeded (all 3 bits set).
    assert _dispatched_observation_setex_calls(mock_cache_empty) == []
    progress_calls = _progress_setex_calls(mock_cache_empty)
    assert len(progress_calls) == 2
    assert progress_calls[-1].kwargs["value"][8:] == bytes([0b00000111])


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
    assert _dispatched_observation_setex_calls(mock_cache_empty) == []
    # All 3 items delivered individually, recorded in one progress flush
    progress_calls = _progress_setex_calls(mock_cache_empty)
    assert len(progress_calls) == 1
    assert progress_calls[-1].kwargs["value"][8:] == bytes([0b00000111])


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
    # I-cache-TTL regression test: the per-envelope progress record must be
    # cached with DISPATCHED_BATCH_PROGRESS_CACHE_TTL (>= the 24h PubSub
    # retry window), not a shorter single-item TTL - otherwise a redelivered
    # envelope can silently re-post already-delivered items once the record
    # expires mid-retry-window.
    mocker.patch("core.utils._cache_db", mock_cache_empty)
    mocker.patch("core.utils.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("core.dispatchers.TokenCachingAsyncERClient", mock_erclient_class)
    mocker.patch("core.utils.pubsub", mock_pubsub_client)

    await process_request(_make_batch_request(mocker, items_count=3))

    setex_calls = _progress_setex_calls(mock_cache_empty)
    assert len(setex_calls) == 1
    for call in setex_calls:
        assert call.kwargs["time"] == settings.DISPATCHED_BATCH_PROGRESS_CACHE_TTL
        assert settings.DISPATCHED_BATCH_PROGRESS_CACHE_TTL >= settings.MAX_EVENT_AGE_SECONDS


@pytest.mark.asyncio
async def test_batch_items_cached_as_compact_bitmap_not_per_item_keys(
    mocker,
    mock_cache_empty,
    mock_gundi_client_v2_class,
    mock_erclient_class,
    mock_pubsub_client,
):
    # Memory regression test: at 24h+ TTLs with one key per observation per
    # destination, this cache used to dominate Redis during large backfills
    # (see the 2026-08-05 incident: 33.7M keys, 10 GB). Delivery is now
    # recorded in ONE compact fingerprint+bitmap record per envelope instead
    # of N per-item sentinel keys.
    mocker.patch("core.utils._cache_db", mock_cache_empty)
    mocker.patch("core.utils.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("core.dispatchers.TokenCachingAsyncERClient", mock_erclient_class)
    mocker.patch("core.utils.pubsub", mock_pubsub_client)

    await process_request(_make_batch_request(mocker, items_count=3))

    assert _dispatched_observation_setex_calls(mock_cache_empty) == []
    setex_calls = _progress_setex_calls(mock_cache_empty)
    assert len(setex_calls) == 1
    value = setex_calls[0].kwargs["value"]
    assert isinstance(value, bytes)
    # 8-byte fingerprint + 1-byte bitmap for 3 items - far smaller than N
    # per-item JSON records.
    assert len(value) == 9


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
    from tests.conftest import async_return
    err = ERClientException("ER error ON POST: service unavailable")
    err.status_code = 503
    # First chunk's bulk _post must return an awaitable (a coroutine, via
    # async_return) to represent success - a bare None isn't awaitable and
    # would make chunk 1 itself raise, defeating the point of this test.
    mock_erclient_class.return_value._post.side_effect = [async_return(None), err]

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
    from tests.conftest import async_return
    bulk_err = ERClientException("ER error ON POST: bad request")
    bulk_err.status_code = 400
    mock_erclient_class.return_value._post.side_effect = bulk_err
    item_err = ERClientException("ER error: bad record")
    item_err.status_code = 400
    # Successful per-item posts must be awaitables (async_return), not bare
    # None - a bare None isn't awaitable and would make every fallback item
    # raise TypeError, masking the one genuine per-item failure this test
    # is about.
    mock_erclient_class.return_value.post_sensor_observation.side_effect = [
        async_return(None), item_err, async_return(None)
    ]

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

from unittest.mock import ANY

import pytest

from core import settings
from core.services import process_event


@pytest.mark.asyncio
async def test_process_event_v2_successfully(
    mocker,
    mock_cache,
    mock_gundi_client_v2_class,
    mock_erclient_class,
    mock_pubsub_client,
    event_v2_as_cloud_event
):
    # Mock external dependencies
    mocker.patch("core.utils._cache_db", mock_cache)
    mocker.patch("core.utils.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("core.dispatchers.AsyncERClient", mock_erclient_class)
    await process_event(event_v2_as_cloud_event)
    # Check that the report was sent o ER
    assert mock_erclient_class.return_value.__aenter__.called
    assert mock_erclient_class.return_value.post_report.called
    # Check that the trace was written to redis db
    assert mock_cache.setex.called


@pytest.mark.asyncio
async def test_process_attachment_v2_successfully(
    mocker,
    mock_cache_with_cached_event,
    mock_gundi_client_v2_class,
    mock_erclient_class,
    mock_pubsub_client,
    mock_get_cloud_storage,
    dispatched_event,
    attachment_v2_as_cloud_event
):
    # Mock external dependencies
    mocker.patch("core.utils._cache_db", mock_cache_with_cached_event)
    mocker.patch("core.utils.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("core.dispatchers.AsyncERClient", mock_erclient_class)
    mocker.patch("core.dispatchers.get_cloud_storage", mock_get_cloud_storage)
    await process_event(attachment_v2_as_cloud_event)
    # Check that the report was sent o ER
    assert mock_erclient_class.called
    assert mock_erclient_class.return_value.post_report_attachment.called
    # Check that the related event was retrieved from the cache
    assert mock_cache_with_cached_event.get.called
    event_cache_key = f"dispatched_observation.{dispatched_event.gundi_id}.{dispatched_event.destination_id}"
    mock_cache_with_cached_event.get.assert_called_with(event_cache_key)
    # Check that the trace was written to redis db
    assert mock_cache_with_cached_event.setex.called
    attachment_gundi_id = attachment_v2_as_cloud_event.data["message"]["attributes"]["gundi_id"]
    attachment_destination_id = attachment_v2_as_cloud_event.data["message"]["attributes"]["destination_id"]
    attachment_cache_key = f"dispatched_observation.{attachment_gundi_id}.{attachment_destination_id}"
    mock_cache_with_cached_event.setex.assert_called_with(
        name=attachment_cache_key,
        time=settings.DISPATCHED_OBSERVATIONS_CACHE_TTL,
        value=ANY
    )

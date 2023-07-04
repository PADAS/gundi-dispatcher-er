import pytest
from core.services import process_event


@pytest.mark.asyncio
async def test_process_event_v2_successfully(
    mocker,
    mock_cache,
    mock_gundi_client_v2_class,
    mock_erclient_class,
    mock_pubsub_client,
    mock_get_cloud_storage,
    event_v2_as_cloud_event
):
    # Mock external dependencies
    mocker.patch("core.utils._cache_db", mock_cache)
    mocker.patch("core.utils.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("core.dispatchers.AsyncERClient", mock_erclient_class)
    mocker.patch("core.dispatchers.get_cloud_storage", mock_get_cloud_storage)
    await process_event(event_v2_as_cloud_event)
    # Check that the report was sent o ER
    assert mock_erclient_class.return_value.__aenter__.called
    assert mock_erclient_class.return_value.post_report.called
    # Check that the trace was written to redis db
    assert mock_cache.setex.called

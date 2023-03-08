import pytest
from unittest.mock import MagicMock
from .conftest import async_return
from core.services import process_event


@pytest.mark.asyncio
async def test_process_position_successfully(
    mocker,
    mock_cache,
    mock_gundi_client,
    mock_erclient_class,
    mock_pubsub_client,
    position_as_cloud_event,
    outbound_configuration_gcp_pubsub,
):
    # Mock external dependencies
    mock_gundi_client.get_outbound_integration_list.return_value = async_return(
        [outbound_configuration_gcp_pubsub]
    )
    mocker.patch("core.utils._cache_db", mock_cache)
    mocker.patch("core.utils.portal", mock_gundi_client)
    mocker.patch("core.dispatchers.AsyncERClient", mock_erclient_class)
    await process_event(position_as_cloud_event)
    assert mock_erclient_class.called
    assert mock_erclient_class.return_value.post_sensor_observation.called



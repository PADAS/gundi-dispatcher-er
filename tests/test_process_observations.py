import pytest
from .conftest import async_return
from core.services import process_event
from core.errors import ReferenceDataError


@pytest.mark.asyncio
async def test_process_position_successfully(
    mocker,
    mock_cache,
    mock_gundi_client_class,
    mock_erclient_class,
    mock_pubsub_client,
    position_as_cloud_event,
    outbound_configuration_gcp_pubsub,
):
    # Override the mocked config to one that uses pubsub broker
    mock_gundi_client_class.return_value.get_outbound_integration_list.return_value = async_return(
        [outbound_configuration_gcp_pubsub]
    )
    mocker.patch("core.utils._cache_db", mock_cache)
    mocker.patch("core.utils.PortalApi", mock_gundi_client_class)
    mocker.patch("core.dispatchers.AsyncERClient", mock_erclient_class)
    await process_event(position_as_cloud_event)
    assert mock_erclient_class.called
    assert mock_erclient_class.return_value.post_sensor_observation.called


@pytest.mark.asyncio
async def test_process_geoevent_successfully(
    mocker,
    mock_cache,
    mock_gundi_client_class,
    mock_erclient_class,
    mock_pubsub_client,
    geoevent_as_cloud_event,
    outbound_configuration_gcp_pubsub,
):
    # Mock external dependencies
    mock_gundi_client_class.get_outbound_integration_list.return_value = async_return(
        [outbound_configuration_gcp_pubsub]
    )
    mocker.patch("core.utils._cache_db", mock_cache)
    mocker.patch("core.utils.PortalApi", mock_gundi_client_class)
    mocker.patch("core.dispatchers.AsyncERClient", mock_erclient_class)
    await process_event(geoevent_as_cloud_event)
    assert mock_erclient_class.called
    assert mock_erclient_class.return_value.__aenter__.return_value.post_report.called


@pytest.mark.asyncio
async def test_process_cameratrap_event_successfully(
    mocker,
    mock_cache,
    mock_gundi_client_class,
    mock_erclient_class,
    mock_pubsub_client,
    mock_get_cloud_storage,
    cameratrap_event_as_cloud_event,
    outbound_configuration_gcp_pubsub,
):
    # Mock external dependencies
    mock_gundi_client_class.return_value.get_outbound_integration_list.return_value = async_return(
        [outbound_configuration_gcp_pubsub]
    )
    mocker.patch("core.utils._cache_db", mock_cache)
    mocker.patch("core.utils.PortalApi", mock_gundi_client_class)
    mocker.patch("core.dispatchers.AsyncERClient", mock_erclient_class)
    mocker.patch("core.dispatchers.get_cloud_storage", mock_get_cloud_storage)
    #get_cloud_storage
    await process_event(cameratrap_event_as_cloud_event)
    assert mock_erclient_class.called
    assert mock_erclient_class.return_value.post_camera_trap_report.called


@pytest.mark.asyncio
async def test_raise_exception_on_portal_connection_error(
    mocker,
    mock_cache,
    mock_gundi_client_class_with_client_connect_error,
    mock_erclient_class,
    mock_pubsub_client,
    position_as_cloud_event,
    outbound_configuration_gcp_pubsub,
):
    mocker.patch("core.utils._cache_db", mock_cache)
    mocker.patch("core.utils.PortalApi", mock_gundi_client_class_with_client_connect_error)
    mocker.patch("core.dispatchers.AsyncERClient", mock_erclient_class)
    with pytest.raises(Exception) as e_info:
        await process_event(position_as_cloud_event)
    # Check that the right exception type is raised to activate the retry mechanism
    assert e_info.type == ReferenceDataError


@pytest.mark.asyncio
async def test_raise_exception_on_portal_500_error(
    mocker,
    mock_cache,
    mock_gundi_client_class_with_with_500_error,
    mock_erclient_class,
    mock_pubsub_client,
    position_as_cloud_event,
    outbound_configuration_gcp_pubsub,
):
    mocker.patch("core.utils._cache_db", mock_cache)
    mocker.patch("core.utils.PortalApi", mock_gundi_client_class_with_with_500_error)
    mocker.patch("core.dispatchers.AsyncERClient", mock_erclient_class)
    with pytest.raises(Exception) as e_info:
        await process_event(position_as_cloud_event)
    # Check that the right exception type is raised to activate the retry mechanism
    assert e_info.type == ReferenceDataError


@pytest.mark.asyncio
async def test_process_position_with_faulty_cache_successfully(
    mocker,
    mock_cache_with_connection_error,
    mock_gundi_client_class,
    mock_erclient_class,
    mock_pubsub_client,
    position_as_cloud_event,
    outbound_configuration_gcp_pubsub,
):
    # Mock external dependencies
    mock_gundi_client_class.return_value.get_outbound_integration_list.return_value = async_return(
        [outbound_configuration_gcp_pubsub]
    )
    mocker.patch("core.utils._cache_db", mock_cache_with_connection_error)
    mocker.patch("core.utils.PortalApi", mock_gundi_client_class)
    mocker.patch("core.dispatchers.AsyncERClient", mock_erclient_class)
    await process_event(position_as_cloud_event)
    assert mock_erclient_class.called
    assert mock_erclient_class.return_value.post_sensor_observation.called


@pytest.mark.asyncio
async def test_process_geoevent_with_faulty_cache_successfully(
    mocker,
    mock_cache_with_connection_error,
    mock_gundi_client_class,
    mock_erclient_class,
    mock_pubsub_client,
    geoevent_as_cloud_event,
    outbound_configuration_gcp_pubsub,
):
    # Mock external dependencies
    mock_gundi_client_class.return_value.get_outbound_integration_list.return_value = async_return(
        [outbound_configuration_gcp_pubsub]
    )
    mocker.patch("core.utils._cache_db", mock_cache_with_connection_error)
    mocker.patch("core.utils.PortalApi", mock_gundi_client_class)
    mocker.patch("core.dispatchers.AsyncERClient", mock_erclient_class)
    await process_event(geoevent_as_cloud_event)
    assert mock_erclient_class.called
    assert mock_erclient_class.return_value.__aenter__.return_value.post_report.called


@pytest.mark.asyncio
async def test_process_cameratrap_event_with_faulty_cache_successfully(
    mocker,
    mock_cache_with_connection_error,
    mock_gundi_client_class,
    mock_erclient_class,
    mock_pubsub_client,
    mock_get_cloud_storage,
    cameratrap_event_as_cloud_event,
    outbound_configuration_gcp_pubsub,
):
    # Mock external dependencies
    mock_gundi_client_class.return_value.get_outbound_integration_list.return_value = async_return(
        [outbound_configuration_gcp_pubsub]
    )
    mocker.patch("core.utils._cache_db", mock_cache_with_connection_error)
    mocker.patch("core.utils.PortalApi", mock_gundi_client_class)
    mocker.patch("core.dispatchers.AsyncERClient", mock_erclient_class)
    mocker.patch("core.dispatchers.get_cloud_storage", mock_get_cloud_storage)
    #get_cloud_storage
    await process_event(cameratrap_event_as_cloud_event)
    assert mock_erclient_class.called
    assert mock_erclient_class.return_value.post_camera_trap_report.called

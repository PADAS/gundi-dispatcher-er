from unittest.mock import ANY, call

import pytest

from core import settings
from core.errors import DispatcherException
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
    mocker.patch("core.utils.pubsub", mock_pubsub_client)
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
    mocker.patch("core.utils.pubsub", mock_pubsub_client)
    await process_event(attachment_v2_as_cloud_event)
    # Check that the report was sent o ER
    assert mock_erclient_class.called
    assert mock_erclient_class.return_value.post_report_attachment.called
    # Check that the related event was retrieved from the cache
    assert mock_cache_with_cached_event.get.called
    event_cache_key = f"dispatched_observation.{dispatched_event.gundi_id}.{dispatched_event.destination_id}"
    mock_cache_with_cached_event.get.assert_any_call(event_cache_key)
    # Check that the trace was written to redis db
    assert mock_cache_with_cached_event.setex.called
    attachment_gundi_id = attachment_v2_as_cloud_event.data["message"]["attributes"]["gundi_id"]
    attachment_destination_id = attachment_v2_as_cloud_event.data["message"]["attributes"]["destination_id"]
    attachment_cache_key = f"dispatched_observation.{attachment_gundi_id}.{attachment_destination_id}"
    mock_cache_with_cached_event.setex.assert_any_call(
        name=attachment_cache_key,
        time=settings.DISPATCHED_OBSERVATIONS_CACHE_TTL,
        value=ANY
    )


@pytest.mark.asyncio
async def test_process_observation_v2_successfully(
    mocker,
    mock_cache,
    mock_gundi_client_v2_class,
    mock_erclient_class,
    mock_pubsub_client,
    observation_v2_as_cloud_event
):
    # Mock external dependencies
    mocker.patch("core.utils._cache_db", mock_cache)
    mocker.patch("core.utils.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("core.dispatchers.AsyncERClient", mock_erclient_class)
    mocker.patch("core.utils.pubsub", mock_pubsub_client)
    await process_event(observation_v2_as_cloud_event)
    # Check that the config was retrieved from the portal
    assert mock_gundi_client_v2_class.return_value.get_integration_details.called
    # Check that the observation was sent o ER
    assert mock_erclient_class.return_value.__aenter__.called
    assert mock_erclient_class.return_value.post_sensor_observation.called
    # Check that the trace was written to redis db
    assert mock_cache.setex.called


@pytest.mark.asyncio
async def test_system_event_is_published_on_successful_delivery(
    mocker,
    mock_cache,
    mock_gundi_client,
    mock_erclient_class,
    mock_pubsub_client,
    mock_gundi_client_v2_class,
    event_v2_as_cloud_event,
    observation_delivered_pubsub_message
):

    # Mock external dependencies
    mocker.patch("core.utils._cache_db", mock_cache)
    mocker.patch("core.utils.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("core.dispatchers.AsyncERClient", mock_erclient_class)
    mocker.patch("core.utils.pubsub", mock_pubsub_client)
    await process_event(event_v2_as_cloud_event)
    # Check that the report was sent o ER
    assert mock_erclient_class.return_value.__aenter__.called
    assert mock_erclient_class.return_value.post_report.called
    # Check that the right event was published to the right pubsub topic
    assert mock_pubsub_client.PublisherClient.called
    assert mock_pubsub_client.PubsubMessage.called
    assert mock_pubsub_client.PublisherClient.called
    assert mock_pubsub_client.PublisherClient.return_value.publish.called
    mock_pubsub_client.PublisherClient.return_value.publish.assert_any_call(
        f"projects/{settings.GCP_PROJECT_ID}/topics/{settings.DISPATCHER_EVENTS_TOPIC}",
        [observation_delivered_pubsub_message]
    )


@pytest.mark.asyncio
async def test_system_event_is_published_on_delivery_failure(
    mocker,
    mock_cache,
    mock_gundi_client,
    mock_erclient_class_with_service_unavailable_error,
    mock_pubsub_client_with_observation_delivery_failure,
    mock_gundi_client_v2_class,
    event_v2_as_cloud_event,
    observation_delivery_failure_pubsub_message
):
    # Mock external dependencies
    mocker.patch("core.utils._cache_db", mock_cache)
    mocker.patch("core.utils.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("core.dispatchers.AsyncERClient", mock_erclient_class_with_service_unavailable_error)
    mocker.patch("core.utils.pubsub", mock_pubsub_client_with_observation_delivery_failure)
    # Check that the dispatcher raises an exception so the message is retried later
    with pytest.raises(DispatcherException):
        await process_event(event_v2_as_cloud_event)
    # Check that the call to send the report to ER was made
    assert mock_erclient_class_with_service_unavailable_error.return_value.__aenter__.called
    assert mock_erclient_class_with_service_unavailable_error.return_value.post_report.called
    # Check that an event was published to the right pubsub topic to inform other services about the error
    assert mock_pubsub_client_with_observation_delivery_failure.PublisherClient.called
    assert mock_pubsub_client_with_observation_delivery_failure.PubsubMessage.called
    assert mock_pubsub_client_with_observation_delivery_failure.PublisherClient.called
    assert mock_pubsub_client_with_observation_delivery_failure.PublisherClient.return_value.publish.called
    mock_pubsub_client_with_observation_delivery_failure.PublisherClient.return_value.publish.assert_any_call(
        f"projects/{settings.GCP_PROJECT_ID}/topics/{settings.DISPATCHER_EVENTS_TOPIC}",
        [observation_delivery_failure_pubsub_message]
    )

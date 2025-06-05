import base64
import json
from unittest.mock import ANY, call
import pytest
from erclient import er_errors

from gundi_core import schemas
from core import settings
from core.errors import DispatcherException
from core.utils import get_dispatched_observation
from core.services import process_request


@pytest.mark.parametrize("event_v2", [
    "event_v2_as_pubsub_request",
    "event_v2_as_eventarc_request",
    "event_v2_with_state_as_pubsub_request"
])
@pytest.mark.asyncio
async def test_process_event_v2_successfully(
    mocker,
    request,
    mock_cache_empty,
    mock_gundi_client_v2_class,
    mock_erclient_class,
    mock_pubsub_client,
    event_v2
):
    event_v2 = request.getfixturevalue(event_v2)
    # Mock external dependencies
    mocker.patch("core.utils._cache_db", mock_cache_empty)
    mocker.patch("core.utils.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("core.dispatchers.AsyncERClient", mock_erclient_class)
    mocker.patch("core.utils.pubsub", mock_pubsub_client)
    await process_request(event_v2)
    # Check that the report was sent o ER
    assert mock_erclient_class.return_value.__aenter__.called
    mock_post_report = mock_erclient_class.return_value.post_report
    assert mock_post_report.called
    # Check that the trace was written to redis db
    assert mock_cache_empty.setex.called
    # Check the payload sent to ER
    post_report_data = mock_post_report.call_args.kwargs["data"]
    cloud_event_data = event_v2.get_json()["message"]["data"]
    decoded_system_event = json.loads(base64.b64decode(cloud_event_data))
    event_v2_data = decoded_system_event.get("payload")
    er_event = schemas.v2.EREvent(**event_v2_data)
    serialized_event = json.loads(er_event.json(exclude_none=True, exclude_unset=True))
    assert post_report_data == serialized_event



@pytest.mark.parametrize("event_update_v2", [
    "event_update_v2_as_pubsub_request",
    "event_update_v2_as_eventarc_event",
    "event_update_v2_with_state_as_pubsub_request",
])
@pytest.mark.asyncio
async def test_process_event_update_v2_successfully(
    mocker,
    request,
    mock_cache_empty,
    mock_gundi_client_v2_class,
    mock_erclient_class,
    mock_pubsub_client,
    event_update_v2,
    dispatched_event_trace
):
    event_update_v2 = request.getfixturevalue(event_update_v2)
    # Mock external dependencies
    mocker.patch("core.utils._cache_db", mock_cache_empty)
    mocker.patch("core.utils.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("core.dispatchers.AsyncERClient", mock_erclient_class)
    mocker.patch("core.utils.pubsub", mock_pubsub_client)
    await process_request(event_update_v2)
    # Check that the report was patched in ER
    assert mock_erclient_class.return_value.__aenter__.called
    mock_patch_report = mock_erclient_class.return_value.patch_report
    assert mock_patch_report.called
    # Check the payload sent to ER
    patch_report_data = mock_patch_report.call_args.kwargs["data"]
    patch_report_id = mock_patch_report.call_args.kwargs["event_id"]
    cloud_event_data = event_update_v2.get_json()["message"]["data"]
    decoded_system_event = json.loads(base64.b64decode(cloud_event_data))
    event_v2_data = decoded_system_event.get("payload")
    assert patch_report_id == dispatched_event_trace.external_id
    assert patch_report_data == event_v2_data.get("changes")


@pytest.mark.asyncio
async def test_process_attachment_v2_successfully(
    mocker,
    mock_cache_with_one_miss_then_hit,
    mock_gundi_client_v2_class,
    mock_erclient_class,
    mock_pubsub_client,
    mock_get_cloud_storage,
    dispatched_event,
    attachment_v2_as_request
):
    # Mock external dependencies
    mocker.patch("core.utils._cache_db", mock_cache_with_one_miss_then_hit)
    mocker.patch("core.utils.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("core.dispatchers.AsyncERClient", mock_erclient_class)
    mocker.patch("core.dispatchers.get_cloud_storage", mock_get_cloud_storage)
    mocker.patch("core.utils.pubsub", mock_pubsub_client)
    await process_request(attachment_v2_as_request)
    # Check that the report was sent o ER
    assert mock_erclient_class.called
    assert mock_erclient_class.return_value.post_report_attachment.called
    # Check that the related event was retrieved from the cache
    assert mock_cache_with_one_miss_then_hit.get.called
    event_cache_key = f"dispatched_observation.{dispatched_event.gundi_id}.{dispatched_event.destination_id}"
    mock_cache_with_one_miss_then_hit.get.assert_any_call(event_cache_key)
    # Check that the trace was written to redis db
    assert mock_cache_with_one_miss_then_hit.setex.called
    request_data = attachment_v2_as_request.get_json()
    attachment_gundi_id = request_data["message"]["attributes"]["gundi_id"]
    attachment_destination_id = request_data["message"]["attributes"]["destination_id"]
    attachment_cache_key = f"dispatched_observation.{attachment_gundi_id}.{attachment_destination_id}"
    mock_cache_with_one_miss_then_hit.setex.assert_any_call(
        name=attachment_cache_key,
        time=settings.DISPATCHED_OBSERVATIONS_CACHE_TTL,
        value=ANY
    )


@pytest.mark.asyncio
async def test_process_observation_v2_successfully(
    mocker,
    mock_cache_empty,
    mock_gundi_client_v2_class,
    mock_erclient_class,
    mock_pubsub_client,
    observation_v2_as_request
):
    # Mock external dependencies
    mocker.patch("core.utils._cache_db", mock_cache_empty)
    mocker.patch("core.utils.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("core.dispatchers.AsyncERClient", mock_erclient_class)
    mocker.patch("core.utils.pubsub", mock_pubsub_client)
    await process_request(observation_v2_as_request)
    # Check that the config was retrieved from the portal
    assert mock_gundi_client_v2_class.return_value.get_integration_details.called
    # Check that the observation was sent o ER
    assert mock_erclient_class.return_value.__aenter__.called
    assert mock_erclient_class.return_value.post_sensor_observation.called
    # Check that the trace was written to redis db
    assert mock_cache_empty.setex.called


@pytest.mark.asyncio
async def test_process_text_message_successfully(
    mocker,
    mock_cache_empty,
    mock_gundi_client_v2_class,
    mock_erclient_class,
    mock_pubsub_client,
    text_message_as_pubsub_request
):
    # Mock external dependencies
    mocker.patch("core.utils._cache_db", mock_cache_empty)
    mocker.patch("core.utils.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("core.dispatchers.AsyncERClient", mock_erclient_class)
    mocker.patch("core.utils.pubsub", mock_pubsub_client)
    await process_request(text_message_as_pubsub_request)
    # Check that the config was retrieved from the portal
    assert mock_gundi_client_v2_class.return_value.get_integration_details.called
    # Check that the message was sent o ER with the right schema and params
    assert mock_erclient_class.return_value.post_message.called
    expected_message = {
        "message_type": "inbox",
        "text": "Assistance needed, please respond.",
        "message_time": "2025-06-05T04:07:37.401000-07:00",
        "device_location": {
            "lon": -72.704459,
            "lat": -51.688246
        },
        "additional": {
            "status": {
                "autonomous": 0,
                "lowBattery": 1,
                "intervalChange": 0,
                "resetDetected": 0
            }
        }
    }
    expected_manufacturer_id = "2075752244"
    mock_erclient_class.return_value.post_message.assert_called_once_with(
        message=expected_message,
        params={"manufacturer_id": expected_manufacturer_id}
    )
    # Check that the trace was written to redis db
    assert mock_cache_empty.setex.called
    assert mock_erclient_class.return_value.__aenter__.called



@pytest.mark.asyncio
async def test_system_event_is_published_on_successful_delivery(
    mocker,
    mock_cache_empty,
    mock_gundi_client,
    mock_erclient_class,
    mock_pubsub_client,
    mock_gundi_client_v2_class,
    event_v2_as_pubsub_request,
    observation_delivered_pubsub_message
):

    # Mock external dependencies
    mocker.patch("core.utils._cache_db", mock_cache_empty)
    mocker.patch("core.utils.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("core.dispatchers.AsyncERClient", mock_erclient_class)
    mocker.patch("core.utils.pubsub", mock_pubsub_client)
    await process_request(event_v2_as_pubsub_request)
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


@pytest.mark.parametrize(
    "mock_erclient_class_with_error,er_error",
    [
        ("missing_event_type", er_errors.ERClientBadRequest),
        ("bad_credentials", er_errors.ERClientBadCredentials),
        ("missing_permissions", er_errors.ERClientPermissionDenied),
        ("service_internal_error", er_errors.ERClientInternalError),
        ("service_unreachable_502", er_errors.ERClientServiceUnreachable),
        ("service_unreachable_503", er_errors.ERClientServiceUnreachable),
    ],
    indirect=["mock_erclient_class_with_error"])
@pytest.mark.asyncio
async def test_system_event_is_published_on_delivery_failure(
    mocker,
    mock_cache_empty,
    mock_gundi_client,
    mock_erclient_class_with_error,
    er_error,
    mock_pubsub_client_with_observation_delivery_failure,
    mock_gundi_client_v2_class,
    event_v2_as_pubsub_request,
    observation_delivery_failure_pubsub_message
):
    # Mock external dependencies
    mocker.patch("core.utils._cache_db", mock_cache_empty)
    mocker.patch("core.utils.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("core.dispatchers.AsyncERClient", mock_erclient_class_with_error)
    mocker.patch("core.utils.pubsub", mock_pubsub_client_with_observation_delivery_failure)
    # Check that the dispatcher raises an exception so the message is retried later
    with pytest.raises(DispatcherException):
        await process_request(event_v2_as_pubsub_request)
    # Check that the call to send the report to ER was made
    assert mock_erclient_class_with_error.return_value.__aenter__.called
    assert mock_erclient_class_with_error.return_value.post_report.called
    # Check that an event was published to the right pubsub topic to inform other services about the error
    assert mock_pubsub_client_with_observation_delivery_failure.PublisherClient.called
    assert mock_pubsub_client_with_observation_delivery_failure.PubsubMessage.called
    assert mock_pubsub_client_with_observation_delivery_failure.PublisherClient.called
    assert mock_pubsub_client_with_observation_delivery_failure.PublisherClient.return_value.publish.called
    mock_pubsub_client_with_observation_delivery_failure.PublisherClient.return_value.publish.assert_any_call(
        f"projects/{settings.GCP_PROJECT_ID}/topics/{settings.DISPATCHER_EVENTS_TOPIC}",
        [observation_delivery_failure_pubsub_message]
    )


@pytest.mark.asyncio
async def test_process_event_v2_with_custom_provider_key(
    mocker,
    mock_cache_empty,
    mock_gundi_client_v2_class,
    mock_erclient_class,
    mock_pubsub_client,
    event_v2_with_provider_key_as_request
):
    # Mock external dependencies
    mocker.patch("core.utils._cache_db", mock_cache_empty)
    mocker.patch("core.utils.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("core.dispatchers.AsyncERClient", mock_erclient_class)
    mocker.patch("core.utils.pubsub", mock_pubsub_client)
    await process_request(event_v2_with_provider_key_as_request)
    # Check that the report was sent o ER
    assert mock_erclient_class.return_value.__aenter__.called
    assert mock_erclient_class.return_value.post_report.called
    # Check that the trace was written to redis db
    assert mock_cache_empty.setex.called


@pytest.mark.asyncio
async def test_process_observation_v2_with_custom_provider_key(
    mocker,
    mock_cache_empty,
    mock_gundi_client_v2_class,
    mock_erclient_class,
    mock_pubsub_client,
    observation_v2_with_provider_key_as_request
):
    # Mock external dependencies
    mocker.patch("core.utils._cache_db", mock_cache_empty)
    mocker.patch("core.utils.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("core.dispatchers.AsyncERClient", mock_erclient_class)
    mocker.patch("core.utils.pubsub", mock_pubsub_client)
    await process_request(observation_v2_with_provider_key_as_request)
    # Check that the config was retrieved from the portal
    assert mock_gundi_client_v2_class.return_value.get_integration_details.called
    # Check that the observation was sent o ER
    assert mock_erclient_class.return_value.__aenter__.called
    assert mock_erclient_class.return_value.post_sensor_observation.called
    # Check that the trace was written to redis db
    assert mock_cache_empty.setex.called


@pytest.mark.asyncio
async def test_raise_exception_on_internal_exception(
        mocker,
        mock_cache_empty,
        mock_erclient_class,
        mock_pubsub_client,
        mock_gundi_client_v2_class_with_internal_exception,
        event_v2_as_pubsub_request,
):
    # Mock external dependencies
    mocker.patch("core.utils._cache_db", mock_cache_empty)
    mocker.patch("core.utils.GundiClient", mock_gundi_client_v2_class_with_internal_exception)
    mocker.patch("core.dispatchers.AsyncERClient", mock_erclient_class)
    mocker.patch("core.utils.pubsub", mock_pubsub_client)

    # Check that the dispatcher raises an exception so the message is retried later
    with pytest.raises(Exception):
        await process_request(event_v2_as_pubsub_request)


@pytest.mark.asyncio
async def test_get_dispatched_observation_from_cache(
    mocker,
    mock_cache_with_cached_event,
    dispatched_event,
):
    # Mock external dependencies
    mocker.patch("core.utils._cache_db", mock_cache_with_cached_event)

    # Check that the event is retrieved from the cache
    event = await get_dispatched_observation(gundi_id=dispatched_event.gundi_id, destination_id=dispatched_event.destination_id)
    assert event == dispatched_event
    event_cache_key = f"dispatched_observation.{dispatched_event.gundi_id}.{dispatched_event.destination_id}"
    mock_cache_with_cached_event.get.assert_called_once_with(event_cache_key)


@pytest.mark.asyncio
async def test_get_dispatched_observation_from_portal(
    mocker,
    mock_cache_empty,
    mock_gundi_client_v2_class,
    dispatched_event,
):
    # Mock external dependencies
    mocker.patch("core.utils._cache_db", mock_cache_empty)
    mocker.patch("core.utils.GundiClient", mock_gundi_client_v2_class)
    # Check that the event is retrieved from the cache
    event = await get_dispatched_observation(gundi_id=dispatched_event.gundi_id, destination_id=dispatched_event.destination_id)
    assert event == dispatched_event
    event_cache_key = f"dispatched_observation.{dispatched_event.gundi_id}.{dispatched_event.destination_id}"
    mock_cache_empty.get.assert_called_once_with(event_cache_key)
    mock_gundi_client_v2_class.return_value.get_traces.assert_called_once_with(
        params={"object_id": dispatched_event.gundi_id, "destination": dispatched_event.destination_id}
    )

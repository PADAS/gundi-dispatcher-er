import asyncio
import time

import pytest

from core import settings
from core.services import send_observation_to_dead_letter_topic, process_request
from gundi_core import events as system_events


@pytest.mark.parametrize(
    "transformed_observation,attributes,expected_topic",
    [
        ("event_v2_transformed_er_as_dict", "event_v2_attributes", settings.EVENTS_DEAD_LETTER_TOPIC),
        ("event_update_v2_transformed_er_as_dict", "event_update_v2_attributes", settings.EVENTS_UPDATES_DEAD_LETTER_TOPIC),
        ("attachment_v2_transformed_er_as_dict", "attachment_v2_attributes", settings.ATTACHMENTS_DEAD_LETTER_TOPIC),
        ("observations_v2_transformed_er_as_dict", "observation_v2_attributes", settings.OBSERVATIONS_DEAD_LETTER_TOPIC),
        ("text_message_v2_as_dict", "text_message_v2_attributes", settings.TEXT_MESSAGES_DEAD_LETTER_TOPIC),
    ]
)
@pytest.mark.asyncio
async def test_send_observation_v2_to_dead_letter_topic(
        mocker,
        request,
        mock_pubsub_client,
        transformed_observation,
        attributes,
        expected_topic
):
    transformed_observation = request.getfixturevalue(transformed_observation)
    attributes = request.getfixturevalue(attributes)
    mocker.patch("core.services.pubsub", mock_pubsub_client)

    await send_observation_to_dead_letter_topic(transformed_observation, attributes)

    # Check that the message was published to the expected topic
    mock_pubsub_publisher = mock_pubsub_client.PublisherClient
    publish_calls = [c for c in mock_pubsub_publisher.mock_calls if c[0] == "().publish"]
    assert len(publish_calls) == 1, "Expected one publish call to the dead letter topic"
    call = publish_calls[0]
    assert call.args[0] == f"projects/{settings.GCP_PROJECT_ID}/topics/{expected_topic}"


@pytest.mark.asyncio
async def test_too_old_v2_event_publishes_retries_exhausted_error(
        mocker, mock_pubsub_client, mock_publish_event, event_v2_as_pubsub_request_too_old
):
    mocker.patch("core.services.pubsub", mock_pubsub_client)
    mocker.patch("core.services.publish_event", mock_publish_event)

    await process_request(event_v2_as_pubsub_request_too_old)

    # The message was sent to the events DLQ topic
    publish_calls = [c for c in mock_pubsub_client.PublisherClient.mock_calls if c[0] == "().publish"]
    assert len(publish_calls) == 1
    assert publish_calls[0].args[0] == f"projects/{settings.GCP_PROJECT_ID}/topics/{settings.EVENTS_DEAD_LETTER_TOPIC}"
    # And a retries-exhausted failure event was published for the portal
    assert mock_publish_event.called
    call_kwargs = mock_publish_event.call_args.kwargs
    event = call_kwargs["event"]
    assert isinstance(event, system_events.ObservationDeliveryFailed)
    assert "retries exhausted" in event.payload.error.lower()
    assert event.payload.server_response_status is None
    assert str(event.payload.observation.gundi_id) == "23ca4b15-18b6-4cf4-9da6-36dd69c6f638"
    assert call_kwargs["topic_name"] == settings.DISPATCHER_EVENTS_TOPIC


@pytest.mark.asyncio
async def test_too_old_v2_event_update_publishes_update_failed_error(
        mocker, mock_pubsub_client, mock_publish_event, event_update_v2_as_pubsub_request_too_old
):
    mocker.patch("core.services.pubsub", mock_pubsub_client)
    mocker.patch("core.services.publish_event", mock_publish_event)

    await process_request(event_update_v2_as_pubsub_request_too_old)

    assert mock_publish_event.called
    event = mock_publish_event.call_args.kwargs["event"]
    assert isinstance(event, system_events.ObservationUpdateFailed)
    assert "retries exhausted" in event.payload.error.lower()


@pytest.mark.asyncio
async def test_too_old_v1_message_does_not_publish_failure_event(
        mocker, mock_pubsub_client, mock_publish_event, position_as_request_too_old
):
    mocker.patch("core.services.pubsub", mock_pubsub_client)
    mocker.patch("core.services.publish_event", mock_publish_event)

    await process_request(position_as_request_too_old)

    # v1 messages go to the legacy DLQ with no portal event (deprecated path)
    publish_calls = [c for c in mock_pubsub_client.PublisherClient.mock_calls if c[0] == "().publish"]
    assert len(publish_calls) == 1
    assert publish_calls[0].args[0] == f"projects/{settings.GCP_PROJECT_ID}/topics/{settings.LEGACY_DEAD_LETTER_TOPIC}"
    assert not mock_publish_event.called


@pytest.mark.asyncio
async def test_failure_publishing_retries_exhausted_event_does_not_raise(
        mocker, mock_pubsub_client, mock_publish_event, event_v2_as_pubsub_request_too_old
):
    mocker.patch("core.services.pubsub", mock_pubsub_client)
    mock_publish_event.side_effect = Exception("PubSub is down")
    mocker.patch("core.services.publish_event", mock_publish_event)

    # Must not raise: the DLQ send already succeeded and the message must be acked
    await process_request(event_v2_as_pubsub_request_too_old)


@pytest.mark.asyncio
async def test_slow_event_publish_cannot_delay_the_dlq_ack(
        mocker, mock_pubsub_client, event_v2_as_pubsub_request_too_old
):
    # publish_event retries with backoff (worst case ~65s); if the events
    # topic is down, the retries-exhausted notification must be cut off by
    # RETRIES_EXHAUSTED_PUBLISH_TIMEOUT_SECONDS so the function can return
    # (ack) before the platform timeout would kill it.
    async def hanging_publish(**kwargs):
        await asyncio.sleep(30)

    mocker.patch("core.services.pubsub", mock_pubsub_client)
    mocker.patch("core.services.publish_event", side_effect=hanging_publish)
    mocker.patch.object(settings, "RETRIES_EXHAUSTED_PUBLISH_TIMEOUT_SECONDS", 0.05)

    start = time.monotonic()
    # Must neither raise nor hang: the DLQ send already happened and the
    # message must be acked
    await process_request(event_v2_as_pubsub_request_too_old)
    assert time.monotonic() - start < 5

import pytest

from core import settings
from core.services import send_observation_to_dead_letter_topic


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

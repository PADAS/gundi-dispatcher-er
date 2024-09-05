import json
import logging
import aiohttp
from datetime import datetime, timezone, timedelta
from gcloud.aio import pubsub
from opentelemetry.trace import SpanKind
from core import dispatchers
from core.utils import (
    extract_fields_from_message,
    get_inbound_integration_detail,
    get_outbound_config_detail,
    ExtraKeys,
)
from .errors import DispatcherException, ReferenceDataError
from . import tracing
from . import settings
from .event_handlers import event_handlers, event_schemas


logger = logging.getLogger(__name__)


async def dispatch_transformed_observation(
    stream_type: str, outbound_config_id: str, inbound_int_id: str, observation
):
    extra_dict = {
        ExtraKeys.OutboundIntId: outbound_config_id,
        ExtraKeys.InboundIntId: inbound_int_id,
        ExtraKeys.Observation: observation,
        ExtraKeys.StreamType: stream_type,
    }

    if not outbound_config_id or not inbound_int_id:
        logger.error(
            f"Missing outbound config or inbound integration id",
            extra=extra_dict,
        )

    # Get details about teh destination
    config = await get_outbound_config_detail(outbound_config_id)
    if not config:
        logger.error(
            f"No outbound config detail found",
            extra={**extra_dict, ExtraKeys.AttentionNeeded: True},
        )
        raise ReferenceDataError

    # Get details about the source
    inbound_integration = await get_inbound_integration_detail(inbound_int_id)
    provider = inbound_integration.provider

    try:  # Select the dispatcher
        dispatcher_cls = dispatchers.dispatcher_cls_by_type[stream_type]
    except KeyError as e:
        extra_dict[ExtraKeys.Provider] = config.type_slug
        logger.error(
            f"No dispatcher found",
            extra={
                **extra_dict,
                ExtraKeys.Provider: config.type_slug,
                ExtraKeys.AttentionNeeded: True,
            },
        )
        raise Exception("No dispatcher found")
    else:  # Send the observation to the destination
        try:
            dispatcher = dispatcher_cls(config, provider)
            await dispatcher.send(observation)
        except Exception as e:
            logger.error(
                f"Exception occurred dispatching observation",
                extra={
                    **extra_dict,
                    ExtraKeys.Provider: config.type_slug,
                    ExtraKeys.AttentionNeeded: True,
                },
            )
            raise DispatcherException(f"Exception occurred dispatching observation: {e}")


async def send_observation_to_dead_letter_topic(transformed_observation, attributes):
    with tracing.tracer.start_as_current_span(
            "send_message_to_dead_letter_topic", kind=SpanKind.CLIENT
    ) as current_span:

        print(f"Forwarding observation to dead letter topic: {transformed_observation}")
        # Publish to another PubSub topic
        connect_timeout, read_timeout = settings.DEFAULT_REQUESTS_TIMEOUT
        timeout_settings = aiohttp.ClientTimeout(
            sock_connect=connect_timeout, sock_read=read_timeout
        )
        async with aiohttp.ClientSession(
            raise_for_status=True, timeout=timeout_settings
        ) as session:
            client = pubsub.PublisherClient(session=session)
            # Get the topic
            topic_name = settings.DEAD_LETTER_TOPIC
            current_span.set_attribute("topic", topic_name)
            topic = client.topic_path(settings.GCP_PROJECT_ID, topic_name)
            # Prepare the payload
            binary_payload = json.dumps(transformed_observation, default=str).encode("utf-8")
            messages = [pubsub.PubsubMessage(binary_payload, **attributes)]
            logger.info(f"Sending observation to PubSub topic {topic_name}..")
            try:  # Send to pubsub
                response = await client.publish(topic, messages)
            except Exception as e:
                logger.exception(
                    f"Error sending observation to dead letter topic {topic_name}: {e}. Please check if the topic exists or review settings."
                )
                raise e
            else:
                logger.info(f"Observation sent to the dead letter topic successfully.")
                logger.debug(f"GCP PubSub response: {response}")

        current_span.set_attribute("is_sent_to_dead_letter_queue", True)
        current_span.add_event(
            name="routing_service.observation_sent_to_dead_letter_queue"
        )


async def process_transformed_observation(transformed_observation, attributes):
    observation_type = attributes.get("observation_type")
    if observation_type not in dispatchers.dispatcher_cls_by_type.keys():
        error_msg = f"Observation type `{observation_type}` is not supported by this dispatcher."
        logger.error(
            error_msg,
            extra={
                ExtraKeys.AttentionNeeded: True,
            },
        )
        raise DispatcherException(f"Exception occurred dispatching observation: {error_msg}")

    with tracing.tracer.start_as_current_span(
            "er_dispatcher.process_transformed_observation", kind=SpanKind.CLIENT
    ) as current_span:
        current_span.add_event(
            name="routing_service.transformed_observation_received_at_dispatcher"
        )
        current_span.set_attribute("transformed_message", str(transformed_observation))
        current_span.set_attribute("environment", settings.TRACE_ENVIRONMENT)
        current_span.set_attribute("service", "er-dispatcher")
        try:
            observation_type = attributes.get("observation_type")
            device_id = attributes.get("device_id")
            integration_id = attributes.get("integration_id")
            outbound_config_id = attributes.get("outbound_config_id")
            retry_attempt: int = attributes.get("retry_attempt") or 0
            logger.debug(f"transformed_observation: {transformed_observation}")
            logger.info(
                "received transformed observation",
                extra={
                    ExtraKeys.DeviceId: device_id,
                    ExtraKeys.InboundIntId: integration_id,
                    ExtraKeys.OutboundIntId: outbound_config_id,
                    ExtraKeys.StreamType: observation_type,
                    ExtraKeys.RetryAttempt: retry_attempt,
                },
            )
        except Exception as e:
            logger.exception(
                f"Exception occurred prior to dispatching transformed observation",
                extra={
                    ExtraKeys.AttentionNeeded: True,
                    ExtraKeys.Observation: transformed_observation,
                },
            )
            raise e
        try:
            logger.info(
                "Dispatching for transformed observation.",
                extra={
                    ExtraKeys.InboundIntId: integration_id,
                    ExtraKeys.OutboundIntId: outbound_config_id,
                    ExtraKeys.StreamType: observation_type,
                },
            )
            with tracing.tracer.start_as_current_span(
                "er_dispatcher.dispatch_transformed_observation", kind=SpanKind.CLIENT
            ) as current_span:
                await dispatch_transformed_observation(
                    observation_type,
                    outbound_config_id,
                    integration_id,
                    transformed_observation,
                )
                current_span.set_attribute("is_dispatched_successfully", True)
                current_span.set_attribute("destination_id", str(outbound_config_id))
                current_span.add_event(
                    name="er_dispatcher.observation_dispatched_successfully"
                )
        except (DispatcherException, ReferenceDataError) as e:
            with tracing.tracer.start_as_current_span(
                    "er_dispatcher.error_dispatching_observation", kind=SpanKind.CLIENT
            ) as error_span:
                error_msg = f"External error occurred processing transformed observation: {e}"
                logger.exception(
                    error_msg,
                    extra={
                        ExtraKeys.AttentionNeeded: True,
                        ExtraKeys.DeviceId: device_id,
                        ExtraKeys.InboundIntId: integration_id,
                        ExtraKeys.OutboundIntId: outbound_config_id,
                        ExtraKeys.StreamType: observation_type,
                    },
                )
                error_span.set_attribute("error", error_msg)
                # Raise the exception so the message is retried later by GCP
                raise e

        except Exception as e:
            # Log and raise so it's retried
            with tracing.tracer.start_as_current_span(
                    "er_dispatcher.error_dispatching_observation", kind=SpanKind.CLIENT
            ) as error_span:
                error_msg = (
                    f"Unexpected internal error occurred processing transformed observation: {e}"
                )
                logger.exception(
                    error_msg,
                    extra={
                        ExtraKeys.AttentionNeeded: True,
                        ExtraKeys.DeadLetter: True,
                        ExtraKeys.DeviceId: device_id,
                        ExtraKeys.InboundIntId: integration_id,
                        ExtraKeys.OutboundIntId: outbound_config_id,
                        ExtraKeys.StreamType: observation_type,
                    },
                )
                error_span.set_attribute("error", error_msg)
                # Raise the exception so the message is retried later by GCP
                raise e


async def process_transformer_event_v2(raw_event, attributes):
    with tracing.tracer.start_as_current_span(
            "er_dispatcher.process_transformer_event_v2", kind=SpanKind.CLIENT
    ) as current_span:
        current_span.add_event(
            name="er_dispatcher.transformed_observation_received_at_dispatcher"
        )
        current_span.set_attribute("transformed_message", str(raw_event))
        current_span.set_attribute("environment", settings.TRACE_ENVIRONMENT)
        current_span.set_attribute("service", "er-dispatcher")
        logger.debug(f"Message received: \npayload: {raw_event} \nattributes: {attributes}")
        if schema_version := raw_event.get("schema_version") != "v1":
            error_message = f"Schema version '{schema_version}' not supported. Message discarded."
            logger.error(error_message)
            current_span.set_attribute("error", error_message)
            await send_observation_to_dead_letter_topic(raw_event, attributes)
            return {}
        event_type = raw_event.get("event_type")
        try:
            handler = event_handlers[event_type]
        except KeyError:
            error_message = f"Event of type '{event_type}' unknown. Ignored."
            logger.error(error_message)
            current_span.set_attribute("error", error_message)
            await send_observation_to_dead_letter_topic(raw_event, attributes)
            return {}
        try:
            schema = event_schemas[event_type]
        except KeyError:
            error_message = f"Event Schema for '{event_type}' not found. Message discarded."
            current_span.set_attribute("error", error_message)
            await send_observation_to_dead_letter_topic(raw_event, attributes)
            return {}
        parsed_event = schema.parse_obj(raw_event)
        return await handler(event=parsed_event, attributes=attributes)


def is_too_old(timestamp):
    if not timestamp:
        logger.warning("No timestamp found in Pubsub Message. Skipping age check.")
        return False
    try:  # The timestamp does not always include the microseconds part
        event_time = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        event_time = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")
    event_time = event_time.replace(tzinfo=timezone.utc)
    current_time = datetime.now(timezone.utc)
    event_age_seconds = (current_time - event_time).total_seconds()
    return event_age_seconds > settings.MAX_EVENT_AGE_SECONDS


async def process_request(request):
    # Extract the observation and attributes from the CloudEvent
    json_data = request.get_json()
    pubsub_message = json_data["message"]
    transformed_observation, attributes = extract_fields_from_message(pubsub_message)
    # Load tracing context
    tracing.pubsub_instrumentation.load_context_from_attributes(attributes)
    with tracing.tracer.start_as_current_span(
            "er_dispatcher.process_event", kind=SpanKind.CLIENT
    ) as current_span:
        pubsub_message_id = pubsub_message.get("message_id")
        gundi_event_id = transformed_observation.get("event_id")
        current_span.set_attribute("pubsub_message_id", str(pubsub_message_id))
        current_span.set_attribute("gundi_event_id", str(gundi_event_id))
        logger.debug(
            f"Received PubsubMessage(PubSub ID:{pubsub_message_id}, Gundi Event ID: {gundi_event_id}): {pubsub_message}")
        # ToDo Check duplicates using message_id / gundi_event_id
        # Handle retries
        timestamp = pubsub_message.get("publish_time") or pubsub_message.get("time")
        if is_too_old(timestamp):
            logger.warning(f"Event is too old (timestamp = {timestamp}) and will be sent to dead-letter.")
            current_span.set_attribute("is_too_old", True)
            await send_observation_to_dead_letter_topic(transformed_observation, attributes)
            return  # Skip the event
        # Process the event according to the gundi version
        if attributes.get("gundi_version", "v1") == "v2":
            await process_transformer_event_v2(transformed_observation, attributes)
        else:  # Default to v1
            await process_transformed_observation(transformed_observation, attributes)

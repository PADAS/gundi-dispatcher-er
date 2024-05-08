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
    get_integration_details,
    get_dispatched_observation,
    cache_dispatched_observation,
    is_null,
    publish_event,
)
from gundi_core.schemas import v2 as gundi_schemas_v2
from gundi_core import events as system_events
from .errors import DispatcherException, ReferenceDataError
from . import tracing
from . import settings


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
            "dispatch_transformed_observation - value error.",
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


async def dispatch_transformed_observation_v2(
    observation, stream_type: str, data_provider_id:str, provider_key: str, destination_id: str,
    gundi_id: str, related_to=None
):
    extra_dict = {
        ExtraKeys.OutboundIntId: destination_id,
        ExtraKeys.Provider: provider_key,
        ExtraKeys.Observation: observation,
        ExtraKeys.StreamType: stream_type,
        ExtraKeys.GundiId: gundi_id,
        ExtraKeys.RelatedTo: related_to
    }

    if not destination_id or not provider_key:
        logger.error(
            "dispatch_transformed_observation - value error.",
            extra=extra_dict,
        )

    # Get details about the destination
    destination_integration = await get_integration_details(integration_id=destination_id)
    if not destination_integration:
        logger.error(
            f"No destination config details found",
            extra={**extra_dict, ExtraKeys.AttentionNeeded: True},
        )
        raise ReferenceDataError

    # # Get details about the data provider ?
    # provider_integration = await get_integration_details(integration_id=data_provider_id)
    # # Look for the configuration of the push action
    # configurations = provider_integration.configurations

    # Check for related observations
    if not is_null(related_to):
        # Check if the related object was dispatched
        related_observation = await get_dispatched_observation(gundi_id=related_to, destination_id=destination_id)
        if not related_observation:
            logger.error(
                f"Error getting related observation. Will retry later.",
                extra={**extra_dict, ExtraKeys.AttentionNeeded: True},
            )
            raise ReferenceDataError
    else:
        related_observation = None
    try:  # Select the dispatcher
        dispatcher_cls = dispatchers.dispatcher_cls_by_type[stream_type]
    except KeyError as e:
        logger.error(
            f"No dispatcher found",
            extra={
                **extra_dict,
                ExtraKeys.AttentionNeeded: True,
            }
        )
        raise Exception("No dispatcher found")
    else:  # Send the observation to the destination
        try:
            dispatcher = dispatcher_cls(
                integration=destination_integration,
                provider=provider_key
            )
            result = await dispatcher.send(observation, related_observation=related_observation)
        except Exception as e:
            logger.error(
                f"Exception occurred dispatching observation",
                extra={
                    **extra_dict,
                    ExtraKeys.Provider: provider_key,
                    ExtraKeys.AttentionNeeded: True,
                },
            )
            # Emit events for the portal and other interested services (EDA)
            await publish_event(
                event=system_events.ObservationDeliveryFailed(
                    payload=gundi_schemas_v2.DispatchedObservation(
                        gundi_id=gundi_id,
                        related_to=related_to,
                        external_id=None,  # ID returned by the destination system
                        data_provider_id=data_provider_id,
                        destination_id=destination_id,
                        delivered_at=datetime.now(timezone.utc)  # UTC
                    )
                ),
                topic_name=settings.DISPATCHER_EVENTS_TOPIC
            )
            raise DispatcherException(f"Exception occurred dispatching observation: {e}")
        else:
            # Cache data related to the dispatched observation
            if isinstance(result, list):
                result = result[0]
            dispatched_observation = gundi_schemas_v2.DispatchedObservation(
                gundi_id=gundi_id,
                related_to=related_to,
                external_id=result.get("id"),  # ID returned by the destination system
                data_provider_id=data_provider_id,
                destination_id=destination_id,
                delivered_at=datetime.now(timezone.utc)  # UTC
            )
            cache_dispatched_observation(observation=dispatched_observation)
            # Emit events for the portal and other interested services (EDA)
            await publish_event(
                event=system_events.ObservationDelivered(
                    payload=dispatched_observation
                ),
                topic_name=settings.DISPATCHER_EVENTS_TOPIC
            )


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
            logger.exception(
                f"External error occurred processing transformed observation",
                extra={
                    ExtraKeys.AttentionNeeded: True,
                    ExtraKeys.DeviceId: device_id,
                    ExtraKeys.InboundIntId: integration_id,
                    ExtraKeys.OutboundIntId: outbound_config_id,
                    ExtraKeys.StreamType: observation_type,
                },
            )
            # Raise the exception so the function execution is marked as failed and retried later
            raise e

        except Exception as e:
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
            # Unexpected internal errors will be redirected straight to deadletter
            current_span.set_attribute("error", error_msg)
            # Send it to a dead letter pub/sub topic
            await send_observation_to_dead_letter_topic(transformed_observation, attributes)


async def process_transformed_observation_v2(transformed_observation, attributes):
    stream_type = attributes.get("stream_type")
    if stream_type not in dispatchers.dispatcher_cls_by_type.keys():
        error_msg = f"Stream type `{stream_type}` is not supported by this dispatcher."
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
            name="er_dispatcher.transformed_observation_received_at_dispatcher"
        )
        current_span.set_attribute("transformed_message", str(transformed_observation))
        current_span.set_attribute("environment", settings.TRACE_ENVIRONMENT)
        current_span.set_attribute("service", "er-dispatcher")
        try:
            source_id = attributes.get("external_source_id")
            data_provider_id = attributes.get("data_provider_id")
            provider_key = transformed_observation.pop("provider_key", attributes.get("provider_key"))
            destination_id = attributes.get("destination_id")
            gundi_id = attributes.get("gundi_id")
            related_to = attributes.get("related_to")
            logger.debug(f"transformed_observation: {transformed_observation}")
            logger.info(
                "received transformed observation",
                extra={
                    ExtraKeys.DeviceId: source_id,
                    ExtraKeys.InboundIntId: data_provider_id,
                    ExtraKeys.Provider: provider_key,
                    ExtraKeys.OutboundIntId: destination_id,
                    ExtraKeys.StreamType: stream_type,
                    ExtraKeys.GundiId: gundi_id,
                    ExtraKeys.RelatedTo: related_to
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
                    ExtraKeys.InboundIntId: data_provider_id,
                    ExtraKeys.OutboundIntId: destination_id,
                    ExtraKeys.StreamType: stream_type,
                    ExtraKeys.GundiId: gundi_id,
                    ExtraKeys.RelatedTo: related_to
                },
            )
            with tracing.tracer.start_as_current_span(
                "er_dispatcher.dispatch_transformed_observation", kind=SpanKind.CLIENT
            ) as current_span:
                await dispatch_transformed_observation_v2(
                    observation=transformed_observation,
                    stream_type=stream_type,
                    data_provider_id=data_provider_id,
                    provider_key=provider_key,
                    destination_id=destination_id,
                    gundi_id=gundi_id,
                    related_to=related_to
                )
                current_span.set_attribute("is_dispatched_successfully", True)
                current_span.set_attribute("destination_id", str(destination_id))
                current_span.add_event(
                    name="er_dispatcher.observation_dispatched_successfully"
                )
        except (DispatcherException, ReferenceDataError) as e:
            logger.exception(
                f"External error occurred processing transformed observation",
                extra={
                    ExtraKeys.AttentionNeeded: True,
                    ExtraKeys.DeviceId: source_id,
                    ExtraKeys.InboundIntId: data_provider_id,
                    ExtraKeys.OutboundIntId: destination_id,
                    ExtraKeys.StreamType: stream_type,
                },
            )
            # Raise the exception so the function execution is marked as failed and retried later
            raise e

        except Exception as e:
            error_msg = (
                f"Unexpected internal error occurred processing transformed observation: {e}"
            )
            logger.exception(
                error_msg,
                extra={
                    ExtraKeys.AttentionNeeded: True,
                    ExtraKeys.DeadLetter: True,
                    ExtraKeys.DeviceId: source_id,
                    ExtraKeys.InboundIntId: data_provider_id,
                    ExtraKeys.OutboundIntId: destination_id,
                    ExtraKeys.StreamType: stream_type,
                },
            )
            # Unexpected internal errors will be redirected straight to deadletter
            current_span.set_attribute("error", error_msg)
            # Send it to a dead letter pub/sub topic
            await send_observation_to_dead_letter_topic(transformed_observation, attributes)


def is_event_too_old(event):
    logger.debug(f"event attributes: {event._attributes}")
    timestamp = event._attributes.get("time")
    if not timestamp:
        return False
    try:  # The timestamp does not always include the microseconds part
        event_time = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        event_time = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")
    event_time = event_time.replace(tzinfo=timezone.utc)
    current_time = datetime.now(timezone.utc)
    # Notice: We have seen cloud events with future timestamps. Don't use .seconds
    event_age_seconds = (current_time - event_time).total_seconds()
    # Ignore events that are too old
    return event_age_seconds > settings.MAX_EVENT_AGE_SECONDS


async def process_event(event):
    # Extract the observation and attributes from the CloudEvent
    transformed_observation, attributes = extract_fields_from_message(event.data["message"])
    # Load tracing context
    tracing.pubsub_instrumentation.load_context_from_attributes(attributes)
    with tracing.tracer.start_as_current_span(
            "er_dispatcher.process_event", kind=SpanKind.CLIENT
    ) as current_span:
        # Handle retries
        if is_event_too_old(event):
            logger.warning(f"Event is too old (timestamp = {event._attributes.get('time')}) and will be sent to dead-letter.")
            current_span.set_attribute("is_too_old", True)
            await send_observation_to_dead_letter_topic(transformed_observation, attributes)
            return  # Skip the event
        # Process the event according to the gundi version
        if attributes.get("gundi_version", "v1") == "v2":
            await process_transformed_observation_v2(transformed_observation, attributes)
        else:  # Default to v1
            await process_transformed_observation(transformed_observation, attributes)

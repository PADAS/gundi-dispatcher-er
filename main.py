import asyncio
import logging
from functions_framework import cloud_event
from opentelemetry.trace import SpanKind
from core import dispatchers
from core.utils import (
    extract_fields_from_message,
    get_inbound_integration_detail,
    get_outbound_config_detail,
    ExtraKeys,
)
from core.errors import DispatcherException, ReferenceDataError
from core import tracing
from core import settings

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


async def process_transformed_observation(message):
    # Extract the observation and attributes from the CloudEvent
    transformed_observation, attributes = extract_fields_from_message(message)
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
    # Load tracing context
    tracing.pubsub_instrumentation.load_context_from_attributes(attributes)
    with tracing.tracer.start_as_current_span(
            "er_serverless_dispatcher", kind=SpanKind.CLIENT
    ) as current_span:
        current_span.add_event(
            name="routing_service.transformed_observation_received_at_dispatcher"
        )
        current_span.set_attribute("transformed_message", str(transformed_observation))
        current_span.set_attribute("environment", settings.TRACE_ENVIRONMENT)
        current_span.set_attribute("service", "cdip-routing")
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
                "routing_service.dispatch_transformed_observation", kind=SpanKind.CLIENT
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
                    name="routing_service.observation_dispatched_successfully"
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
            # ToDo: Send to a dead letter pub/sub topic
            # tracing_headers = tracing.faust_instrumentation.build_context_headers()
            # await observations_transformed_deadletter.send(
            #     value=transformed_observation, headers=tracing_headers
            # )
            current_span.set_attribute("is_sent_to_dead_letter_queue", True)
            current_span.add_event(
                name="routing_service.observation_sent_to_dead_letter_queue"
            )


async def main_async(event):
    await process_transformed_observation(event.data["message"])


# Wrapper to be able to run the async function
@cloud_event
def main(event):
    print(f"Event received:\n{event}")
    asyncio.run(main_async(event))
    print(f"Event processed successfully.")
    return {}

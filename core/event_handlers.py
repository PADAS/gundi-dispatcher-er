import logging
import traceback
from datetime import datetime, timezone

from gundi_core.events import UpdateErrorDetails, DeliveryErrorDetails
from gundi_core.events.transformers import (
    EventTransformedER,
    EventUpdateTransformedER,
    AttachmentTransformedER,
    ObservationTransformedER,
    MessageTransformedER
)
from core import tracing, dispatchers, settings
from core.errors import ReferenceDataError, DispatcherException
from core.utils import (
    ExtraKeys,
    get_integration_details,
    get_dispatched_observation,
    cache_dispatched_observation,
    is_null,
    publish_event,
)
from gundi_core.schemas import v2 as gundi_schemas_v2
from gundi_core import events as system_events, schemas
from opentelemetry.trace import SpanKind


logger = logging.getLogger(__name__)


async def dispatch_transformed_observation_v2(observation, attributes: dict):
    with tracing.tracer.start_as_current_span(
            "er_dispatcher.dispatch_transformed_observation", kind=SpanKind.CLIENT
    ) as current_span:
        data_provider_id = attributes.get("data_provider_id")
        destination_id = attributes.get("destination_id")
        provider_key = attributes.get("provider_key")
        stream_type = attributes.get("stream_type")
        gundi_id = attributes.get("gundi_id")
        related_to = attributes.get("related_to")
        extra_dict = {
            ExtraKeys.OutboundIntId: destination_id,
            ExtraKeys.Provider: provider_key,
            ExtraKeys.Observation: observation,
            ExtraKeys.StreamType: stream_type,
            ExtraKeys.GundiId: gundi_id,
            ExtraKeys.RelatedTo: related_to
        }

        if not destination_id or not provider_key:
            error_msg = f"Missing destination_id or provider_key in observation {gundi_id}"
            logger.error(
                error_msg,
                extra=extra_dict,  # FixMe: extra is not visible in GCP logs
            )
            raise ReferenceDataError(error_msg)

        # Get details about the destination
        destination_integration = await get_integration_details(integration_id=destination_id)
        if not destination_integration:
            error_msg = f"No destination config details found for destination_id {destination_id}"
            logger.error(
                error_msg,
                extra={**extra_dict, ExtraKeys.AttentionNeeded: True},
            )
            raise ReferenceDataError(error_msg)

        # Check for related observations
        if not is_null(related_to):
            # Check if the related object was dispatched
            related_observation = await get_dispatched_observation(gundi_id=related_to, destination_id=destination_id)
            if not related_observation:
                error_msg = f"Error getting related observation {related_to}. Will retry later.",
                logger.error(
                    error_msg,
                    extra={**extra_dict, ExtraKeys.AttentionNeeded: True},
                )
                raise ReferenceDataError(error_msg)
            elif not related_observation.external_id:
                error_msg = f"Related observation {related_to} was not dispatched yet. Will retry later."
                logger.error(
                    error_msg,
                    extra={**extra_dict, ExtraKeys.AttentionNeeded: True},
                )
                raise ReferenceDataError(error_msg)
        else:
            related_observation = None

        # If it's an update, get the external id (ER Event uuid)
        if stream_type == schemas.v2.StreamPrefixEnum.event_update:
            dispatched_observation = await get_dispatched_observation(gundi_id=gundi_id, destination_id=destination_id)
            if not dispatched_observation or not dispatched_observation.external_id:
                error_msg = f"Event {gundi_id} wasn't delivered yet. Will retry later."
                logger.warning(
                    error_msg,
                    extra={**extra_dict, ExtraKeys.AttentionNeeded: True},
                )
                await publish_event(
                    event=system_events.ObservationUpdateFailed(
                        payload=UpdateErrorDetails(
                            error=error_msg,
                            observation=gundi_schemas_v2.UpdatedObservation(
                                gundi_id=gundi_id,
                                related_to=related_to,
                                data_provider_id=data_provider_id,
                                destination_id=destination_id,
                                updated_at=datetime.now(timezone.utc)  # UTC
                            )
                        )
                    ),
                    topic_name=settings.DISPATCHER_EVENTS_TOPIC
                )
                raise ReferenceDataError(error_msg)
            external_id = str(dispatched_observation.external_id)
        else:
            external_id = None

        try:  # Select the dispatcher
            dispatcher_cls = dispatchers.dispatcher_cls_by_type[stream_type]
        except KeyError as e:
            error_msg = f"No dispatcher found for stream type {stream_type}",
            logger.exception(
                error_msg,
                extra={
                    **extra_dict,
                    ExtraKeys.AttentionNeeded: True,
                }
            )
            raise Exception(error_msg)
        else:  # Send the observation to the destination
            try:
                dispatcher = dispatcher_cls(
                    integration=destination_integration,
                    provider=provider_key
                )
                kwargs = {
                    "external_id": external_id,  # Used in updates
                    "related_observation": related_observation  # Used in attachments
                }
                result = await dispatcher.send(observation, **kwargs)
            except Exception as e:
                error = f"{type(e).__name__}: {e}"
                error_msg = f"Exception occurred dispatching observation {gundi_id}: {error}"
                logger.exception(
                    error_msg,
                    extra={
                        **extra_dict,
                        ExtraKeys.Provider: provider_key,
                        ExtraKeys.AttentionNeeded: True,
                    },
                )
                # Emit events for the portal and other interested services (EDA)
                if stream_type == schemas.v2.StreamPrefixEnum.event_update.value:
                    await publish_event(
                        event=system_events.ObservationUpdateFailed(
                            payload=UpdateErrorDetails(
                                error=error,
                                error_traceback=traceback.format_exc(),
                                server_response_status=getattr(e, "status_code", None),
                                server_response_body=getattr(e, "response_body", ""),
                                observation=gundi_schemas_v2.UpdatedObservation(
                                    gundi_id=gundi_id,
                                    related_to=related_to,
                                    data_provider_id=data_provider_id,
                                    destination_id=destination_id,
                                    updated_at=datetime.now(timezone.utc)  # UTC
                                )
                            )
                        ),
                        topic_name=settings.DISPATCHER_EVENTS_TOPIC
                    )
                else:
                    await publish_event(
                        event=system_events.ObservationDeliveryFailed(
                            payload=DeliveryErrorDetails(
                                error=error,
                                error_traceback=traceback.format_exc(),
                                server_response_status=getattr(e, "status_code", None),
                                server_response_body=getattr(e, "response_body", ""),
                                observation=gundi_schemas_v2.DispatchedObservation(
                                    gundi_id=gundi_id,
                                    related_to=related_to,
                                    external_id=None,  # ID returned by the destination system
                                    data_provider_id=data_provider_id,
                                    destination_id=destination_id,
                                    delivered_at=datetime.now(timezone.utc)  # UTC
                                )
                            )
                        ),
                        topic_name=settings.DISPATCHER_EVENTS_TOPIC
                    )
                raise DispatcherException(error_msg)
            else:
                logger.debug(f"Observation {gundi_id} delivered with success. ER response: {result}")
                current_span.set_attribute("is_dispatched_successfully", True)
                current_span.set_attribute("destination_id", str(destination_id))
                current_span.add_event(
                    name="er_dispatcher.observation_dispatched_successfully"
                )
                # Emit events for the portal and other interested services (EDA)
                if stream_type == schemas.v2.StreamPrefixEnum.event_update.value:
                    await publish_event(
                        event=system_events.ObservationUpdated(
                            payload=gundi_schemas_v2.UpdatedObservation(
                                gundi_id=gundi_id,
                                related_to=related_to,
                                data_provider_id=data_provider_id,
                                destination_id=destination_id,
                                updated_at=datetime.now(timezone.utc)  # UTC
                            )
                        ),
                        topic_name=settings.DISPATCHER_EVENTS_TOPIC
                    )

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


async def handle_er_event(event: EventTransformedER, attributes: dict):
    # Trace observations with Open Telemetry
    with tracing.tracer.start_as_current_span(
            "er_dispatcher.handle_er_event", kind=SpanKind.CONSUMER
    ) as current_span:
        current_span.set_attribute("payload", repr(event.payload))
        return await dispatch_transformed_observation_v2(observation=event.payload, attributes=attributes)


async def handle_er_event_update(event: EventUpdateTransformedER, attributes: dict):
    # Trace observations with Open Telemetry
    with tracing.tracer.start_as_current_span(
            "er_dispatcher.handle_er_event_update", kind=SpanKind.CONSUMER
    ) as current_span:
        event_update = event.payload
        current_span.set_attribute("payload", repr(event.payload))
        current_span.set_attribute("changes", str(event_update.changes))
        return await dispatch_transformed_observation_v2(observation=event.payload, attributes=attributes)


async def handle_er_attachment(event: AttachmentTransformedER, attributes: dict):
    # Trace observations with Open Telemetry
    with tracing.tracer.start_as_current_span(
            "er_dispatcher.handle_er_attachment", kind=SpanKind.CONSUMER
    ) as current_span:
        current_span.set_attribute("payload", repr(event.payload))
        return await dispatch_transformed_observation_v2(observation=event.payload, attributes=attributes)


async def handle_er_observation(event: AttachmentTransformedER, attributes: dict):
    # Trace observations with Open Telemetry
    with tracing.tracer.start_as_current_span(
            "er_dispatcher.handle_er_observation", kind=SpanKind.CONSUMER
    ) as current_span:
        current_span.set_attribute("payload", repr(event.payload))
        return await dispatch_transformed_observation_v2(observation=event.payload, attributes=attributes)


async def handle_er_message(event: MessageTransformedER, attributes: dict):
    # Trace observations with Open Telemetry
    with tracing.tracer.start_as_current_span(
            "er_dispatcher.handle_er_message", kind=SpanKind.CONSUMER
    ) as current_span:
        current_span.set_attribute("payload", repr(event.payload))
        return await dispatch_transformed_observation_v2(observation=event.payload, attributes=attributes)

event_schemas = {
    "EventTransformedER": EventTransformedER,
    "EventUpdateTransformedER": EventUpdateTransformedER,
    "AttachmentTransformedER": AttachmentTransformedER,
    "ObservationTransformedER": ObservationTransformedER,
    "MessageTransformedER": MessageTransformedER
}

event_handlers = {
    "EventTransformedER": handle_er_event,
    "EventUpdateTransformedER": handle_er_event_update,
    "AttachmentTransformedER": handle_er_attachment,
    "ObservationTransformedER": handle_er_observation,
    "MessageTransformedER": handle_er_message
}
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
# NOTE: ObservationsBatchTransformedER lives in gundi_core.events.batches, not
# .transformers (verified against gundi_core 1.13.0's actual module layout).
from gundi_core.events import ObservationsBatchTransformedER
from core import tracing, dispatchers, settings, throttling
from core.errors import ReferenceDataError, DispatcherException
from core.utils import (
    ExtraKeys,
    get_integration_details,
    get_dispatched_observation,
    cache_dispatched_observation,
    is_observation_dispatched,
    is_null,
    publish_event,
)
from gundi_core.schemas import v2 as gundi_schemas_v2
from gundi_core import events as system_events, schemas
from gundi_core.schemas.v2 import LogLevel
from opentelemetry.trace import SpanKind


logger = logging.getLogger(__name__)


async def publish_throttling_notice(attributes: dict, scope: str):
    # One INFO-level breadcrumb in the portal activity log when a destination
    # enters cooldown ("why is my data delayed"). INFO never counts toward
    # health thresholds. Failures here must not affect delivery handling.
    if scope == throttling.SITE_SCOPE:
        title = "Deliveries to this destination are temporarily deferred (destination unreachable or overloaded)"
    else:
        family_name = throttling.FAMILY_DISPLAY_NAMES.get(scope, scope.capitalize())
        title = f"{family_name} deliveries to this destination are temporarily deferred (rate limited)"
    try:
        await publish_event(
            event=system_events.DispatcherCustomLog(
                payload=gundi_schemas_v2.CustomDispatcherLog(
                    gundi_id=attributes.get("gundi_id"),
                    related_to=attributes.get("related_to"),
                    data_provider_id=attributes.get("data_provider_id"),
                    destination_id=attributes.get("destination_id"),
                    title=title,
                    level=LogLevel.INFO,
                )
            ),
            topic_name=settings.DISPATCHER_EVENTS_TOPIC,
        )
    except Exception as e:
        logger.exception(f"Error publishing throttling notice: {e}")


async def publish_batch_dead_lettered_notice(attributes: dict):
    # Batch envelopes carry no gundi_id in their attributes, so
    # publish_retries_exhausted_event (which requires one) bails out for
    # them with just a warning log - the age-out would otherwise be
    # completely silent: no activity-log entry, no failure event, and up to
    # OBSERVATIONS_BATCH_MAX_ITEMS observations simply vanish with no trace.
    title = (
        "Observations batch dead-lettered after retries "
        f"(batch_count={attributes.get('batch_count')})"
    )
    try:
        await publish_event(
            event=system_events.DispatcherCustomLog(
                payload=gundi_schemas_v2.CustomDispatcherLog(
                    gundi_id=None,
                    related_to=attributes.get("related_to"),
                    data_provider_id=attributes.get("data_provider_id"),
                    destination_id=attributes.get("destination_id"),
                    title=title,
                    level=LogLevel.ERROR,
                )
            ),
            topic_name=settings.DISPATCHER_EVENTS_TOPIC,
        )
    except Exception as e:
        logger.exception(f"Error publishing batch dead-lettered notice: {e}")


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
                notify_scope = throttling.record_distress(
                    destination_id=destination_id,
                    stream_type=stream_type,
                    status_code=getattr(e, "status_code", None),
                    error=error,
                    retry_after=getattr(e, "retry_after", None),
                )
                if notify_scope:
                    await publish_throttling_notice(attributes=attributes, scope=notify_scope)
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
                throttling.record_success(
                    destination_id=destination_id, stream_type=stream_type
                )
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


# ER status codes that mean the payload itself is bad: retrying the same
# bytes can never succeed, so shrink the batch instead of nacking it.
# 403 is deliberately NOT here: it's typically an auth/permission condition
# (revoked token, provider key not yet authorized) that is recoverable once
# fixed, and the single-item path already retries it for 24h - treating it
# as permanent here would turn a fixable config error into permanent data
# loss for every item in the batch.
PERMANENT_ER_STATUS_CODES = {400}


def _chunked(items, size):
    for start in range(0, len(items), size):
        yield items[start:start + size]


async def _publish_batch_delivered(batch, delivered_gundi_ids):
    if not delivered_gundi_ids:
        return
    await publish_event(
        event=system_events.ObservationsBatchDelivered(
            payload=system_events.ObservationsBatchDeliveryDetails(
                batch_id=batch.batch_id,
                data_provider_id=batch.data_provider_id,
                destination_id=batch.destination_id,
                delivered_at=datetime.now(timezone.utc),
                gundi_ids=delivered_gundi_ids,
            )
        ),
        topic_name=settings.DISPATCHER_EVENTS_TOPIC,
    )


async def _publish_item_delivery_failed(batch, item, exception):
    await publish_event(
        event=system_events.ObservationDeliveryFailed(
            payload=DeliveryErrorDetails(
                error=f"{type(exception).__name__}: {exception}",
                error_traceback=traceback.format_exc(),
                server_response_status=getattr(exception, "status_code", None),
                server_response_body=getattr(exception, "response_body", ""),
                observation=gundi_schemas_v2.DispatchedObservation(
                    gundi_id=item.gundi_id,
                    related_to=None,
                    external_id=None,
                    data_provider_id=batch.data_provider_id,
                    destination_id=batch.destination_id,
                    delivered_at=datetime.now(timezone.utc),
                ),
            )
        ),
        topic_name=settings.DISPATCHER_EVENTS_TOPIC,
    )


def _cache_item_as_dispatched(batch, item):
    cache_dispatched_observation(
        observation=gundi_schemas_v2.DispatchedObservation(
            gundi_id=item.gundi_id,
            related_to=None,
            external_id=None,  # By design: ER bulk responses carry no reliable per-item IDs
            data_provider_id=batch.data_provider_id,
            destination_id=batch.destination_id,
            delivered_at=datetime.now(timezone.utc),
        ),
        # Longer TTL than the single-item path: this cache is what makes
        # envelope redelivery idempotent (see is_observation_dispatched), and
        # PubSub keeps retrying for MAX_EVENT_AGE_SECONDS (24h) - a shorter
        # TTL would silently expire the skip-cache mid-retry-window and
        # re-post already-delivered items as duplicates.
        ttl=settings.DISPATCHED_OBSERVATIONS_BATCH_CACHE_TTL,
    )


async def dispatch_observations_batch_v2(batch, attributes: dict):
    with tracing.tracer.start_as_current_span(
            "er_dispatcher.dispatch_observations_batch", kind=SpanKind.CLIENT
    ) as current_span:
        destination_id = str(batch.destination_id)
        stream_type = gundi_schemas_v2.StreamPrefixEnum.observation.value
        current_span.set_attribute("batch_id", str(batch.batch_id))
        current_span.set_attribute("destination_id", destination_id)
        current_span.set_attribute("batch_count", len(batch.items))

        destination_integration = await get_integration_details(integration_id=destination_id)
        if not destination_integration:
            error_msg = f"No destination config details found for destination_id {destination_id}"
            logger.error(error_msg)
            raise ReferenceDataError(error_msg)

        # Skip items already delivered — makes envelope redelivery idempotent
        pending = [
            item for item in batch.items
            if not is_observation_dispatched(gundi_id=str(item.gundi_id), destination_id=destination_id)
        ]
        current_span.set_attribute("pending_count", len(pending))
        if not pending:
            # Everything is already cached as dispatched, but the original
            # attempt may have died after caching and before publishing
            # ObservationsBatchDelivered (e.g. function timeout). Publish for
            # ALL items so trace stamping isn't lost forever on redelivery —
            # the portal handler is idempotent against repeat events.
            logger.info(f"All items in batch {batch.batch_id} already delivered. Skipping.")
            await _publish_batch_delivered(batch, [str(item.gundi_id) for item in batch.items])
            return

        delivered_gundi_ids = []
        for chunk in _chunked(pending, settings.ER_BULK_SIZE):
            # A fresh dispatcher (and so a fresh underlying http client) per
            # chunk: _send's `async with self.er_client` permanently closes
            # the client on exit, so reusing one dispatcher across chunks
            # would make every chunk after the first raise on a closed
            # client (see C2 in the final review).
            dispatcher = dispatchers.ERObservationsBatchDispatcher(
                integration=destination_integration,
                provider=batch.provider_key,
            )
            try:
                await dispatcher.send([item.observation for item in chunk])
            except Exception as e:
                status_code = getattr(e, "status_code", None)
                error = f"{type(e).__name__}: {e}"
                if status_code in PERMANENT_ER_STATUS_CODES:
                    # Permanent: shrink the batch — post items individually so
                    # the poison record(s) get identified and failed alone.
                    logger.warning(
                        f"Bulk post rejected ({status_code}) for batch {batch.batch_id}. "
                        f"Falling back to per-item posts for {len(chunk)} items."
                    )
                    fallback_delivered_any = False
                    for item in chunk:
                        # Fresh client per item too, for the same reason as
                        # above (each single_dispatcher.send closes its client).
                        single_dispatcher = dispatchers.ERObservationDispatcher(
                            integration=destination_integration,
                            provider=batch.provider_key,
                        )
                        try:
                            await single_dispatcher.send(item.observation)
                        except Exception as item_exc:
                            logger.warning(
                                f"Observation {item.gundi_id} in batch {batch.batch_id} failed individually: {item_exc}"
                            )
                            await _publish_item_delivery_failed(batch, item, item_exc)
                        else:
                            _cache_item_as_dispatched(batch, item)
                            delivered_gundi_ids.append(str(item.gundi_id))
                            fallback_delivered_any = True
                    if fallback_delivered_any:
                        # A successful fallback delivery proves the site is
                        # reachable, same as a successful bulk chunk — clear
                        # any lingering cooldown instead of leaving the
                        # destination throttled.
                        throttling.record_success(
                            destination_id=destination_id, stream_type=stream_type
                        )
                else:
                    # Transient: record distress, report partial progress, and
                    # nack the envelope. Redelivery skips delivered items via
                    # the dispatched-observation cache.
                    notify_scope = throttling.record_distress(
                        destination_id=destination_id,
                        stream_type=stream_type,
                        status_code=status_code,
                        error=error,
                        retry_after=getattr(e, "retry_after", None),
                    )
                    if notify_scope:
                        await publish_throttling_notice(attributes=attributes, scope=notify_scope)
                    await _publish_batch_delivered(batch, delivered_gundi_ids)
                    raise DispatcherException(
                        f"Transient error dispatching batch {batch.batch_id}: {error}"
                    )
            else:
                for item in chunk:
                    _cache_item_as_dispatched(batch, item)
                    delivered_gundi_ids.append(str(item.gundi_id))
                throttling.record_success(destination_id=destination_id, stream_type=stream_type)

        current_span.set_attribute("delivered_count", len(delivered_gundi_ids))
        await _publish_batch_delivered(batch, delivered_gundi_ids)


async def handle_er_observations_batch(event: ObservationsBatchTransformedER, attributes: dict):
    with tracing.tracer.start_as_current_span(
            "er_dispatcher.handle_er_observations_batch", kind=SpanKind.CONSUMER
    ) as current_span:
        current_span.set_attribute("batch_count", len(event.payload.items))
        return await dispatch_observations_batch_v2(batch=event.payload, attributes=attributes)


event_schemas = {
    "EventTransformedER": EventTransformedER,
    "EventUpdateTransformedER": EventUpdateTransformedER,
    "AttachmentTransformedER": AttachmentTransformedER,
    "ObservationTransformedER": ObservationTransformedER,
    "ObservationsBatchTransformedER": ObservationsBatchTransformedER,
    "MessageTransformedER": MessageTransformedER
}

event_handlers = {
    "EventTransformedER": handle_er_event,
    "EventUpdateTransformedER": handle_er_event_update,
    "AttachmentTransformedER": handle_er_attachment,
    "ObservationTransformedER": handle_er_observation,
    "ObservationsBatchTransformedER": handle_er_observations_batch,
    "MessageTransformedER": handle_er_message
}
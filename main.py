import asyncio
import json
from functions_framework import cloud_event
from opentelemetry.trace import SpanKind
from core.dispatcher import ERPositionDispatcher
from core.utils import extract_fields_from_message, get_inbound_integration_detail, get_outbound_config_detail
from core.errors import DispatcherException
from core import tracing


async def positions_dispatcher(cloud_event):
    with tracing.tracer.start_as_current_span(
            "er_positions_dispatcher", kind=SpanKind.CLIENT
    ) as current_span:
        # Extract the payload from the CloudEvent
        payload = json.loads(cloud_event.data.decode('utf-8'))

        # Extract the observation from the message
        observation, attributes = extract_fields_from_message(payload["message"])

        # Get some configuration data as needed
        inbound_config_id = attributes.get("integration_id")
        current_span.set_attribute("integration_id", str(inbound_config_id))
        outbound_config_id = attributes.get("outbound_config_id")
        current_span.set_attribute("destination_id", str(outbound_config_id))
        inbound_config = await get_inbound_integration_detail(inbound_config_id)
        outbound_config = await get_outbound_config_detail(outbound_config_id)
        provider = inbound_config.provider

        try:  # Dispatch the observation
            dispatcher = ERPositionDispatcher(outbound_config, provider)
            await dispatcher.send(observation)
        except Exception as e:  # ToDo: Handle the different errors
            raise DispatcherException("Exception occurred dispatching observation")
        else:
            current_span.set_attribute("is_dispatched_successfully", True)
            current_span.add_event(
                name="routing_service.observation_dispatched_successfully"
            )


@cloud_event
async def main(cloud_event):
    # Notice: the event loop is automatically created and managed by Cloud Functions
    await positions_dispatcher(cloud_event)


# Wrapper to be able to run the async function locally with the functions framework
def main_sync_to_aync(cloud_event):
    asyncio.run(main(cloud_event))
    return {}

import asyncio
import json
from functions_framework import cloud_event
from opentelemetry.trace import SpanKind
from core.dispatcher import ERPositionDispatcher
from core.utils import extract_fields_from_message, get_inbound_integration_detail, get_outbound_config_detail
from core.errors import DispatcherException
from core import tracing


async def positions_dispatcher(event):
    with tracing.tracer.start_as_current_span(
            "er_positions_dispatcher", kind=SpanKind.CLIENT
    ) as current_span:
        # Extract the observation from the CloudEvent
        observation, attributes = extract_fields_from_message(event)

        # Get some configuration data as needed
        inbound_config_id = attributes.get("integration_id")
        current_span.set_attribute("integration_id", str(inbound_config_id))
        outbound_config_id = attributes.get("outbound_config_id")
        current_span.set_attribute("destination_id", str(outbound_config_id))
        inbound_config = await get_inbound_integration_detail(inbound_config_id)
        outbound_config = await get_outbound_config_detail(outbound_config_id)
        provider = inbound_config.provider

        try:  # Dispatch the observation
            print(f"Sending observation to ER with config: {outbound_config} ")
            dispatcher = ERPositionDispatcher(outbound_config, provider)
            await dispatcher.send(observation)
        except Exception as e:  # ToDo: Handle the different errors
            raise DispatcherException("Exception occurred dispatching observation")
        else:
            current_span.set_attribute("is_dispatched_successfully", True)
            current_span.add_event(
                name="routing_service.observation_dispatched_successfully"
            )
            print("Observation dispatched successfully.")


@cloud_event
async def main_async(event):
    await positions_dispatcher(event)


# Wrapper to be able to run the async function
def main(event, context):
    print(f"Event received:\n{event}\nContext:{context}")
    asyncio.run(main_async(event))
    return {}

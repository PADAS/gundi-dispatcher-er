import asyncio
import logging
from functions_framework import cloud_event
from core import tracing
from core.services import process_event

logger = logging.getLogger(__name__)


# Wrapper to be able to run the async function
@cloud_event
def main(event):
    print(f"CloudEvent received:\n{event}")
    asyncio.run(process_event(event))
    print(f"CloudEvent processed successfully.")
    return {}

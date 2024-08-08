import asyncio
import logging
from functions_framework import http
from core import tracing
from core.services import process_request

logger = logging.getLogger(__name__)


@http
def main(request):
    logger.info(f"Request received:\n{request}")
    body = request.data
    headers = request.headers
    print(f"Message Received.\n RAW body: {body}\n headers: {headers}")
    logger.debug(f"Request received:\n{request}")
    asyncio.run(process_request(request))
    logger.info(f"Request processed successfully.")
    return {}

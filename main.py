import asyncio
import logging
from functions_framework import http
from core import tracing
from core.services import process_request
from core.throttling import ThrottledMessage

logger = logging.getLogger(__name__)


@http
def main(request):
    logger.info(f"Request received:\n{request}")
    body = request.data
    headers = request.headers
    print(f"Message Received.\n RAW body: {body}\n headers: {headers}")
    logger.debug(f"Request received:\n{request}")
    try:
        asyncio.run(process_request(request))
    except ThrottledMessage as e:
        # Deferral, not failure: 429 nacks the push message so PubSub
        # redelivers it later. Deliberately no failure event, no activity log.
        logger.info(f"Message deferred by throttle gate, returning 429: {e}")
        return {
            "status": "throttled",
            "destination_id": str(e.destination_id),
            "family": e.family,
        }, 429
    logger.info(f"Request processed successfully.")
    return {}

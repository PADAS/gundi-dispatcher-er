import asyncio
import logging
import time

from redis import exceptions as redis_exceptions

from core import settings
from core import utils

logger = logging.getLogger(__name__)

EVENTS_FAMILY = "events"
OBSERVATIONS_FAMILY = "observations"
MESSAGES_FAMILY = "messages"
SITE_SCOPE = "site"

FAMILY_BY_STREAM_TYPE = {
    "ev": EVENTS_FAMILY,
    "evu": EVENTS_FAMILY,
    "att": EVENTS_FAMILY,  # attachments post to events
    "obv": OBSERVATIONS_FAMILY,
    "obvu": OBSERVATIONS_FAMILY,
    "txt": MESSAGES_FAMILY,
}


class ThrottledMessage(Exception):
    # Raised by check_admission when a message must be deferred. main.py turns
    # this into an HTTP 429 so PubSub nacks and redelivers later.
    def __init__(self, destination_id, family, reason, retry_after=None):
        super().__init__(
            f"Delivery throttled for destination {destination_id} ({family}): {reason}"
        )
        self.destination_id = destination_id
        self.family = family
        self.reason = reason  # "cooldown" | "rate"
        self.retry_after = retry_after


def get_family(stream_type):
    # Unknown stream types map to the most conservative family
    return FAMILY_BY_STREAM_TYPE.get(stream_type, EVENTS_FAMILY)


def _cap_for_family(family):
    return {
        EVENTS_FAMILY: settings.DEFAULT_MAX_EVENT_DELIVERIES_PER_MINUTE,
        OBSERVATIONS_FAMILY: settings.DEFAULT_MAX_OBSERVATION_DELIVERIES_PER_MINUTE,
        MESSAGES_FAMILY: settings.DEFAULT_MAX_MESSAGE_DELIVERIES_PER_MINUTE,
    }[family]


def _cooldown_key(destination_id, scope):
    return f"throttle:cooldown:{destination_id}:{scope}"


def _level_key(destination_id, scope):
    return f"throttle:cooldown_level:{destination_id}:{scope}"


def _rate_key(destination_id, family, window):
    return f"throttle:rate:{destination_id}:{family}:{window}"


def _evaluate(destination_id, family):
    # Returns (admitted, reason, retry_after). Plain commands instead of a Lua
    # script: INCR is atomic, and the check-then-increment race admits at most
    # a few extra messages — acceptable for a kindness cap, and it keeps this
    # module testable against the suite's MagicMock Redis.
    db = utils._cache_db
    for scope in (SITE_SCOPE, family):
        ttl = db.ttl(_cooldown_key(destination_id, scope))
        if ttl and ttl > 0:
            return False, "cooldown", ttl
    now = int(time.time())
    rate_key = _rate_key(destination_id, family, now // 60)
    count = db.incr(rate_key)
    if count == 1:
        # Two windows so a straggler INCR never resurrects an expired key
        db.expire(rate_key, 120)
    if count <= _cap_for_family(family):
        return True, None, None
    return False, "rate", 60 - (now % 60)


async def check_admission(destination_id, stream_type):
    # Raises ThrottledMessage when the message must be deferred (nacked).
    if not settings.THROTTLING_ENABLED or not destination_id:
        return
    family = get_family(stream_type)
    try:
        admitted, reason, retry_after = _evaluate(destination_id, family)
        if admitted:
            return
        if reason == "rate" and retry_after <= settings.THROTTLE_GRACE_WAIT_MAX_SECONDS:
            # The window opens soon: wait it out instead of paying a redelivery
            await asyncio.sleep(retry_after)
            admitted, reason, retry_after = _evaluate(destination_id, family)
            if admitted:
                return
    except redis_exceptions.RedisError as e:
        # Fail open: throttling is a kindness, not a correctness requirement
        logger.warning(f"Throttle gate unavailable, admitting message: {e}")
        return
    logger.info(
        f"Message deferred by throttle gate. destination_id={destination_id}, "
        f"family={family}, reason={reason}, retry_after={retry_after}"
    )
    raise ThrottledMessage(
        destination_id=destination_id, family=family, reason=reason, retry_after=retry_after
    )

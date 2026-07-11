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
        # TTL semantics: -2 missing, -1 no expiry (shouldn't happen for our
        # setex keys; treated as no cooldown, failing open), 0 = expiring this
        # second - still honored so nothing leaks through the final second.
        if ttl is not None and ttl >= 0:
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
    except Exception as e:
        # Fail open on ANY gate malfunction (Redis errors or bugs): throttling
        # is a kindness, not a correctness requirement, and a broken gate must
        # never 500-loop the stream.
        logger.warning(f"Throttle gate unavailable, admitting message: {e}")
        return
    logger.info(
        f"Message deferred by throttle gate. destination_id={destination_id}, "
        f"family={family}, reason={reason}, retry_after={retry_after}"
    )
    raise ThrottledMessage(
        destination_id=destination_id, family=family, reason=reason, retry_after=retry_after
    )


SITE_DISTRESS_STATUSES = {502, 503, 504}
TRANSPORT_ERROR_MARKER = "Request to ER failed"  # erclient's wrapper for httpx.RequestError


def _scope_for_failure(status_code, error):
    # 429 is endpoint-level rate limiting -> cool down the family only.
    # 5xx / transport failures are site-wide distress -> cool down everything.
    # 409 is ER's per-source limit ("one obs/sec/source") and must NOT pause
    # the whole destination; other errors are not distress signals.
    if status_code == 429:
        return "family"
    if status_code in SITE_DISTRESS_STATUSES:
        return SITE_SCOPE
    if not status_code and error and TRANSPORT_ERROR_MARKER in error:
        return SITE_SCOPE
    return None


def record_distress(destination_id, stream_type, status_code=None, error=None, retry_after=None):
    # Set/extend a cooldown after a failed delivery. Returns the scope string
    # ("site" or a family name) when the portal should be notified (first
    # cooldown within the notify window), else None.
    if not settings.THROTTLING_ENABLED or not destination_id:
        return None
    scope = _scope_for_failure(status_code, error)
    if not scope:
        return None
    scope_key = get_family(stream_type) if scope == "family" else SITE_SCOPE
    db = utils._cache_db
    try:
        level = db.incr(_level_key(destination_id, scope_key))
        db.expire(
            _level_key(destination_id, scope_key),
            settings.THROTTLE_COOLDOWN_LEVEL_TTL_SECONDS,
        )
        if retry_after:
            ttl = min(int(retry_after), settings.THROTTLE_COOLDOWN_MAX_SECONDS)
        else:
            ttl = min(
                settings.THROTTLE_COOLDOWN_BASE_SECONDS * (2 ** (level - 1)),
                settings.THROTTLE_COOLDOWN_MAX_SECONDS,
            )
        db.setex(_cooldown_key(destination_id, scope_key), ttl, scope_key)
        # One notification per destination per notify window
        notify = db.set(
            f"throttle:notify:{destination_id}", "1",
            ex=settings.THROTTLE_NOTIFY_TTL_SECONDS, nx=True,
        )
        return scope_key if notify else None
    except Exception as e:
        # Fail open on ANY error (Redis or bugs, e.g. a malformed retry_after):
        # this runs inside the dispatch failure handler, and an escaping
        # exception here would suppress the failure event for the portal.
        logger.warning(f"Could not record destination distress: {e}")
        return None


def record_success(destination_id, stream_type):
    # A successful delivery proves the site is reachable: clear the site scope
    # and the delivered family's scope. Other families' cooldowns are left to
    # expire — a flowing observation says nothing about the events endpoint.
    if not settings.THROTTLING_ENABLED or not destination_id:
        return
    family = get_family(stream_type)
    db = utils._cache_db
    try:
        db.delete(
            _cooldown_key(destination_id, SITE_SCOPE),
            _level_key(destination_id, SITE_SCOPE),
            _cooldown_key(destination_id, family),
            _level_key(destination_id, family),
        )
    except Exception as e:
        # Fail open: an escaping exception here would turn a SUCCESSFUL
        # delivery into a retry (duplicate data at the destination)
        logger.warning(f"Could not clear throttle state after successful delivery: {e}")

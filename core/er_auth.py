import json
import logging
from datetime import datetime, timezone
from urllib.parse import urlparse

from core.utils import get_redis_db

logger = logging.getLogger(__name__)

TOKEN_CACHE_KEY_PREFIX = "er_dispatcher.auth_token"
# Cached tokens valid for less than this are treated as a miss.
MIN_REMAINING_VALIDITY_SECONDS = 60
LOGIN_MAX_TRIES = 3
LOGIN_MAX_TIME_SECONDS = 10

_cache_db = get_redis_db()


def _token_cache_key(token_url, username):
    host = urlparse(token_url).hostname or token_url
    return f"{TOKEN_CACHE_KEY_PREFIX}.{host}.{username}"


def read_cached_token(token_url, username):
    """Return (access_token, expires_at) from the cache, or None. Never raises."""
    try:
        raw_entry = _cache_db.get(_token_cache_key(token_url, username))
    except Exception as e:
        logger.warning(f"Error reading ER auth token from cache: {e}")
        return None
    if not raw_entry:
        return None
    try:
        entry = json.loads(raw_entry)
        expires_at = datetime.fromisoformat(entry["expires_at"])
        return entry["access_token"], expires_at
    except (ValueError, KeyError, TypeError) as e:
        logger.warning(f"Discarding invalid ER auth token cache entry: {e}")
        return None


def write_cached_token(token_url, username, access_token, expires_at):
    """Cache an access token with TTL matching its expiry. Never raises."""
    ttl_seconds = int((expires_at - datetime.now(tz=timezone.utc)).total_seconds())
    if ttl_seconds <= 0:
        return
    entry = json.dumps(
        {"access_token": access_token, "expires_at": expires_at.isoformat()}
    )
    try:
        _cache_db.setex(_token_cache_key(token_url, username), ttl_seconds, entry)
    except Exception as e:
        logger.warning(f"Error writing ER auth token to cache: {e}")


def invalidate_cached_token(token_url, username):
    """Delete a cached token (e.g. after ER rejects it). Never raises."""
    try:
        _cache_db.delete(_token_cache_key(token_url, username))
    except Exception as e:
        logger.warning(f"Error deleting ER auth token from cache: {e}")

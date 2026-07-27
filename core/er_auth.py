import json
import logging
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import backoff
import httpx
from erclient import AsyncERClient

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
        # Discard naive datetimes (no timezone info) to prevent comparison errors
        if expires_at.tzinfo is None:
            logger.warning(f"Discarding invalid ER auth token cache entry: naive expires_at")
            return None
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


def _is_permanent_login_error(exception):
    """4xx from the token endpoint (bad credentials/request) — do not retry."""
    return (
        isinstance(exception, httpx.HTTPStatusError)
        and exception.response.status_code < 500
    )


class TokenCachingAsyncERClient(AsyncERClient):
    """
    AsyncERClient that shares password-grant tokens across dispatcher
    invocations via Redis, instead of logging in per instance.

    ER's /oauth2/token endpoint has a concurrency bug (django-oauth-toolkit
    #995/#960) triggered by simultaneous password grants for the same user,
    so logins are also retried with backoff on transient failures.
    The refresh-token grant is deliberately never used: refresh rotation is
    racy under concurrency, and a fresh password grant every ~47h is cheap.
    """

    async def auth_headers(self):
        # Static-token clients and clients that already logged in have valid
        # auth; a client with a year-2099 expiry (token= kwarg) always hits this.
        if not self._auth_is_valid():
            cached = read_cached_token(self.token_url, self.username)
            if cached:
                access_token, expires_at = cached
                min_valid_until = datetime.now(tz=timezone.utc) + timedelta(
                    seconds=MIN_REMAINING_VALIDITY_SECONDS
                )
                if expires_at > min_valid_until:
                    self.auth = {"token_type": "Bearer", "access_token": access_token}
                    self.auth_expires = expires_at
        if not self._auth_is_valid():
            # No refresh grant here on purpose (see class docstring).
            await self.login()
        return {
            "Authorization": f'{self.auth["token_type"]} {self.auth["access_token"]}',
            "Accept-Type": "application/json",
        }

    @backoff.on_exception(
        backoff.expo,
        (httpx.HTTPStatusError, httpx.RequestError),
        max_tries=LOGIN_MAX_TRIES,
        max_time=LOGIN_MAX_TIME_SECONDS,
        giveup=_is_permanent_login_error,
        logger=logger,
    )
    async def login(self):
        result = await super().login()
        write_cached_token(
            self.token_url, self.username, self.auth["access_token"], self.auth_expires
        )
        return result

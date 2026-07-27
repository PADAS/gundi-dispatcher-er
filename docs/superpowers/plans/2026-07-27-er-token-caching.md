# ER Token Caching Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop the dispatcher from doing an OAuth2 password grant per observation by caching ER access tokens in Redis, with backoff-retried logins and one-shot 401 recovery.

**Architecture:** A new `core/er_auth.py` provides `TokenCachingAsyncERClient` (subclass of erclient's `AsyncERClient`) that consults a Redis token cache before logging in, retries transient login failures with backoff, and writes fresh tokens back to the cache. `make_er_client` (v1 and v2) returns the subclass. Base dispatcher classes gain a `send()` wrapper that invalidates the cache and retries `_send()` once on 401.

**Tech Stack:** Python 3.10, `erclient` (earthranger-client 1.16.0), `httpx`, `walrus` (Redis), `backoff` 2.2, pytest + pytest-asyncio + pytest-mock.

**Spec:** `docs/superpowers/specs/2026-07-27-er-token-caching-design.md`

## Global Constraints

- No new dependencies — `backoff`, `walrus`, `httpx` are already in `requirements.txt`; do not touch `requirements.in`.
- The OAuth2 refresh-token grant must never be used (spec non-goal: refresh rotation is racy in DOT).
- All Redis access in the auth path must be failure-safe: log a warning and fall through, never raise (mirrors `core/utils.py::read_config_from_cache_safe`).
- Integrations configured with a static long-lived `token` must bypass caching entirely.
- Login retries: max 3 attempts, only on 5xx `httpx.HTTPStatusError` and `httpx.RequestError`; 4xx raises immediately.
- Cache key format: `er_dispatcher.auth_token.{host}.{username}`; value JSON `{"access_token": ..., "expires_at": <ISO-8601 UTC>}`.
- Cached tokens valid for < 60 seconds are treated as cache misses.
- Match the repo's existing code style (no type-annotation retrofits of untouched code; `logging` not `print`; tests in classes are not required — this repo uses flat test functions).
- Run commands from the repo root: `/Users/chrisdo/padas/gundi-dispatcher-er`. Use the repo venv: `.venv/bin/pytest`.

---

### Task 1: Token cache helpers in `core/er_auth.py`

**Files:**
- Create: `core/er_auth.py`
- Create: `tests/test_er_auth.py`

**Interfaces:**
- Consumes: `core.utils.get_redis_db()` (existing; returns a `walrus.Database`).
- Produces (used by Tasks 2, 3, 5, 6):
  - `read_cached_token(token_url: str, username: str) -> tuple[str, datetime] | None`
  - `write_cached_token(token_url: str, username: str, access_token: str, expires_at: datetime) -> None`
  - `invalidate_cached_token(token_url: str, username: str) -> None`
  - Module global `_cache_db` (patch target for tests: `core.er_auth._cache_db`)

- [ ] **Step 1: Write the failing tests**

Create `tests/test_er_auth.py`:

```python
import json
from datetime import datetime, timedelta, timezone

import pytest
from redis import exceptions as redis_exceptions

from core import er_auth

TOKEN_URL = "https://fake-site.pamdas.org/oauth2/token"
USERNAME = "gundi_serviceaccount"
EXPECTED_CACHE_KEY = "er_dispatcher.auth_token.fake-site.pamdas.org.gundi_serviceaccount"


def _cache_entry(token="cached-token", expires_in_hours=47):
    expires_at = datetime.now(tz=timezone.utc) + timedelta(hours=expires_in_hours)
    return (
        json.dumps({"access_token": token, "expires_at": expires_at.isoformat()}),
        expires_at,
    )


def test_read_cached_token_returns_token_and_expiry_on_hit(mocker):
    entry, expires_at = _cache_entry()
    mock_cache = mocker.MagicMock()
    mock_cache.get.return_value = entry
    mocker.patch("core.er_auth._cache_db", mock_cache)

    result = er_auth.read_cached_token(TOKEN_URL, USERNAME)

    assert result == ("cached-token", expires_at)
    mock_cache.get.assert_called_once_with(EXPECTED_CACHE_KEY)


def test_read_cached_token_returns_none_on_miss(mocker):
    mock_cache = mocker.MagicMock()
    mock_cache.get.return_value = None
    mocker.patch("core.er_auth._cache_db", mock_cache)

    assert er_auth.read_cached_token(TOKEN_URL, USERNAME) is None


def test_read_cached_token_returns_none_on_redis_error(mocker):
    mock_cache = mocker.MagicMock()
    mock_cache.get.side_effect = redis_exceptions.ConnectionError("redis is down")
    mocker.patch("core.er_auth._cache_db", mock_cache)

    assert er_auth.read_cached_token(TOKEN_URL, USERNAME) is None


def test_read_cached_token_returns_none_on_corrupt_entry(mocker):
    mock_cache = mocker.MagicMock()
    mock_cache.get.return_value = "not-json"
    mocker.patch("core.er_auth._cache_db", mock_cache)

    assert er_auth.read_cached_token(TOKEN_URL, USERNAME) is None


def test_write_cached_token_sets_entry_with_ttl(mocker):
    mock_cache = mocker.MagicMock()
    mocker.patch("core.er_auth._cache_db", mock_cache)
    expires_at = datetime.now(tz=timezone.utc) + timedelta(seconds=1000)

    er_auth.write_cached_token(TOKEN_URL, USERNAME, "new-token", expires_at)

    args, _ = mock_cache.setex.call_args
    key, ttl, entry = args
    assert key == EXPECTED_CACHE_KEY
    assert 990 <= ttl <= 1000
    parsed = json.loads(entry)
    assert parsed["access_token"] == "new-token"
    assert parsed["expires_at"] == expires_at.isoformat()


def test_write_cached_token_skips_already_expired_token(mocker):
    mock_cache = mocker.MagicMock()
    mocker.patch("core.er_auth._cache_db", mock_cache)
    expires_at = datetime.now(tz=timezone.utc) - timedelta(seconds=1)

    er_auth.write_cached_token(TOKEN_URL, USERNAME, "stale-token", expires_at)

    mock_cache.setex.assert_not_called()


def test_write_cached_token_swallows_redis_error(mocker):
    mock_cache = mocker.MagicMock()
    mock_cache.setex.side_effect = redis_exceptions.ConnectionError("redis is down")
    mocker.patch("core.er_auth._cache_db", mock_cache)
    expires_at = datetime.now(tz=timezone.utc) + timedelta(hours=1)

    er_auth.write_cached_token(TOKEN_URL, USERNAME, "new-token", expires_at)  # must not raise


def test_invalidate_cached_token_deletes_key(mocker):
    mock_cache = mocker.MagicMock()
    mocker.patch("core.er_auth._cache_db", mock_cache)

    er_auth.invalidate_cached_token(TOKEN_URL, USERNAME)

    mock_cache.delete.assert_called_once_with(EXPECTED_CACHE_KEY)


def test_invalidate_cached_token_swallows_redis_error(mocker):
    mock_cache = mocker.MagicMock()
    mock_cache.delete.side_effect = redis_exceptions.ConnectionError("redis is down")
    mocker.patch("core.er_auth._cache_db", mock_cache)

    er_auth.invalidate_cached_token(TOKEN_URL, USERNAME)  # must not raise
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_er_auth.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'core.er_auth'` (or ImportError).

- [ ] **Step 3: Write the implementation**

Create `core/er_auth.py`:

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_er_auth.py -v`
Expected: all 9 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add core/er_auth.py tests/test_er_auth.py
git commit -m "feat: add Redis-backed ER auth token cache helpers"
```

---

### Task 2: `TokenCachingAsyncERClient.login()` — backoff-retried grant that writes the cache

**Files:**
- Modify: `core/er_auth.py`
- Modify: `tests/test_er_auth.py`

**Interfaces:**
- Consumes: Task 1 helpers; `erclient.AsyncERClient.login()` / `_token_request()` (parent sets `self.auth` dict and `self.auth_expires` on success; raises `httpx.HTTPStatusError` on non-200).
- Produces (used by Tasks 3–6): class `core.er_auth.TokenCachingAsyncERClient(AsyncERClient)` with `login()` that retries transient failures and caches the token. Constructor signature identical to `AsyncERClient`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_er_auth.py`:

```python
import httpx


def _make_client():
    return er_auth.TokenCachingAsyncERClient(
        service_root="https://fake-site.pamdas.org/api/v1.0",
        username=USERNAME,
        password="fake-password",
        token_url=TOKEN_URL,
        client_id="das_web_client",
        provider_key="fake-provider",
    )


def _token_response(status_code=200):
    request = httpx.Request("POST", TOKEN_URL)
    if status_code == 200:
        return httpx.Response(
            200,
            json={
                "access_token": "new-token",
                "refresh_token": "fake-refresh-token",
                "expires_in": 172800,
                "token_type": "Bearer",
            },
            request=request,
        )
    return httpx.Response(status_code, json={"error": "error"}, request=request)


@pytest.fixture
def mock_token_cache(mocker):
    mock_cache = mocker.MagicMock()
    mock_cache.get.return_value = None
    mocker.patch("core.er_auth._cache_db", mock_cache)
    return mock_cache


@pytest.fixture
def fast_backoff(mocker):
    # backoff awaits asyncio.sleep between retries; skip the real waits
    return mocker.patch("asyncio.sleep", mocker.AsyncMock())


@pytest.mark.asyncio
async def test_login_success_sets_auth_and_writes_cache(mocker, mock_token_cache):
    client = _make_client()
    mock_post = mocker.AsyncMock(return_value=_token_response(200))
    mocker.patch.object(client._http_session, "post", mock_post)

    await client.login()

    assert client.auth["access_token"] == "new-token"
    args, _ = mock_token_cache.setex.call_args
    key, ttl, entry = args
    assert key == EXPECTED_CACHE_KEY
    assert json.loads(entry)["access_token"] == "new-token"
    # erclient subtracts a 5-minute margin from expires_in (48h)
    assert 0 < ttl <= 172800 - 5 * 60


@pytest.mark.asyncio
async def test_login_retries_on_transient_500_then_succeeds(
    mocker, mock_token_cache, fast_backoff
):
    client = _make_client()
    mock_post = mocker.AsyncMock(
        side_effect=[_token_response(500), _token_response(500), _token_response(200)]
    )
    mocker.patch.object(client._http_session, "post", mock_post)

    await client.login()

    assert client.auth["access_token"] == "new-token"
    assert mock_post.await_count == 3


@pytest.mark.asyncio
async def test_login_raises_after_max_retries_on_persistent_500(
    mocker, mock_token_cache, fast_backoff
):
    client = _make_client()
    mock_post = mocker.AsyncMock(return_value=_token_response(500))
    mocker.patch.object(client._http_session, "post", mock_post)

    with pytest.raises(httpx.HTTPStatusError):
        await client.login()

    assert mock_post.await_count == 3
    mock_token_cache.setex.assert_not_called()


@pytest.mark.asyncio
async def test_login_does_not_retry_on_bad_credentials_400(
    mocker, mock_token_cache, fast_backoff
):
    client = _make_client()
    mock_post = mocker.AsyncMock(return_value=_token_response(400))
    mocker.patch.object(client._http_session, "post", mock_post)

    with pytest.raises(httpx.HTTPStatusError):
        await client.login()

    assert mock_post.await_count == 1


@pytest.mark.asyncio
async def test_login_retries_on_network_error(mocker, mock_token_cache, fast_backoff):
    client = _make_client()
    mock_post = mocker.AsyncMock(
        side_effect=[
            httpx.ConnectError("connection refused", request=httpx.Request("POST", TOKEN_URL)),
            _token_response(200),
        ]
    )
    mocker.patch.object(client._http_session, "post", mock_post)

    await client.login()

    assert client.auth["access_token"] == "new-token"
    assert mock_post.await_count == 2
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_er_auth.py -v`
Expected: the 5 new tests FAIL with `AttributeError: module 'core.er_auth' has no attribute 'TokenCachingAsyncERClient'`; the 9 Task-1 tests still PASS.

- [ ] **Step 3: Write the implementation**

In `core/er_auth.py`, add to the imports at the top:

```python
import backoff
import httpx
from erclient import AsyncERClient
```

Append at the bottom:

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_er_auth.py -v`
Expected: all 14 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add core/er_auth.py tests/test_er_auth.py
git commit -m "feat: add TokenCachingAsyncERClient with backoff-retried, cache-writing login"
```

---

### Task 3: `auth_headers()` override — consult the cache before logging in

**Files:**
- Modify: `core/er_auth.py`
- Modify: `tests/test_er_auth.py`

**Interfaces:**
- Consumes: Task 1 `read_cached_token`; Task 2 `login()`; parent attributes `self.auth` (dict with `token_type`/`access_token`), `self.auth_expires`, `self._auth_is_valid()`.
- Produces: `TokenCachingAsyncERClient.auth_headers() -> dict` — same contract as parent (`Authorization` + `Accept-Type` headers), used implicitly by every erclient `post_*` method.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_er_auth.py`:

```python
@pytest.mark.asyncio
async def test_auth_headers_uses_cached_token_without_login(mocker, mock_token_cache):
    entry, _ = _cache_entry(token="cached-token", expires_in_hours=47)
    mock_token_cache.get.return_value = entry
    client = _make_client()
    mock_post = mocker.AsyncMock()
    mocker.patch.object(client._http_session, "post", mock_post)

    headers = await client.auth_headers()

    assert headers["Authorization"] == "Bearer cached-token"
    mock_post.assert_not_awaited()


@pytest.mark.asyncio
async def test_auth_headers_logs_in_on_cache_miss(mocker, mock_token_cache):
    client = _make_client()
    mock_post = mocker.AsyncMock(return_value=_token_response(200))
    mocker.patch.object(client._http_session, "post", mock_post)

    headers = await client.auth_headers()

    assert headers["Authorization"] == "Bearer new-token"
    assert mock_post.await_count == 1


@pytest.mark.asyncio
async def test_auth_headers_treats_nearly_expired_cache_entry_as_miss(
    mocker, mock_token_cache
):
    # Valid for 30s — under the 60s minimum remaining validity
    expires_at = datetime.now(tz=timezone.utc) + timedelta(seconds=30)
    mock_token_cache.get.return_value = json.dumps(
        {"access_token": "nearly-expired", "expires_at": expires_at.isoformat()}
    )
    client = _make_client()
    mock_post = mocker.AsyncMock(return_value=_token_response(200))
    mocker.patch.object(client._http_session, "post", mock_post)

    headers = await client.auth_headers()

    assert headers["Authorization"] == "Bearer new-token"
    assert mock_post.await_count == 1


@pytest.mark.asyncio
async def test_auth_headers_reuses_in_memory_auth_without_touching_cache(
    mocker, mock_token_cache
):
    client = _make_client()
    mock_post = mocker.AsyncMock(return_value=_token_response(200))
    mocker.patch.object(client._http_session, "post", mock_post)

    await client.auth_headers()  # first call logs in
    await client.auth_headers()  # second call reuses self.auth

    assert mock_post.await_count == 1
    assert mock_token_cache.get.call_count == 1


@pytest.mark.asyncio
async def test_auth_headers_with_static_token_never_touches_cache(
    mocker, mock_token_cache
):
    client = er_auth.TokenCachingAsyncERClient(
        service_root="https://fake-site.pamdas.org/api/v1.0",
        token="static-long-lived-token",
        token_url=TOKEN_URL,
        client_id="das_web_client",
        provider_key="fake-provider",
    )
    mock_post = mocker.AsyncMock()
    mocker.patch.object(client._http_session, "post", mock_post)

    headers = await client.auth_headers()

    assert headers["Authorization"] == "Bearer static-long-lived-token"
    mock_token_cache.get.assert_not_called()
    mock_post.assert_not_awaited()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_er_auth.py -v`
Expected: `test_auth_headers_uses_cached_token_without_login` and
`test_auth_headers_treats_nearly_expired_cache_entry_as_miss` FAIL (the parent
`auth_headers` never reads the cache — the first errors trying the refresh
grant or logging in; the near-expiry test may fail on call counts). The other
three may already pass via parent behavior; that is fine.

- [ ] **Step 3: Write the implementation**

In `core/er_auth.py`, extend the datetime import at the top of the file:

```python
from datetime import datetime, timedelta, timezone
```

Add to `TokenCachingAsyncERClient` (above `login`):

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_er_auth.py -v`
Expected: all 19 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add core/er_auth.py tests/test_er_auth.py
git commit -m "feat: consult Redis token cache in auth_headers before logging in"
```

---

### Task 4: Wire `TokenCachingAsyncERClient` into both `make_er_client` factories

**Files:**
- Modify: `core/dispatchers.py` (lines ~51 and ~185: the two `return AsyncERClient(` calls; line ~33: v1 `__init__`)
- Modify: `tests/test_dispatchers.py`, `tests/test_process_observations.py`, `tests/test_process_observations_v2.py`, `tests/test_throttling.py` (patch-target rename)

**Interfaces:**
- Consumes: Task 2/3 `TokenCachingAsyncERClient` (constructor-compatible with `AsyncERClient`).
- Produces: `ERDispatcher.make_er_client(...)` and `ERDispatcherV2.make_er_client(...)` return `TokenCachingAsyncERClient`; `ERDispatcher` instances now keep `self.provider` (Task 6 needs it for client rebuild). Test patch target becomes `core.dispatchers.TokenCachingAsyncERClient`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_er_auth.py`:

```python
from types import SimpleNamespace

from core.dispatchers import ERDispatcher, ERDispatcherV2


def test_make_er_client_v1_returns_token_caching_client():
    config = SimpleNamespace(
        endpoint="https://fake-site.pamdas.org",
        login=USERNAME,
        password="fake-password",
        token=None,
    )
    client = ERDispatcher.make_er_client(config, "fake-provider")
    assert isinstance(client, er_auth.TokenCachingAsyncERClient)


def test_make_er_client_v2_returns_token_caching_client(destination_integration_v2):
    client = ERDispatcherV2.make_er_client(
        integration=destination_integration_v2, provider="fake-provider"
    )
    assert isinstance(client, er_auth.TokenCachingAsyncERClient)
```

(`destination_integration_v2` is an existing fixture in `tests/conftest.py`.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_er_auth.py -v -k make_er_client`
Expected: both FAIL with `assert isinstance(...)` — the factories still return plain `AsyncERClient`.

- [ ] **Step 3: Modify `core/dispatchers.py`**

Add the import (below `from erclient import AsyncERClient`, which stays — it is still the base type):

```python
from core.er_auth import TokenCachingAsyncERClient
```

In `ERDispatcher.__init__` (v1), store the provider (needed by Task 6's rebuild):

```python
    def __init__(self, config: schemas.OutboundConfiguration, provider: str):
        super().__init__(config)
        self.provider = provider
        self.er_client = self.make_er_client(config, provider)
```

In **both** `ERDispatcher.make_er_client` and `ERDispatcherV2.make_er_client`, change the return statement's class:

```python
        return TokenCachingAsyncERClient(
```

(constructor arguments are unchanged in both).

- [ ] **Step 4: Update the patch target in existing tests**

The existing suite patches the class dispatchers use; rename all 28 occurrences:

```bash
sed -i '' 's/core\.dispatchers\.AsyncERClient/core.dispatchers.TokenCachingAsyncERClient/g' \
  tests/test_dispatchers.py tests/test_process_observations.py \
  tests/test_process_observations_v2.py tests/test_throttling.py
```

- [ ] **Step 5: Run the full suite to verify everything passes**

Run: `.venv/bin/pytest -x -q`
Expected: all tests PASS (the two new ones and the whole existing suite).

- [ ] **Step 6: Commit**

```bash
git add core/dispatchers.py tests/
git commit -m "feat: dispatchers build TokenCachingAsyncERClient instead of AsyncERClient"
```

---

### Task 5: 401 invalidate-and-retry wrapper in `ERDispatcherV2`

**Files:**
- Modify: `core/dispatchers.py` (class `ERDispatcherV2` and its five subclasses: `EREventDispatcher`, `EREventUpdateDispatcher`, `EREventAttachmentDispatcher`, `ERObservationDispatcher`, `ERMessageDispatcher`)
- Modify: `tests/test_dispatchers.py`

**Interfaces:**
- Consumes: Task 1 `invalidate_cached_token(token_url, username)`; Task 4 factories; `erclient.er_errors.ERClientBadCredentials`; `AsyncERClient` instance attributes `token_url` / `username`; v2 instance attrs `self.integration`, `self.provider`.
- Produces: `ERDispatcherV2.send(data, **kwargs)` (concrete, public — call sites in `core/event_handlers.py` are unchanged); abstract `ERDispatcherV2._send(data, **kwargs)` implemented by each subclass (the old `send` bodies, renamed).

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_dispatchers.py`:

```python
def _make_erclient_mock_for_auth_retry(mocker, post_method_name, side_effect):
    erclient_mock = mocker.MagicMock()
    setattr(erclient_mock, post_method_name, mocker.AsyncMock(side_effect=side_effect))
    erclient_mock.__aenter__ = mocker.AsyncMock(return_value=erclient_mock)
    erclient_mock.__aexit__ = mocker.AsyncMock(return_value=None)
    erclient_mock.close = mocker.AsyncMock(return_value=None)
    erclient_mock.token_url = "https://fake-site.pamdas.org/oauth2/token"
    erclient_mock.username = "fake-username"
    return erclient_mock


@pytest.mark.asyncio
async def test_v2_dispatcher_retries_send_once_on_bad_credentials(
    mocker,
    mock_cache_empty,
    mock_er_bad_credentials_error,
    post_report_response,
    destination_integration_v2,
    event_v2_transformed_er,
):
    mocker.patch("core.er_auth._cache_db", mock_cache_empty)
    erclient_mock = _make_erclient_mock_for_auth_retry(
        mocker, "post_report", [mock_er_bad_credentials_error, post_report_response]
    )
    mocked_erclient_class = mocker.MagicMock(return_value=erclient_mock)
    mocker.patch("core.dispatchers.TokenCachingAsyncERClient", mocked_erclient_class)
    dispatcher = dispatchers.EREventDispatcher(
        integration=destination_integration_v2, provider="fake-provider"
    )

    result = await dispatcher.send(event_v2_transformed_er.payload)

    assert result == post_report_response
    assert erclient_mock.post_report.await_count == 2
    mock_cache_empty.delete.assert_called_once()  # cached token invalidated
    assert mocked_erclient_class.call_count == 2  # client rebuilt for the retry


@pytest.mark.asyncio
async def test_v2_dispatcher_raises_on_second_bad_credentials(
    mocker,
    mock_cache_empty,
    mock_er_bad_credentials_error,
    destination_integration_v2,
    event_v2_transformed_er,
):
    mocker.patch("core.er_auth._cache_db", mock_cache_empty)
    erclient_mock = _make_erclient_mock_for_auth_retry(
        mocker,
        "post_report",
        [mock_er_bad_credentials_error, mock_er_bad_credentials_error],
    )
    mocker.patch(
        "core.dispatchers.TokenCachingAsyncERClient",
        mocker.MagicMock(return_value=erclient_mock),
    )
    dispatcher = dispatchers.EREventDispatcher(
        integration=destination_integration_v2, provider="fake-provider"
    )

    with pytest.raises(er_errors.ERClientBadCredentials):
        await dispatcher.send(event_v2_transformed_er.payload)

    assert erclient_mock.post_report.await_count == 2


@pytest.mark.asyncio
async def test_v2_dispatcher_does_not_retry_on_permission_denied(
    mocker,
    mock_cache_empty,
    mock_er_missing_permissions_error,
    destination_integration_v2,
    event_v2_transformed_er,
):
    mocker.patch("core.er_auth._cache_db", mock_cache_empty)
    erclient_mock = _make_erclient_mock_for_auth_retry(
        mocker, "post_report", [mock_er_missing_permissions_error]
    )
    mocker.patch(
        "core.dispatchers.TokenCachingAsyncERClient",
        mocker.MagicMock(return_value=erclient_mock),
    )
    dispatcher = dispatchers.EREventDispatcher(
        integration=destination_integration_v2, provider="fake-provider"
    )

    with pytest.raises(er_errors.ERClientPermissionDenied):
        await dispatcher.send(event_v2_transformed_er.payload)

    assert erclient_mock.post_report.await_count == 1
    mock_cache_empty.delete.assert_not_called()
```

Check the imports at the top of `tests/test_dispatchers.py` and ensure these exist (add any that are missing):

```python
import pytest
from erclient import er_errors
from core import dispatchers
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_dispatchers.py -v -k "bad_credentials or permission_denied"`
Expected: the two retry tests FAIL (`post_report.await_count == 1`, no `delete` call, class called once) — today the first 401 propagates immediately. The permission-denied test may already pass; that is fine.

- [ ] **Step 3: Implement the wrapper in `ERDispatcherV2`**

In `core/dispatchers.py`, extend the er_auth import from Task 4:

```python
from core.er_auth import TokenCachingAsyncERClient, invalidate_cached_token
```

Add near the other erclient import:

```python
from erclient.er_errors import ERClientBadCredentials
```

Add to `ERDispatcherV2` (after `__init__`), and declare `_send` abstract:

```python
    async def send(self, data, **kwargs):
        try:
            return await self._send(data, **kwargs)
        except ERClientBadCredentials:
            logger.warning(
                "ER rejected the auth token (401). Invalidating cached token and retrying once.",
                extra={"integration_id": str(self.integration.id)},
            )
            invalidate_cached_token(self.er_client.token_url, self.er_client.username)
            # The failed _send closed the client's http session; build a fresh one.
            self.er_client = self.make_er_client(
                integration=self.integration, provider=self.provider
            )
            return await self._send(data, **kwargs)

    @abstractmethod
    async def _send(self, data, **kwargs):
        ...
```

Rename `send` → `_send` in the five v2 subclasses (method bodies unchanged):
- `EREventDispatcher.send` → `_send`
- `EREventUpdateDispatcher.send` → `_send`
- `EREventAttachmentDispatcher.send` → `_send`
- `ERObservationDispatcher.send` → `_send`
- `ERMessageDispatcher.send` → `_send`

Note: `DispatcherV2` (the parent ABC) keeps its abstract `send`; `ERDispatcherV2.send` satisfies it.

- [ ] **Step 4: Run the full suite**

Run: `.venv/bin/pytest -x -q`
Expected: all PASS. Note for the pre-existing parametrized test
`test_dispatcher_raises_exception_on_er_api_error[...bad_credentials...]`: the
mocked method's `side_effect` is a persistent exception, so the wrapper's
single retry raises again and the test's expectation (exception propagates)
still holds.

- [ ] **Step 5: Commit**

```bash
git add core/dispatchers.py tests/test_dispatchers.py
git commit -m "feat: v2 dispatchers invalidate cached token and retry once on 401"
```

---

### Task 6: Same 401 wrapper for the v1 `ERDispatcher`

**Files:**
- Modify: `core/dispatchers.py` (class `ERDispatcher` and its three subclasses: `ERPositionDispatcher`, `ERGeoEventDispatcher`, `ERCameraTrapDispatcher`)
- Modify: `tests/test_er_auth.py`

**Interfaces:**
- Consumes: Task 4's `self.provider` on `ERDispatcher`; Task 5's imports (`ERClientBadCredentials`, `invalidate_cached_token`).
- Produces: `ERDispatcher.send(data, **kwargs)` (concrete) + abstract `ERDispatcher._send(data, **kwargs)`; v1 call site `core/services.py::dispatcher.send(...)` unchanged.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_er_auth.py`:

```python
from erclient import er_errors


@pytest.mark.asyncio
async def test_v1_dispatcher_retries_send_once_on_bad_credentials(
    mocker, mock_er_bad_credentials_error
):
    mock_cache = mocker.MagicMock()
    mocker.patch("core.er_auth._cache_db", mock_cache)
    erclient_mock = mocker.MagicMock()
    erclient_mock.post_sensor_observation = mocker.AsyncMock(
        side_effect=[mock_er_bad_credentials_error, {"status": "ok"}]
    )
    erclient_mock.close = mocker.AsyncMock(return_value=None)
    erclient_mock.token_url = TOKEN_URL
    erclient_mock.username = USERNAME
    mocked_erclient_class = mocker.MagicMock(return_value=erclient_mock)
    mocker.patch("core.dispatchers.TokenCachingAsyncERClient", mocked_erclient_class)
    config = SimpleNamespace(
        endpoint="https://fake-site.pamdas.org",
        login=USERNAME,
        password="fake-password",
        token=None,
    )
    from core.dispatchers import ERPositionDispatcher

    dispatcher = ERPositionDispatcher(config, "fake-provider")

    result = await dispatcher.send({"recorded_at": "2026-07-27T10:00:00Z"})

    assert result == {"status": "ok"}
    assert erclient_mock.post_sensor_observation.await_count == 2
    mock_cache.delete.assert_called_once()
    assert mocked_erclient_class.call_count == 2
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest tests/test_er_auth.py -v -k v1_dispatcher`
Expected: FAIL — first 401 propagates (`await_count == 1`).

- [ ] **Step 3: Implement the wrapper in `ERDispatcher`**

Add to `ERDispatcher` in `core/dispatchers.py` (after `__init__`):

```python
    async def send(self, data, **kwargs):
        try:
            return await self._send(data, **kwargs)
        except ERClientBadCredentials:
            logger.warning(
                "ER rejected the auth token (401). Invalidating cached token and retrying once.",
            )
            invalidate_cached_token(self.er_client.token_url, self.er_client.username)
            # The failed _send closed the client's http session; build a fresh one.
            self.er_client = self.make_er_client(self.configuration, self.provider)
            return await self._send(data, **kwargs)

    @abstractmethod
    async def _send(self, data, **kwargs):
        ...
```

Rename `send` → `_send` in the three v1 subclasses (bodies unchanged):
- `ERPositionDispatcher.send` → `_send`
- `ERGeoEventDispatcher.send` → `_send`
- `ERCameraTrapDispatcher.send` → `_send`

- [ ] **Step 4: Run the full suite**

Run: `.venv/bin/pytest -x -q`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add core/dispatchers.py tests/test_er_auth.py
git commit -m "feat: v1 dispatchers invalidate cached token and retry once on 401"
```

---

### Task 7: Documentation and final verification

**Files:**
- Modify: `CLAUDE.md` (Architecture section + Key modules table)

**Interfaces:**
- Consumes: everything above.
- Produces: n/a (docs + final green suite).

- [ ] **Step 1: Update `CLAUDE.md`**

In the "Key modules" table, add a row after `core/dispatchers.py`:

```markdown
| `core/er_auth.py` | `TokenCachingAsyncERClient` — Redis-cached ER auth tokens, backoff-retried password grants; 401 → invalidate + one retry (see `docs/superpowers/specs/2026-07-27-er-token-caching-design.md`) |
```

In the "Redis usage" section, add a bullet:

```markdown
- ER auth tokens from password grants are cached under `er_dispatcher.auth_token.{host}.{username}` with TTL matching token expiry (~48h), so dispatch does not perform an OAuth2 grant per message. Static-token integrations bypass this cache.
```

- [ ] **Step 2: Run the full test suite one final time**

Run: `.venv/bin/pytest -q`
Expected: entire suite PASSES.

- [ ] **Step 3: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: document ER token caching in CLAUDE.md"
```

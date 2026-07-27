import hashlib
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import httpx
import pytest
from erclient import er_errors
from redis import exceptions as redis_exceptions

from core import er_auth
from core.dispatchers import ERDispatcher, ERDispatcherV2, ERPositionDispatcher

TOKEN_URL = "https://fake-site.pamdas.org/oauth2/token"
USERNAME = "gundi_serviceaccount"
PASSWORD = "fake-password"
_CREDENTIAL_FINGERPRINT = hashlib.sha256(f"{USERNAME}:{PASSWORD}".encode()).hexdigest()[:16]
EXPECTED_CACHE_KEY = (
    f"er_dispatcher.auth_token.fake-site.pamdas.org.gundi_serviceaccount.{_CREDENTIAL_FINGERPRINT}"
)


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

    result = er_auth.read_cached_token(TOKEN_URL, USERNAME, PASSWORD)

    assert result == ("cached-token", expires_at)
    mock_cache.get.assert_called_once_with(EXPECTED_CACHE_KEY)


def test_read_cached_token_returns_none_on_miss(mocker):
    mock_cache = mocker.MagicMock()
    mock_cache.get.return_value = None
    mocker.patch("core.er_auth._cache_db", mock_cache)

    assert er_auth.read_cached_token(TOKEN_URL, USERNAME, PASSWORD) is None


def test_read_cached_token_returns_none_on_redis_error(mocker):
    mock_cache = mocker.MagicMock()
    mock_cache.get.side_effect = redis_exceptions.ConnectionError("redis is down")
    mocker.patch("core.er_auth._cache_db", mock_cache)

    assert er_auth.read_cached_token(TOKEN_URL, USERNAME, PASSWORD) is None


def test_read_cached_token_returns_none_on_corrupt_entry(mocker):
    mock_cache = mocker.MagicMock()
    mock_cache.get.return_value = "not-json"
    mocker.patch("core.er_auth._cache_db", mock_cache)

    assert er_auth.read_cached_token(TOKEN_URL, USERNAME, PASSWORD) is None


def test_read_cached_token_returns_none_on_null_access_token(mocker):
    expires_at = datetime.now(tz=timezone.utc) + timedelta(hours=47)
    mock_cache = mocker.MagicMock()
    mock_cache.get.return_value = json.dumps(
        {"access_token": None, "expires_at": expires_at.isoformat()}
    )
    mocker.patch("core.er_auth._cache_db", mock_cache)

    assert er_auth.read_cached_token(TOKEN_URL, USERNAME, PASSWORD) is None


def test_write_cached_token_sets_entry_with_ttl(mocker):
    mock_cache = mocker.MagicMock()
    mocker.patch("core.er_auth._cache_db", mock_cache)
    expires_at = datetime.now(tz=timezone.utc) + timedelta(seconds=1000)

    er_auth.write_cached_token(TOKEN_URL, USERNAME, PASSWORD, "new-token", expires_at)

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

    er_auth.write_cached_token(TOKEN_URL, USERNAME, PASSWORD, "stale-token", expires_at)

    mock_cache.setex.assert_not_called()


def test_write_cached_token_swallows_redis_error(mocker):
    mock_cache = mocker.MagicMock()
    mock_cache.setex.side_effect = redis_exceptions.ConnectionError("redis is down")
    mocker.patch("core.er_auth._cache_db", mock_cache)
    expires_at = datetime.now(tz=timezone.utc) + timedelta(hours=1)

    er_auth.write_cached_token(TOKEN_URL, USERNAME, PASSWORD, "new-token", expires_at)  # must not raise


def test_invalidate_cached_token_deletes_key(mocker):
    mock_cache = mocker.MagicMock()
    mocker.patch("core.er_auth._cache_db", mock_cache)

    er_auth.invalidate_cached_token(TOKEN_URL, USERNAME, PASSWORD)

    mock_cache.delete.assert_called_once_with(EXPECTED_CACHE_KEY)


def test_invalidate_cached_token_swallows_redis_error(mocker):
    mock_cache = mocker.MagicMock()
    mock_cache.delete.side_effect = redis_exceptions.ConnectionError("redis is down")
    mocker.patch("core.er_auth._cache_db", mock_cache)

    er_auth.invalidate_cached_token(TOKEN_URL, USERNAME, PASSWORD)  # must not raise


def test_cached_token_is_not_shared_across_different_passwords(mocker):
    fake_store = {}
    mock_cache = mocker.MagicMock()
    mock_cache.setex.side_effect = lambda key, ttl, value: fake_store.__setitem__(key, value)
    mock_cache.get.side_effect = fake_store.get
    mocker.patch("core.er_auth._cache_db", mock_cache)
    expires_at = datetime.now(tz=timezone.utc) + timedelta(hours=47)

    er_auth.write_cached_token(TOKEN_URL, USERNAME, PASSWORD, "secret-token", expires_at)

    assert er_auth.read_cached_token(TOKEN_URL, USERNAME, PASSWORD) == ("secret-token", expires_at)
    # A client that does not present the same password must not see the token.
    assert er_auth.read_cached_token(TOKEN_URL, USERNAME, "wrong-password") is None


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


@pytest.mark.asyncio
async def test_auth_headers_discards_cached_token_with_naive_expires_at(
    mocker, mock_token_cache
):
    # Cache entry with naive datetime (no UTC offset) — should be treated as invalid
    naive_expires_at = datetime.now() + timedelta(hours=47)
    mock_token_cache.get.return_value = json.dumps(
        {"access_token": "naive-token", "expires_at": naive_expires_at.isoformat()}
    )
    client = _make_client()
    mock_post = mocker.AsyncMock(return_value=_token_response(200))
    mocker.patch.object(client._http_session, "post", mock_post)

    headers = await client.auth_headers()

    # Should have logged in with fresh token, not used the naive one
    assert headers["Authorization"] == "Bearer new-token"
    assert mock_post.await_count == 1


@pytest.mark.asyncio
async def test_auth_headers_discards_cached_token_with_null_access_token(
    mocker, mock_token_cache
):
    # Cache entry with a null access_token — should be treated as invalid
    expires_at = datetime.now(tz=timezone.utc) + timedelta(hours=47)
    mock_token_cache.get.return_value = json.dumps(
        {"access_token": None, "expires_at": expires_at.isoformat()}
    )
    client = _make_client()
    mock_post = mocker.AsyncMock(return_value=_token_response(200))
    mocker.patch.object(client._http_session, "post", mock_post)

    headers = await client.auth_headers()

    # Should have logged in with fresh token, not produced "Bearer None"
    assert headers["Authorization"] == "Bearer new-token"
    assert mock_post.await_count == 1


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


def test_make_er_client_v1_normalizes_http_token_url_to_https():
    config = SimpleNamespace(
        endpoint="http://fake-site.pamdas.org",
        login=USERNAME,
        password="fake-password",
        token=None,
    )
    client = ERDispatcher.make_er_client(config, "fake-provider")
    assert client.service_root == "https://fake-site.pamdas.org/api/v1.0"
    assert client.token_url == "https://fake-site.pamdas.org/oauth2/token"


def test_make_er_client_v2_normalizes_http_token_url_to_https(destination_integration_v2):
    integration = destination_integration_v2.copy(
        update={"base_url": "http://gundi-load-testing.pamdas.org"}
    )
    client = ERDispatcherV2.make_er_client(
        integration=integration, provider="fake-provider"
    )
    assert client.service_root == "https://gundi-load-testing.pamdas.org/api/v1.0"
    assert client.token_url == "https://gundi-load-testing.pamdas.org/oauth2/token"


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
    erclient_mock.password = PASSWORD
    mocked_erclient_class = mocker.MagicMock(return_value=erclient_mock)
    mocker.patch("core.dispatchers.TokenCachingAsyncERClient", mocked_erclient_class)
    config = SimpleNamespace(
        endpoint="https://fake-site.pamdas.org",
        login=USERNAME,
        password="fake-password",
        token=None,
    )

    dispatcher = ERPositionDispatcher(config, "fake-provider")

    result = await dispatcher.send({"recorded_at": "2026-07-27T10:00:00Z"})

    assert result == {"status": "ok"}
    assert erclient_mock.post_sensor_observation.await_count == 2
    mock_cache.delete.assert_called_once()
    assert mocked_erclient_class.call_count == 2


@pytest.mark.asyncio
async def test_v1_dispatcher_does_not_retry_static_token_client_on_bad_credentials(
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
    erclient_mock.username = None
    mocked_erclient_class = mocker.MagicMock(return_value=erclient_mock)
    mocker.patch("core.dispatchers.TokenCachingAsyncERClient", mocked_erclient_class)
    config = SimpleNamespace(
        endpoint="https://fake-site.pamdas.org",
        login=None,
        password=None,
        token="static-long-lived-token",
    )

    dispatcher = ERPositionDispatcher(config, "fake-provider")

    with pytest.raises(er_errors.ERClientBadCredentials):
        await dispatcher.send({"recorded_at": "2026-07-27T10:00:00Z"})

    assert erclient_mock.post_sensor_observation.await_count == 1
    mock_cache.delete.assert_not_called()

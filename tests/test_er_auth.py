import json
from datetime import datetime, timedelta, timezone

import httpx
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

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

# ER Token Caching, Refresh, and Auth-Failure Recovery

**Date:** 2026-07-27
**Repo:** gundi-dispatcher-er
**Status:** Approved

## Problem

For destination integrations configured with username/password (instead of a
long-lived token), the dispatcher performs a fresh OAuth2 password grant
against EarthRanger's `/oauth2/token` for **every observation delivered**.
Each PubSub message builds a new `AsyncERClient`
(`core/dispatchers.py::make_er_client`), so erclient's in-memory token cache
never survives past one message.

Bursts of concurrent password grants for the same integration user trigger a
known django-oauth-toolkit concurrency bug on the EarthRanger side
([django-oauth-toolkit#995](https://github.com/django-oauth/django-oauth-toolkit/issues/995),
[#960](https://github.com/django-oauth/django-oauth-toolkit/issues/960)):
DOT's `TokenView.post` issues the token, then intermittently fails the
immediate re-read with `AccessToken.DoesNotExist` and returns a 500. Observed
in ER prod at hundreds of events/day across dozens of sites; each 500 fails a
delivery, emits a spurious `ObservationDeliveryFailed` event, and forces a
PubSub retry.

## Goals

1. **(a) Cache** the access token obtained from a password grant, shared
   across function instances.
2. **(b) Handle expiration** so a stale token is replaced without failing
   deliveries.
3. **(c) Recover gracefully** from transient `/oauth2/token` 500s and from a
   cached token being rejected (401).

## Non-goals

- Changes to erclient (fix is contained in this repo; erclient's existing
  `token=` / `auth` seam is sufficient).
- Cross-instance login lock (single-flight). A cold cache may still cause a
  handful of concurrent grants roughly once per ~48h per integration; that is
  rare enough. Revisit if the DOT bug still fires at that frequency.
- Using the OAuth2 refresh-token grant. Refresh rotation is itself racy in
  DOT (#960), and multiple function instances sharing one refresh token would
  reintroduce a concurrency bug. A fresh password grant every ~47h is cheap.

## Design

### 1. New module `core/er_auth.py`

**`TokenCachingAsyncERClient(AsyncERClient)`** — a thin subclass; plus module
helpers for the Redis cache.

- **Cache key:** `er_dispatcher.auth_token.{host}.{username}` where `host` is
  the token URL's hostname. Value: JSON
  `{"access_token": "...", "expires_at": "<ISO-8601 UTC>"}`. Redis TTL set to
  `expires_at - now`. Uses the existing `core.utils.get_redis_db()` database.
- **Safe cache access:** every Redis read/write/delete is wrapped in
  try/except that logs a warning and falls through (mirrors
  `read_config_from_cache_safe`). Redis being down degrades to today's
  behavior (login per message); it never breaks delivery.
- **`auth_headers()` override** (replaces the parent implementation):
  1. If current auth is valid (static token from config, or auth already
     fetched by this instance) → return headers.
  2. Else read the cache; accept only if valid for **≥ 60 more seconds**;
     set `self.auth` / `self.auth_expires` from it.
  3. Else `await self.login()`.
  The parent's `refresh_token()` path is intentionally never invoked
  (cache-sourced auth carries no refresh token).
- **`login()` override:** wraps the parent password grant with
  `backoff.expo` retries — max 3 attempts, total wait capped ≈ 10s to stay
  within the function timeout — on:
  - `httpx.HTTPStatusError` with a 5xx status (the DOT race manifests as a
    transient 500), and
  - `httpx.RequestError` (network errors/timeouts).
  4xx (bad credentials) is **not** retried. On success, writes the token to
  the cache with TTL derived from `self.auth_expires` (erclient already
  subtracts a 5-minute margin from `expires_in`; ER's default token lifetime
  is 48h).
- **`invalidate_cached_token(token_url, username)`** — deletes the cache key.

Static-token integrations are untouched: erclient's constructor sets `auth`
with a year-2099 expiry, so step 1 always short-circuits.

### 2. Wiring (`core/dispatchers.py`)

`ERDispatcher.make_er_client` (v1) and `ERDispatcherV2.make_er_client` (v2)
return `TokenCachingAsyncERClient` instead of `AsyncERClient`. Constructor
arguments are unchanged. No changes to any stream-type dispatcher's send
logic beyond the rename below.

### 3. 401 recovery (base dispatcher classes)

Concrete send methods in subclasses rename `send()` → `_send()`. Each base
class (`ERDispatcher`, `ERDispatcherV2`) gains a concrete `send()`:

```python
async def send(self, data, **kwargs):
    try:
        return await self._send(data, **kwargs)
    except ERClientBadCredentials:
        # Cached/issued token rejected (e.g. revoked server-side).
        # AsyncERClient keeps token_url/username as instance attributes.
        invalidate_cached_token(self.er_client.token_url, self.er_client.username)
        # The `async with` in _send closed the httpx session; rebuild.
        self.er_client = self.make_er_client(...)
        return await self._send(data, **kwargs)  # one retry only
```

Rebuild inputs: v2 already stores `self.integration` and `self.provider`.
The v1 base class stores `self.configuration` but currently discards
`provider` after `__init__` — it must now keep it (`self.provider`) so the
client can be rebuilt.

- Only 401 (`ERClientBadCredentials`) triggers the retry. 403
  (`ERClientPermissionDenied`) is a genuine permissions problem and
  propagates immediately.
- A second 401 propagates to the existing failure path
  (`ObservationDeliveryFailed` event + raise → PubSub retry), unchanged.
- The rebuilt client starts with no auth; its `auth_headers()` performs a
  fresh (backoff-protected) login since the cache was just invalidated.

### 4. Expected impact

- Password grants drop from ~1 per observation to ~1 per 48h per integration
  (fleet-wide, shared via Redis; warm instances also reuse in-memory auth).
- Transient token-endpoint 500s are absorbed by in-process backoff instead of
  failing the delivery and emitting `ObservationDeliveryFailed`.
- ER-side `DASAccessToken.DoesNotExist` 500s should drop to near zero for
  Gundi traffic (the trigger — concurrent grants — is removed).

## Error handling summary

| Failure | Behavior |
|---|---|
| `/oauth2/token` 5xx or network error | Retry with backoff (≤3 attempts) inside `login()`; then raise |
| `/oauth2/token` 4xx (bad credentials) | Raise immediately (no retry) |
| Cached token rejected with 401 at delivery | Invalidate cache, rebuild client, retry `_send` once; second 401 raises |
| 403 at delivery | Raise immediately |
| Redis unavailable (read/write/delete) | Log warning, fall through to login; delivery unaffected |

## Testing

New `tests/test_er_auth.py` plus additions to `tests/test_dispatchers.py`,
following repo conventions (pytest-asyncio, pytest-mock, external I/O mocked
at the client level, fixtures in `tests/conftest.py`):

1. Cache hit → no token request issued; delivery uses cached token.
2. Cache miss → exactly one grant; token written with TTL matching
   `auth_expires`.
3. Grant returns 500 twice then 200 → succeeds (backoff retries).
4. Grant returns 500 persistently → raises after max attempts.
5. Grant returns 400/401 → raises immediately, no retries.
6. Cached token rejected with 401 → cache invalidated, client rebuilt, send
   retried once, succeeds.
7. Two consecutive 401s → exception propagates.
8. Static-token integration → zero cache interaction, no login.
9. Redis read/write errors → warning logged, login proceeds, delivery
   succeeds.
10. Cached entry expiring in < 60s → treated as a miss (fresh login).

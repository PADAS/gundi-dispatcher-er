# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

> NOTE: The parent directory's `CLAUDE.md` describes a different project (a generic Gundi v2 webhook integration). This repo is **not** that project — ignore the parent file's architecture notes when working here.

## Overview

**Gundi ER Dispatcher** — a serverless GCP Cloud Function (gen2, `functions-framework`) that consumes PubSub messages containing transformed observations from the Gundi routing pipeline and delivers them to [EarthRanger](https://earthranger.com) via `erclient`'s `AsyncERClient`.

Supported stream types: Positions, Events, Event Updates, Attachments, Observations, and Text Messages. Handles both Gundi **v1** (legacy) and **v2** message formats in a single deployment, routed at `core/services.py::process_request()`.

## Commands

Python 3.10 is pinned via `.python-version`.

### Install dependencies
```bash
pip install -r requirements.txt
```

### Run the full test suite
```bash
pytest
```

### Run a single test
```bash
pytest tests/test_process_observations_v2.py::<test_name> -v
```

### Run the function locally
```bash
functions-framework --signature-type=http --target=main
```
(Note: `README.md` says `--signature-type=cloudevent`, but `main.py` uses the `@http` decorator — use `http`.) Then trigger it with the helper script:
```bash
./test_local.sh
```
`test_local.sh` contains many commented-out sample payloads covering every supported stream type (v1 and v2) — uncomment the one you need when reproducing bugs.

### Recompile dependencies (after editing `requirements.in`)
```bash
pip-compile --output-file=requirements.txt requirements.in
```
Watch out: `requirements.in` pulls two wheels directly from GitHub releases (`earthranger_client`, `cdip_connector`).

### Deploy
```bash
./deploy_function.sh <FUNCTION_NAME> <PUBSUB_TOPIC_ID> <SERVICE_ACCOUNT>
```
Deploys a gen2 Cloud Function in `us-central1` triggered by `google.cloud.pubsub.topic.v1.messagePublished`. Reads env vars from `.env.dev.yaml` (see `.env.yaml.template`). One function is deployed per outbound destination (topic).

## Architecture

### Request flow

1. **Entry** (`main.py::main`): PubSub push subscription POSTs to the function's HTTP endpoint. The body is `{"message": {"data": <base64>, "attributes": {...}}}`.
2. **Dispatch by Gundi version** (`core/services.py::process_request`):
   - `attributes.gundi_version == "v2"` → `process_transformer_event_v2` (default path today)
   - else → `process_transformed_observation` (v1 legacy)
3. **Age gate**: `is_too_old()` compares `publish_time`/`time`/`ce-time` against `MAX_EVENT_AGE_SECONDS` (default 24h). Too old → DLQ and return.
4. **v2 event dispatch** (`core/event_handlers.py`): raw payload is parsed against one of the `EventTransformedER` / `EventUpdateTransformedER` / `AttachmentTransformedER` / `ObservationTransformedER` / `MessageTransformedER` schemas (registered in `event_schemas`/`event_handlers` dicts), then routed to `dispatch_transformed_observation_v2`.
5. **Dispatcher selection** (`core/dispatchers.py::dispatcher_cls_by_type`): maps `StreamPrefixEnum` → concrete `ERDispatcherV2` subclass (or `ERDispatcher` subclass for v1). Each builds an `AsyncERClient` from the destination integration's `AUTHENTICATE` action config.
6. **Delivery & side effects**:
   - Event updates require a prior `DispatchedObservation` cached in Redis (by `gundi_id`+`destination_id`). If missing, raise `ReferenceDataError` → PubSub retries.
   - Attachments require a `related_to` observation already delivered (same cache lookup).
   - On success: cache the new `DispatchedObservation` and publish `ObservationDelivered` / `ObservationUpdated` to `DISPATCHER_EVENTS_TOPIC`.
   - On failure: publish `ObservationDeliveryFailed` / `ObservationUpdateFailed` with traceback and ER response body, then raise → PubSub retries.

### Dead-letter routing (v2)

`get_dlq_topic_for_data_type()` splits DLQ per stream type: `OBSERVATIONS_DEAD_LETTER_TOPIC`, `EVENTS_DEAD_LETTER_TOPIC`, `EVENTS_UPDATES_DEAD_LETTER_TOPIC`, `ATTACHMENTS_DEAD_LETTER_TOPIC`, `TEXT_MESSAGES_DEAD_LETTER_TOPIC`. v1 falls back to `LEGACY_DEAD_LETTER_TOPIC` (env var `DEAD_LETTER_TOPIC`). DLQ is used for unrecoverable cases (unknown event type, unsupported schema version, too-old message). Transient errors raise back to PubSub for retry, **not** DLQ.

### Retry semantics

`DispatcherException` and `ReferenceDataError` are re-raised so GCP PubSub retries (function is deployed with `--retry`). The destination portal/EDA is informed via `publish_event()` to `DISPATCHER_EVENTS_TOPIC` for both success and failure paths — this is how the portal's delivery status UI stays up to date.

### Redis usage (`core/utils.py`)

- `_cache_db` wraps a `walrus.Database` (DB `REDIS_DB`, default `3`).
- Portal config lookups (outbound configs, inbound integrations, v2 Integration details) are cached for `PORTAL_CONFIG_OBJECT_CACHE_TTL` (default 60s).
- Dispatched v2 observations are cached for `DISPATCHED_OBSERVATIONS_CACHE_TTL` (default 1h) keyed by `gundi_id` + `destination_id` — required for event-update and attachment flows.
- Cache reads/writes are wrapped in `read_config_from_cache_safe`/`write_config_in_cache_safe` so Redis connection issues log a warning and fall through to the portal API rather than erroring.
- ER auth tokens from password grants are cached under `er_dispatcher.auth_token.{host}.{username}.{credential-fingerprint}` (fingerprint = `sha256(username:password)[:16]`, so a wrong password is always a cache miss) with TTL matching token expiry (~48h), so dispatch does not perform an OAuth2 grant per message. Entries are Fernet-encrypted under a key derived from the credentials plus the optional `ER_TOKEN_CACHE_SECRET` env var — Redis contents alone can't be read or forged; undecryptable entries are discarded as misses. Static-token integrations bypass this cache.

### Tracing

OpenTelemetry tracing is wired in `core/tracing/__init__.py`. When `TRACING_ENABLED=true`, `requests`/`aiohttp`/`httpx` are auto-instrumented, and the `X-Cloud-Trace-Context` propagator is installed so traces stitch together across the routing pipeline. Tracing context arrives on the PubSub message as `attributes.tracing_context` (JSON string) — `pubsub_instrumentation.load_context_from_attributes()` restores it at the top of `process_request`.

### Key modules

| Path | Purpose |
|------|---------|
| `main.py` | HTTP entry point (`@http` functions-framework decorator) |
| `core/services.py` | Top-level routing: v1 vs v2, age gate, DLQ routing |
| `core/event_handlers.py` | **v2 dispatch logic** — schema parsing, related-observation/update lookups, event publishing |
| `core/dispatchers.py` | `ERDispatcher*` classes per stream type; builds `AsyncERClient` from integration config |
| `core/er_auth.py` | `TokenCachingAsyncERClient` — Redis-cached ER auth tokens, backoff-retried password grants; 401 → invalidate + one retry (see `docs/superpowers/specs/2026-07-27-er-token-caching-design.md`) |
| `core/utils.py` | Portal API wrappers, Redis caching, `publish_event`, `extract_fields_from_message`, `find_config_for_action` |
| `core/settings.py` | All env-var-driven settings, DLQ topic names |
| `core/tracing/` | OpenTelemetry setup and PubSub trace-context propagation |
| `tests/conftest.py` | Large shared fixture set — all external services (Gundi portal, PubSub, Redis, ER API) are mocked |

## Testing

Tests use `pytest-asyncio` + `pytest-mock`. Structure mirrors the module under test (`test_dispatchers.py`, `test_process_observations.py` for v1, `test_process_observations_v2.py` for v2, `test_dead_lettering.py`). All external I/O is mocked at the client level — `AsyncERClient`, `PortalApi`, `GundiClient`, `walrus.Database`, and `gcloud.aio.pubsub.PublisherClient`. When adding a new stream type, register it in `dispatcher_cls_by_type`, `event_schemas`, and `event_handlers`, then add fixtures and cases in `tests/conftest.py` alongside the existing ones.

## Key env vars

| Variable | Purpose |
|----------|---------|
| `GCP_PROJECT_ID` | Used for all PubSub topic paths |
| `REDIS_HOST` / `REDIS_PORT` / `REDIS_DB` | Config + dispatched-observation cache |
| `CDIP_ADMIN_ENDPOINT` / `GUNDI_API_BASE_URL` | Portal endpoints (v1 and v2 respectively) |
| `KEYCLOAK_*` | Auth against the portal |
| `DISPATCHER_EVENTS_TOPIC` | Topic for `ObservationDelivered`/`*Failed`/`*Updated` system events (consumed by the portal/EDA) |
| `OBSERVATIONS_DEAD_LETTER_TOPIC` / `EVENTS_DEAD_LETTER_TOPIC` / `EVENTS_UPDATES_DEAD_LETTER_TOPIC` / `ATTACHMENTS_DEAD_LETTER_TOPIC` / `TEXT_MESSAGES_DEAD_LETTER_TOPIC` | v2 DLQ topics (split by stream type) |
| `DEAD_LETTER_TOPIC` | v1 legacy DLQ topic |
| `MAX_EVENT_AGE_SECONDS` | Drop-to-DLQ threshold (default 86400) |
| `TRACING_ENABLED` / `TRACE_ENVIRONMENT` | OpenTelemetry toggle + env label |
| `BUCKET_NAME` / `CLOUD_STORAGE_TYPE` | Used by attachment/camera-trap dispatchers to fetch files before POSTing to ER |

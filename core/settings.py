import logging.config
import sys

from cdip_connector.core import cdip_settings
from environs import Env

env = Env()
env.read_env()

LOGGING_LEVEL = env.str("LOGGING_LEVEL", "INFO")

DEFAULT_LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {
        "console": {
            "level": LOGGING_LEVEL,
            "class": "logging.StreamHandler",
            "stream": sys.stdout,
        },
    },
    "loggers": {
        "": {
            "handlers": ["console"],
            "level": LOGGING_LEVEL,
        },
    },
}
logging.config.dictConfig(DEFAULT_LOGGING)

DEFAULT_REQUESTS_TIMEOUT = (10, 20)  # Connect, Read

PORTAL_OUTBOUND_INTEGRATIONS_ENDPOINT = (
    f"{cdip_settings.PORTAL_API_ENDPOINT}/integrations/outbound/configurations"
)
PORTAL_INBOUND_INTEGRATIONS_ENDPOINT = (
    f"{cdip_settings.PORTAL_API_ENDPOINT}/integrations/inbound/configurations"
)

# Settings for caching admin portal request/responses
REDIS_HOST = env.str("REDIS_HOST", "localhost")
REDIS_PORT = env.int("REDIS_PORT", 6379)
REDIS_DB = env.int("REDIS_DB", 3)

# Optional secret mixed into the ER auth-token cache encryption key.
# When set (use the same value across all dispatcher deployments sharing the
# Redis), cached tokens cannot be decrypted or brute-forced from Redis
# contents alone. When empty, the key is derived from the integration
# credentials only.
ER_TOKEN_CACHE_SECRET = env.str("ER_TOKEN_CACHE_SECRET", "")

# N-seconds to cache portal responses for configuration objects.
PORTAL_CONFIG_OBJECT_CACHE_TTL = env.int("PORTAL_CONFIG_OBJECT_CACHE_TTL", 60)
DISPATCHED_OBSERVATIONS_CACHE_TTL = env.int("DISPATCHED_OBSERVATIONS_CACHE_TTL", 60 * 60)  # 1 Hour
# Idempotency cache for batch-delivered observations. Must exceed the PubSub
# retry window (24h) so envelope redeliveries keep skipping delivered items.
DISPATCHED_OBSERVATIONS_BATCH_CACHE_TTL = env.int("DISPATCHED_OBSERVATIONS_BATCH_CACHE_TTL", 90000)

# Used in OTel traces/spans to set the 'environment' attribute, used on metrics calculation
TRACE_ENVIRONMENT = env.str("TRACE_ENVIRONMENT", "dev")
TRACING_ENABLED = env.bool("TRACING_ENABLED", True)

# Retries and dead-letter settings
# ToDo: Get retry settings from the outbound config?
GCP_PROJECT_ID = env.str("GCP_PROJECT_ID", "cdip-78ca")
LEGACY_DEAD_LETTER_TOPIC = env.str("DEAD_LETTER_TOPIC", "dispatchers-dead-letter-prod")
OBSERVATIONS_DEAD_LETTER_TOPIC = env.str("OBSERVATIONS_DEAD_LETTER_TOPIC", "observations-dead-letter")
EVENTS_DEAD_LETTER_TOPIC = env.str("EVENTS_DEAD_LETTER_TOPIC", "events-dead-letter")
EVENTS_UPDATES_DEAD_LETTER_TOPIC = env.str("EVENTS_UPDATES_DEAD_LETTER_TOPIC", "events-updates-dead-letter")
ATTACHMENTS_DEAD_LETTER_TOPIC = env.str("ATTACHMENTS_DEAD_LETTER_TOPIC", "attachments-dead-letter")
TEXT_MESSAGES_DEAD_LETTER_TOPIC = env.str("TEXT_MESSAGES_DEAD_LETTER_TOPIC", "text-messages-dead-letter")
DISPATCHER_EVENTS_TOPIC = env.str("DISPATCHER_EVENTS_TOPIC", "dispatcher-events-dev")
MAX_EVENT_AGE_SECONDS = env.int("MAX_EVENT_AGE_SECONDS", 86400)  # 24hrs
# Hard bound for publishing the retries-exhausted notification after a DLQ
# send. publish_event retries with backoff (worst case ~65s), which could
# outlive the function timeout and un-ack an already-dead-lettered message.
RETRIES_EXHAUSTED_PUBLISH_TIMEOUT_SECONDS = env.int("RETRIES_EXHAUSTED_PUBLISH_TIMEOUT_SECONDS", 10)

# Per-destination burst throttling (see docs/superpowers/specs/2026-07-06-er-dispatcher-burst-throttling-design.md)
THROTTLING_ENABLED = env.bool("THROTTLING_ENABLED", False)
DEFAULT_MAX_EVENT_DELIVERIES_PER_MINUTE = env.int("DEFAULT_MAX_EVENT_DELIVERIES_PER_MINUTE", 120)
DEFAULT_MAX_OBSERVATION_DELIVERIES_PER_MINUTE = env.int("DEFAULT_MAX_OBSERVATION_DELIVERIES_PER_MINUTE", 300)
DEFAULT_MAX_MESSAGE_DELIVERIES_PER_MINUTE = env.int("DEFAULT_MAX_MESSAGE_DELIVERIES_PER_MINUTE", 60)
THROTTLE_GRACE_WAIT_MAX_SECONDS = env.int("THROTTLE_GRACE_WAIT_MAX_SECONDS", 2)
THROTTLE_COOLDOWN_BASE_SECONDS = env.int("THROTTLE_COOLDOWN_BASE_SECONDS", 30)
THROTTLE_COOLDOWN_MAX_SECONDS = env.int("THROTTLE_COOLDOWN_MAX_SECONDS", 600)
THROTTLE_COOLDOWN_LEVEL_TTL_SECONDS = env.int("THROTTLE_COOLDOWN_LEVEL_TTL_SECONDS", 900)
THROTTLE_NOTIFY_TTL_SECONDS = env.int("THROTTLE_NOTIFY_TTL_SECONDS", 300)

# Batch delivery (see cdip repo: docs/superpowers/specs/2026-07-29-pipeline-batch-envelope-design.md)
# Max observations per single ER bulk request. Independent from the envelope
# size chosen upstream; an envelope larger than this is posted in sub-chunks.
# max(1, ...): a zero/negative misconfiguration would make the chunking step
# (range with step=ER_BULK_SIZE) raise at runtime.
ER_BULK_SIZE = max(1, env.int("ER_BULK_SIZE", 200))

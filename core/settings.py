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

# N-seconds to cache portal responses for configuration objects.
PORTAL_CONFIG_OBJECT_CACHE_TTL = env.int("PORTAL_CONFIG_OBJECT_CACHE_TTL", 60)
DISPATCHED_OBSERVATIONS_CACHE_TTL = env.int("PORTAL_CONFIG_OBJECT_CACHE_TTL", 60 * 60)  # 1 Hour

# Used in OTel traces/spans to set the 'environment' attribute, used on metrics calculation
TRACE_ENVIRONMENT = env.str("TRACE_ENVIRONMENT", "dev")
TRACING_ENABLED = env.bool("TRACING_ENABLED", True)

# Retries and dead-letter settings
# ToDo: Get retry settings from the outbound config?
GCP_PROJECT_ID = env.str("GCP_PROJECT_ID", "cdip-78ca")
DEAD_LETTER_TOPIC = env.str("DEAD_LETTER_TOPIC", "destinations-dead-letter-dev")
DISPATCHER_EVENTS_TOPIC = env.str("DISPATCHER_EVENTS_TOPIC", "dispatcher-events-dev")
MAX_EVENT_AGE_SECONDS = env.int("MAX_EVENT_AGE_SECONDS", 86400)  # 24hrs

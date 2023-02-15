from cdip_connector.core import cdip_settings
from environs import Env

env = Env()
env.read_env()

LOGGING_LEVEL = env.str("LOGGING_LEVEL", "INFO")

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

# Used in OTel traces/spans to set the 'environment' attribute, used on metrics calculation
TRACE_ENVIRONMENT = env.str("TRACE_ENVIRONMENT", "dev")

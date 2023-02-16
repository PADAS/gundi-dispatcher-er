# ToDo: Move base classes or utils into the SDK
import base64
import json
import aiohttp
import logging
import walrus
from uuid import UUID
from enum import Enum
from cdip_connector.core import schemas
from . import settings
from .errors import ReferenceDataError
from gundi_client import PortalApi


logger = logging.getLogger(__name__)


def get_redis_db():
    logger.debug(
        f"Connecting to REDIS DB :{settings.REDIS_DB} at {settings.REDIS_HOST}:{settings.REDIS_PORT}"
    )
    return walrus.Database(
        host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=settings.REDIS_DB
    )
_cache_ttl = settings.PORTAL_CONFIG_OBJECT_CACHE_TTL
_cache_db = get_redis_db()


portal = PortalApi()


async def get_outbound_config_detail(
    outbound_id: UUID,
) -> schemas.OutboundConfiguration:
    if not outbound_id:
        raise ValueError("integration_id must not be None")

    extra_dict = {
        ExtraKeys.AttentionNeeded: True,
        ExtraKeys.OutboundIntId: str(outbound_id),
    }

    cache_key = f"outbound_detail.{outbound_id}"
    cached = _cache_db.get(cache_key)

    if cached:
        config = schemas.OutboundConfiguration.parse_raw(cached)
        logger.debug(
            "Using cached outbound integration detail",
            extra={
                **extra_dict,
                ExtraKeys.AttentionNeeded: False,
                "outbound_detail": config,
            },
        )
        return config

    logger.debug(f"Cache miss for outbound integration detail", extra={**extra_dict})

    connect_timeout, read_timeout = settings.DEFAULT_REQUESTS_TIMEOUT
    timeout_settings = aiohttp.ClientTimeout(
        sock_connect=connect_timeout, sock_read=read_timeout
    )
    async with aiohttp.ClientSession(
        timeout=timeout_settings, raise_for_status=True
    ) as s:
        try:
            response = await portal.get_outbound_integration(
                session=s, integration_id=str(outbound_id)
            )
        except aiohttp.ServerTimeoutError as e:
            target_url = (
                f"{settings.PORTAL_OUTBOUND_INTEGRATIONS_ENDPOINT}/{str(outbound_id)}"
            )
            logger.error(
                "Read Timeout",
                extra={**extra_dict, ExtraKeys.Url: target_url},
            )
            raise ReferenceDataError(f"Read Timeout for {target_url}")
        except aiohttp.ClientResponseError as e:
            # ToDo: Try to get the url from the exception or from somewhere else
            target_url = str(e.request_info.url)
            logger.exception(
                "Portal returned bad response during request for outbound config detail",
                extra={
                    ExtraKeys.AttentionNeeded: True,
                    ExtraKeys.OutboundIntId: outbound_id,
                    ExtraKeys.Url: target_url,
                    ExtraKeys.StatusCode: response.status_code,
                },
            )
            raise ReferenceDataError(
                f"Request for OutboundIntegration({outbound_id}) returned bad response"
            )
        else:
            try:
                config = schemas.OutboundConfiguration.parse_obj(response)
            except Exception:
                logger.error(
                    f"Failed decoding response for Outbound Integration Detail",
                    extra={**extra_dict, "resp_text": response},
                )
                raise ReferenceDataError(
                    "Failed decoding response for Outbound Integration Detail"
                )
            else:
                if config:  # don't cache empty response
                    _cache_db.setex(cache_key, _cache_ttl, config.json())
                return config


async def get_inbound_integration_detail(
    integration_id: UUID,
) -> schemas.IntegrationInformation:
    if not integration_id:
        raise ValueError("integration_id must not be None")

    extra_dict = {
        ExtraKeys.AttentionNeeded: True,
        ExtraKeys.InboundIntId: str(integration_id),
    }

    cache_key = f"inbound_detail.{integration_id}"
    cached = _cache_db.get(cache_key)

    if cached:
        config = schemas.IntegrationInformation.parse_raw(cached)
        logger.debug(
            "Using cached inbound integration detail",
            extra={**extra_dict, "integration_detail": config},
        )
        return config

    logger.debug(f"Cache miss for inbound integration detai", extra={**extra_dict})

    connect_timeout, read_timeout = settings.DEFAULT_REQUESTS_TIMEOUT
    timeout_settings = aiohttp.ClientTimeout(
        sock_connect=connect_timeout, sock_read=read_timeout
    )
    async with aiohttp.ClientSession(
        timeout=timeout_settings, raise_for_status=True
    ) as s:
        try:
            response = await portal.get_inbound_integration(
                session=s, integration_id=str(integration_id)
            )
        except aiohttp.ServerTimeoutError as e:
            # ToDo: Try to get the url from the exception or from somewhere else
            target_url = (
                f"{settings.PORTAL_INBOUND_INTEGRATIONS_ENDPOINT}/{str(integration_id)}"
            )
            logger.error(
                "Read Timeout",
                extra={**extra_dict, ExtraKeys.Url: target_url},
            )
            raise ReferenceDataError(f"Read Timeout for {target_url}")
        except aiohttp.ClientResponseError as e:
            target_url = str(e.request_info.url)
            logger.exception(
                "Portal returned bad response during request for inbound config detail",
                extra={
                    ExtraKeys.AttentionNeeded: True,
                    ExtraKeys.InboundIntId: integration_id,
                    ExtraKeys.Url: target_url,
                    ExtraKeys.StatusCode: e.status,
                },
            )
            raise ReferenceDataError(
                f"Request for InboundIntegration({integration_id})"
            )
        else:
            try:
                config = schemas.IntegrationInformation.parse_obj(response)
            except Exception:
                logger.error(
                    f"Failed decoding response for InboundIntegration Detail",
                    extra={**extra_dict, "resp_text": response},
                )
                raise ReferenceDataError(
                    "Failed decoding response for InboundIntegration Detail"
                )
            else:
                if config:  # don't cache empty response
                    _cache_db.setex(cache_key, _cache_ttl, config.json())
                return config


def extract_fields_from_message(message):
    if message:
        data = base64.b64decode(message.get("data", "").encode('utf-8'))
        observation = json.loads(data)
        attributes = message.get("attributes")
        if not observation:
            logger.warning(f"No observation was obtained from {message}")
        if not attributes:
            logger.debug(f"No attributes were obtained from {message}")
    else:
        logger.warning(f"message contained no payload", extra={"message": message})
        return None, None
    return observation, attributes



class ExtraKeys(str, Enum):
    def __str__(self):
        return str(self.value)

    DeviceId = "device_id"
    InboundIntId = "inbound_integration_id"
    OutboundIntId = "outbound_integration_id"
    AttentionNeeded = "attention_needed"
    StreamType = "stream_type"
    Provider = "provider"
    Error = "error"
    Url = "url"
    Observation = "observation"
    RetryTopic = "retry_topic"
    RetryAt = "retry_at"
    RetryAttempt = "retry_attempt"
    StatusCode = "status_code"
    DeadLetter = "dead_letter"

# ToDo: Move base classes or utils into the SDK
import json
import logging
from abc import ABC, abstractmethod
from erclient import AsyncERClient
from typing import Union, List
from urllib.parse import urlparse
from gundi_core import schemas
from cdip_connector.core.cloudstorage import get_cloud_storage

from core.utils import find_config_for_action

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = (3.1, 20)


class Dispatcher(ABC):
    stream_type: schemas.StreamPrefixEnum
    destination_type: schemas.DestinationTypes

    def __init__(self, config: schemas.OutboundConfiguration):
        self.configuration = config

    @abstractmethod
    async def send(self, messages: list, **kwargs):
        ...


class ERDispatcher(Dispatcher, ABC):
    DEFAULT_CONNECT_TIMEOUT_SECONDS = 10.0

    def __init__(self, config: schemas.OutboundConfiguration, provider: str):
        super().__init__(config)
        self.er_client = self.make_er_client(config, provider)
        # self.load_batch_size = 1000

    @staticmethod
    def make_er_client(
        config: schemas.OutboundConfiguration, provider: str
    ) -> AsyncERClient:
        provider_key = provider
        url_parse = urlparse(config.endpoint, "https")
        netloc = url_parse.netloc or url_parse.path

        # Check for https
        scheme = url_parse.scheme
        if scheme == "http":
            scheme = "https"

        return AsyncERClient(
            service_root=f"{scheme}://{netloc}/api/v1.0",
            username=config.login,
            password=config.password,
            token=config.token,
            token_url=f"{url_parse.scheme}://{url_parse.hostname}/oauth2/token",
            client_id="das_web_client",
            provider_key=provider_key,
            connect_timeout=ERDispatcher.DEFAULT_CONNECT_TIMEOUT_SECONDS,
        )

    @staticmethod
    def generate_batches(data, batch_size=1000):
        num_obs = len(data)
        for start_index in range(0, num_obs, batch_size):
            yield data[start_index : min(start_index + batch_size, num_obs)]


class ERPositionDispatcher(ERDispatcher):
    def __init__(self, config, provider):
        super(ERPositionDispatcher, self).__init__(config, provider)

    async def send(self, position: dict, **kwargs):
        result = None
        try:
            result = await self.er_client.post_sensor_observation(position)
        except Exception as ex:
            logger.exception(f"exception raised sending to dest {ex}")
            raise ex
        finally:
            await self.er_client.close()
        return result


class ERGeoEventDispatcher(ERDispatcher):
    def __init__(self, config, provider):
        super(ERGeoEventDispatcher, self).__init__(config, provider)

    async def send(self, messages: Union[list, dict], **kwargs):
        results = []
        if isinstance(messages, dict):
            messages = [messages]

        async with self.er_client as client:
            for m in messages:
                try:
                    results.append(await client.post_report(m))
                except Exception as ex:
                    logger.exception(f"exception raised sending to dest {ex}")
                    raise ex
        return results


class ERCameraTrapDispatcher(ERDispatcher):
    def __init__(self, config, provider):
        super(ERCameraTrapDispatcher, self).__init__(config, provider)
        self.cloud_storage = get_cloud_storage()

    async def send(self, camera_trap_payload: dict, **kwargs):
        result = None
        try:
            file_name = camera_trap_payload.get("file")
            # ToDo use async libs for cloudstorage and file handling
            file = self.cloud_storage.download(file_name)
            result = await self.er_client.post_camera_trap_report(
                camera_trap_payload, file
            )
        except Exception as ex:
            logger.exception(f"exception raised sending to dest {ex}")
            raise ex
        else:
            self.cloud_storage.remove(file)
        finally:
            await self.er_client.close()
        return result


class DispatcherV2(ABC):
    stream_type: schemas.v2.StreamPrefixEnum
    destination_type: schemas.DestinationTypes

    def __init__(
            self,
            integration: schemas.v2.Integration,
            **kwargs
    ):
        self.integration = integration

    @abstractmethod
    async def send(self, data, **kwargs):
        ...


class ERDispatcherV2(DispatcherV2, ABC):
    DEFAULT_CONNECT_TIMEOUT_SECONDS = 10.0

    def __init__(
            self,
            integration: schemas.v2.Integration,
            **kwargs
    ):
        super().__init__(integration, **kwargs)
        # provider_key in EarthRanger
        self.provider = kwargs.pop("provider")
        self.er_client = self.make_er_client(
            integration=self.integration,
            provider=self.provider
        )

    @staticmethod
    def make_er_client(
        integration: schemas.v2.Integration,
        provider: str
    ) -> AsyncERClient:
        provider_key = provider
        url_parse = urlparse(integration.base_url, "https")
        netloc = url_parse.netloc or url_parse.path

        # Check for https
        scheme = url_parse.scheme
        if scheme == "http":
            scheme = "https"

        # Look for the configuration of the authentication action
        configurations = integration.configurations
        integration_action_config = find_config_for_action(
            configurations=configurations,
            action_value=schemas.v2.EarthRangerActions.AUTHENTICATE.value
        )
        if not integration_action_config:
            raise ValueError(
                f"Authentication settings for integration {str(integration.id)} are missing. Please fix the integration setup in the portal."
            )
        auth_config = schemas.v2.ERAuthActionConfig.parse_obj(integration_action_config.data)
        return AsyncERClient(
            service_root=f"{scheme}://{netloc}/api/v1.0",
            username=auth_config.username,
            password=auth_config.password,
            token=auth_config.token,
            token_url=f"{url_parse.scheme}://{url_parse.hostname}/oauth2/token",
            client_id="das_web_client",
            provider_key=provider_key,
            connect_timeout=ERDispatcher.DEFAULT_CONNECT_TIMEOUT_SECONDS,
        )

    @staticmethod
    def generate_batches(data, batch_size=1000):
        num_obs = len(data)
        for start_index in range(0, num_obs, batch_size):
            yield data[start_index : min(start_index + batch_size, num_obs)]


class EREventDispatcher(ERDispatcherV2):

    async def send(self, event: schemas.v2.EREvent, **kwargs):
        async with self.er_client as client:
            try:
                event_cleaned = json.loads(event.json(exclude_none=True, exclude_unset=True))
                return await client.post_report(
                    data=event_cleaned
                )
            except Exception as ex:
                logger.exception(f"Exception raised sending to dest {ex}")
                raise ex


class EREventUpdateDispatcher(ERDispatcherV2):

    async def send(self, event_update: schemas.v2.EREventUpdate, **kwargs):
        async with self.er_client as client:
            try:
                return await client.patch_event(
                    data=event_update.changes
                )
            except Exception as ex:
                logger.exception(f"Error patching event: {ex}")
                raise ex


class EREventAttachmentDispatcher(ERDispatcherV2):
    def __init__(
            self,
            integration: schemas.v2.Integration,
            **kwargs
    ):
        super().__init__(integration=integration, **kwargs)
        self.cloud_storage = get_cloud_storage()

    async def send(self, attachment_payload: schemas.v2.ERAttachment, **kwargs):
        result = None
        related_observation = kwargs.get("related_observation")
        if not related_observation:
            raise ValueError("related_observation is required")
        try:
            external_event_id = related_observation.external_id
            file_path = attachment_payload.file_path
            file = self.cloud_storage.download(file_path)
            result = await self.er_client.post_report_attachment(
                report_id=external_event_id, file=file
            )
        except Exception as ex:
            logger.exception(f"exception raised sending to dest {ex}")
            raise ex
        else:
            self.cloud_storage.remove(file)
        finally:
            await self.er_client.close()
        return result


class ERObservationDispatcher(ERDispatcherV2):

    async def send(self, observation: schemas.v2.ERObservation, **kwargs):
        async with self.er_client as client:
            try:
                observation_cleaned = json.loads(observation.json(exclude_none=True, exclude_unset=True))
                return await client.post_sensor_observation(observation_cleaned)
            except Exception as ex:
                logger.exception(f"exception raised sending to dest {ex}")
                raise ex


dispatcher_cls_by_type = {
    # Gundi v1
    schemas.StreamPrefixEnum.position: ERPositionDispatcher,
    schemas.StreamPrefixEnum.geoevent: ERGeoEventDispatcher,
    schemas.StreamPrefixEnum.camera_trap: ERCameraTrapDispatcher,
    # Gundi v2
    schemas.v2.StreamPrefixEnum.event: EREventDispatcher,
    schemas.v2.StreamPrefixEnum.event_update: EREventUpdateDispatcher,
    schemas.v2.StreamPrefixEnum.attachment: EREventAttachmentDispatcher,
    schemas.v2.StreamPrefixEnum.observation: ERObservationDispatcher
}

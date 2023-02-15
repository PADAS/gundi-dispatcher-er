# ToDo: Move base classes or utils into the SDK
import logging
from abc import ABC, abstractmethod
from urllib.parse import urlparse
from cdip_connector.core import schemas
from erclient import AsyncERClient


logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = (3.1, 20)


class Dispatcher(ABC):
    stream_type: schemas.StreamPrefixEnum
    destination_type: schemas.DestinationTypes

    def __init__(self, config: schemas.OutboundConfiguration):
        self.configuration = config

    @abstractmethod
    async def send(self, messages: list):
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
        url_parse = urlparse(config.endpoint)

        return AsyncERClient(
            service_root=config.endpoint,
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

    async def send(self, position: dict):
        result = None
        try:
            result = await self.er_client.post_sensor_observation(position)
        except Exception as ex:
            logger.exception(f"exception raised sending to dest {ex}")
            raise ex
        finally:
            await self.er_client.close()
        return result

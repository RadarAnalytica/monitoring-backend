from contextlib import asynccontextmanager, contextmanager
import clickhouse_connect
from clickhouse_connect.driver.asyncclient import AsyncClient
from copy import deepcopy

from settings import CLICKHOUSE_CONFIG


class AsyncSession:

    def __init__(self, clickhouse_config, **kwargs):
        self.config = deepcopy(clickhouse_config)
        if kwargs:
            self.config.update(kwargs)


    async def __aenter__(self) -> AsyncClient:
        self.client = await clickhouse_connect.get_async_client(**self.config)
        return self.client

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.close()


@asynccontextmanager
async def get_async_connection(**kwargs):
    session = AsyncSession(CLICKHOUSE_CONFIG, **kwargs)
    async with session as client:
        yield client


class SyncSession:

    def __init__(self, clickhouse_config):
        self.config = clickhouse_config

    def __enter__(self) -> AsyncClient:
        self.client = clickhouse_connect.get_client(**self.config)
        return self.client

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()


@contextmanager
def get_sync_connection():
    session = SyncSession(CLICKHOUSE_CONFIG)
    with session as client:
        yield client

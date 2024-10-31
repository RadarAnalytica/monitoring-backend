from contextlib import asynccontextmanager
import clickhouse_connect
from clickhouse_connect.driver.asyncclient import AsyncClient

from settings import CLICKHOUSE_CONFING

@asynccontextmanager
async def get_async_connection() -> AsyncClient:
    session = AsyncSession(CLICKHOUSE_CONFING)
    async with session as client:
        yield client


class AsyncSession:

    def __init__(self, clickhouse_config):
        self.config = clickhouse_config

    async def __aenter__(self) -> AsyncClient:
        self.client = await clickhouse_connect.get_async_client(**self.config)
        return self.client

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.client.close()

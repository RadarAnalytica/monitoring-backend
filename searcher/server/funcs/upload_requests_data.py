from asyncio import TaskGroup
from datetime import datetime

from clickhouse_db.get_async_connection import get_async_connection
from settings import logger



async def upload_requests_worker(requests_slice: list[list[int, str, int, datetime]], client):
    await client.insert('request', requests_slice, column_names=["id", "query", "quantity", "updated"])
    logger.info("Start of part DB renewal")


async def upload_requests_csv_bg(requests_data: list[list[int, str, int, datetime]]):
    logger.info("Uploading requests data")
    async with get_async_connection() as client:
        async with TaskGroup() as tg:
            tg.create_task(upload_requests_worker(requests_data[:250000], client))
            tg.create_task(upload_requests_worker(requests_data[250000:500000], client))
            tg.create_task(upload_requests_worker(requests_data[500000:750000], client))
            tg.create_task(upload_requests_worker(requests_data[750000:], client))
    logger.warning("DB renewal complete")

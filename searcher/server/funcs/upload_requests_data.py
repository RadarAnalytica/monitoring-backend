from asyncio import TaskGroup
from datetime import datetime

from clickhouse_db.get_async_connection import get_async_connection
from settings import logger, TIMEZONE



async def upload_requests_worker(requests_slice):
    async with get_async_connection() as client:
        await client.insert('request', requests_slice, column_names=["query", "quantity", "updated"])
        logger.info("Start of part DB renewal")


async def upload_requests_csv_bg(requests_data: list[list[str, int, datetime]]):
    logger.info("Uploading requests data")
    async with TaskGroup() as tg:
        tg.create_task(upload_requests_worker(requests_data[:250000]))
        tg.create_task(upload_requests_worker(requests_data[250000:500000]))
        tg.create_task(upload_requests_worker(requests_data[500000:750000]))
        tg.create_task(upload_requests_worker(requests_data[750000:]))
    logger.warning("DB renewal complete")

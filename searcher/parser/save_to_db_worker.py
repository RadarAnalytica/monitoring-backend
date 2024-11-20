import asyncio
import gc

from clickhouse_connect.driver import AsyncClient, Client

# from clickhouse_db.get_async_connection import get_async_connection
# from clickhouse_db.get_async_connection import get_sync_connection
from settings import logger


async def save_to_db(queue: asyncio.Queue, table, fields, client: AsyncClient):
    while True:
        items = []
        item = []
        while len(items) < 10000:
            item = await queue.get()
            if item is None:
                break
            items.extend(item)
        if items:
            try:
                await client.insert(table, items, column_names=fields)
                logger.info(f"Запись в БД + city: {items[0][1]}  date: {items[0][2]}")
                gc.collect()
            except Exception as e:
                logger.critical(f"{e}, {items}")
        if item is None:
            await queue.put(item)
            return
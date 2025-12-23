import asyncio
import gc

from clickhouse_connect.driver import AsyncClient
from settings import logger


async def save_to_db(
    queue: asyncio.Queue, table, fields, client: AsyncClient, batch_no=None
):
    counter = 0
    while True:
        items = []
        item = []
        while len(items) < 50000:
            item = await queue.get()
            if item is None:
                break
            items.extend(item)
        if items:
            try:
                await client.insert(table, items, column_names=fields)
                counter += 1
                if counter == 10:
                    counter = 0
                    logger.info(
                        f"Запись в БД: {items[0][1]} {items[0][2]}, batch: {batch_no}"
                    )
                gc.collect()
            except Exception as e:
                logger.critical(f"{e}, {items}")
        if item is None:
            await queue.put(item)
            return

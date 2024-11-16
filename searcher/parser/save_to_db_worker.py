from clickhouse_db.get_async_connection import get_async_connection
from settings import logger


async def save_to_db(queue, table, fields):
    while True:
        items = []
        item = None
        while len(items) < 6000:
            item = await queue.get()
            if item is None:
                break
            else:
                items.extend(item)
            queue.task_done()
        if items:
            try:
                async with get_async_connection() as client:
                    await client.insert(table, items, column_names=fields)
                    logger.info(f"Запись в БД +{items[0][1]}")
            except Exception as e:
                logger.critical(f"{e}, {items}")
        if item is None:
            await queue.put(item)
            break

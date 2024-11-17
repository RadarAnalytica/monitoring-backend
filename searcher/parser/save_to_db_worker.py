from clickhouse_connect.driver import AsyncClient

from clickhouse_db.get_async_connection import get_async_connection
from settings import logger


async def save_to_db(items, table, fields):
    if items:
        try:
            async with get_async_connection() as client:
                client: AsyncClient = client
                result = await client.insert(table, items, column_names=fields)
                del result
                logger.info(f"Запись в БД + city: {items[0][1]}  date: {items[0][2]}")
        except Exception as e:
            logger.critical(f"{e}, {items}")
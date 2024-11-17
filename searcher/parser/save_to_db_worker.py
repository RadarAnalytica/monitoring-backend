from clickhouse_connect.driver import AsyncClient, Client

# from clickhouse_db.get_async_connection import get_async_connection
# from clickhouse_db.get_async_connection import get_sync_connection
from settings import logger


async def save_to_db(items, table, fields, client: Client):
    if items:
        try:
            client.insert(table, items, column_names=fields)
            logger.info(f"Запись в БД + city: {items[0][1]}  date: {items[0][2]}")
        except Exception as e:
            logger.critical(f"{e}, {items}")
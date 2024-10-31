import asyncio

import clickhouse_connect
from settings import CLICKHOUSE_CONFING, logger


async def setup_database():
    client = clickhouse_connect.get_client(**CLICKHOUSE_CONFING)

    # Создание таблицы City
    client.command('DROP TABLE IF EXISTS city')

    # Создание таблицы City
    client.command('DROP TABLE IF EXISTS request')

    # Создание таблицы City
    client.command('DROP TABLE IF EXISTS request_product')

    logger.info("Tables deleted successfully.")
    tables = client.query("SHOW TABLES")
    logger.info(tables.result_rows)
    client.close()

asyncio.run(setup_database())
import asyncio

import clickhouse_connect
from settings import CLICKHOUSE_CONFING, logger


async def setup_database():
    client = clickhouse_connect.get_client(**CLICKHOUSE_CONFING)

    client.command('DROP TABLE IF EXISTS city')

    client.command('DROP TABLE IF EXISTS request')
    client.command('DROP TABLE IF EXISTS product')

    client.command('DROP TABLE IF EXISTS request_product')

    logger.info("Tables deleted successfully.")
    tables = client.query("SHOW TABLES")
    logger.info(tables.result_rows)
    client.close()

asyncio.run(setup_database())
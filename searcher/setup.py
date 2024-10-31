import asyncio

import clickhouse_connect
from settings import CLICKHOUSE_CONFING, logger


async def setup_database():
    client = clickhouse_connect.get_client(**CLICKHOUSE_CONFING)

    # Создание таблицы City
    client.command('''
        CREATE TABLE IF NOT EXISTS city (
            name String,
            dest Int64,
            updated DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree(updated)
        ORDER BY dest;
    ''')

    # Создание таблицы Request
    client.command('''
        CREATE TABLE IF NOT EXISTS request (
            query String,
            quantity UInt32,
            updated DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree(updated)
        ORDER BY query;
    ''')

    # Создание таблицы RequestProducts
    client.command('''
        CREATE TABLE IF NOT EXISTS request_product (
            city Int64 CODEC(LZ4),
            query String CODEC(LZ4),
            date Date CODEC(LZ4),
            products Array(UInt32) CODEC(LZ4),
            INDEX idx_bloom (products) TYPE bloom_filter(0.01) GRANULARITY 64
        ) ENGINE = MergeTree()
        PARTITION BY city
        ORDER BY date DESC
        SETTINGS index_granularity = 8192;''')

    logger.info("Tables created successfully.")
    tables = client.query("SHOW TABLES")
    logger.info(tables.result_rows)
    client.close()

asyncio.run(setup_database())
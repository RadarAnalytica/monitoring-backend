import asyncio

import clickhouse_connect
from settings import CLICKHOUSE_CONFING, logger


async def setup_database():
    client = clickhouse_connect.get_client(**CLICKHOUSE_CONFING)
    logger.info("Setup start")
    client.command('''
        CREATE TABLE IF NOT EXISTS city (
            name String,
            dest Int64,
            updated DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree(updated)
        ORDER BY dest;
    ''')

    client.command('''
        CREATE TABLE IF NOT EXISTS request (
            query String,
            quantity UInt32,
            updated DateTime DEFAULT now() CODEC(DoubleDelta)
        ) ENGINE = ReplacingMergeTree(updated)
        ORDER BY query;
    ''')

    client.command('''
        CREATE TABLE IF NOT EXISTS product (
            name String CODEC(LZ4),
            id UInt32 CODEC(LZ4),
            updated DateTime DEFAULT now() CODEC(DoubleDelta)
        ) ENGINE = ReplacingMergeTree(updated)
        ORDER BY name;
        ''')

    client.command('''CREATE TABLE IF NOT EXISTS request_product (
            city Int64 CODEC(LZ4HC),
            date Date CODEC(LZ4HC),
            query String CODEC(LZ4HC),
            product UInt32 CODEC(LZ4HC),
            place UInt16 Codec(LZ4HC)
        ) ENGINE = MergeTree()
        PARTITION BY city
        ORDER BY (date, query);''')

    client.command('''DROP TABLE IF EXISTS request_product;''')
    client.command('''RENAME TABLE request_product_2 TO request_product;''')
    logger.info("Tables created successfully.")
    tables = client.query("SHOW TABLES")
    logger.info(tables.result_rows)
    client.close()

asyncio.run(setup_database())
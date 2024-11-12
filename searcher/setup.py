import asyncio

import clickhouse_connect
from settings import CLICKHOUSE_CONFING, logger


async def setup_database():
    client = clickhouse_connect.get_client(**CLICKHOUSE_CONFING)
    logger.info("Setup start")
    client.command('''
        CREATE TABLE IF NOT EXISTS city (
            id UInt8,
            name String,
            dest Int64,
            updated DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree(updated)
        ORDER BY dest;
    ''')

    client.command('''
            CREATE TABLE IF NOT EXISTS dates (
                id UInt16,
                date Date,
            ) ENGINE = MergeTree()
            ORDER BY date;
        ''')

    client.command('''
        CREATE TABLE IF NOT EXISTS request (
            id UInt32,
            query String,
            quantity UInt32,
            updated DateTime DEFAULT now()
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
                        product UInt32 CODEC(LZ4HC),
                        city UInt8 CODEC(LZ4HC),
                        date UInt16 CODEC(LZ4HC),
                        query UInt32 CODEC(LZ4HC),
                        place UInt16 Codec(LZ4HC),
                        advert FixedString(1) Codec(LZ4HC),
                        natural_place Int16 Codec (LZ4HC)
                    ) ENGINE = MergeTree()
                    PRIMARY KEY (product, city, date) 
                    ORDER BY (product, city, date, query);''')
    logger.info("Tables created successfully.")
    tables = client.query("SHOW TABLES")
    logger.info(tables.result_rows)
    client.close()

asyncio.run(setup_database())

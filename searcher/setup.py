import asyncio

import clickhouse_connect
from settings import CLICKHOUSE_CONFING, logger


async def setup_database():
    client = clickhouse_connect.get_client(**CLICKHOUSE_CONFING)

    # Создание таблицы City
    client.command(
        """
        CREATE TABLE IF NOT EXISTS city (
            name String,
            dest Int64,
            updated DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree(updated)
        ORDER BY dest;
    """
    )

    # Создание таблицы Request
    client.command(
        """
        CREATE TABLE IF NOT EXISTS request (
            query String,
            quantity UInt32,
            updated DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree(updated)
        ORDER BY query;
    """
    )

    # Создание таблицы RequestProducts
    client.command(
        """
        CREATE TABLE IF NOT EXISTS request_product (
            city Int64 CODEC(LZ4),
            query String CODEC(LZ4),
            date Date CODEC(Delta, LZ4),
            products Array(UInt32) CODEC(LZ4),
            INDEX idx_products (products) TYPE set(900) GRANULARITY 90
        ) ENGINE = MergeTree()
        PARTITION BY city
        ORDER BY date;"""
    )
    client.command("DROP TABLE IF EXISTS request_product_2;")
    client.command(
        """
        CREATE TABLE IF NOT EXISTS request_product_2 (
            city Int64 CODEC(LZ4HC),
            query String CODEC(LZ4HC),
            date Date CODEC(LZ4HC),
            product UInt32 CODEC(LZ4),
            place UInt16,
            INDEX idx_product (query, product) TYPE bloom_filter(0.01) GRANULARITY 1
        ) ENGINE = MergeTree()
        PARTITION BY city
        ORDER BY (date, query);"""
    )
    count = 7430728
    max_rows = 500
    steps = count // max_rows
    extra_step = 1 if count % max_rows else 0
    logger.info("START TO ALTER DB TO UNNESTED")
    for i in range(steps + extra_step):
        client.command(f"""INSERT INTO request_product_2 (city, query, date, product, place) 
            SELECT city, query, date, product, indexOf(products, product) AS place 
            FROM request_product 
            ARRAY JOIN products AS product 
            WHERE indexOf(products, product) > 0 
            LIMIT {max_rows} OFFSET {i * max_rows};""")
        logger.info("Tables created successfully.")
    tables = client.query("SHOW TABLES")
    logger.info(tables.result_rows)
    client.close()


asyncio.run(setup_database())

import asyncio
from datetime import date, timedelta

from clickhouse_db.get_async_connection import get_async_connection
from server.funcs.upload_requests_data import recount_growth_by_date
from settings import logger

async def main():
    stmt = """INSERT INTO query_products_daily
SELECT
    query,
    date,
    groupUniqArrayState(product) AS products
FROM request_product WHERE city = 1 AND date = {date} AND product BETWEEN 1 AND 200000000
GROUP BY query, date;
"""
    stmt_1 = """INSERT INTO query_products_daily
SELECT
    query,
    date,
    groupUniqArrayState(product) AS products
FROM request_product WHERE city = 1 AND date = {date} AND product BETWEEN 200000001 AND 280000000
GROUP BY query, date"""
    stmt_2 = """INSERT INTO query_products_daily
SELECT
    query,
    date,
    groupUniqArrayState(product) AS products
FROM request_product WHERE city = 1 AND date = {date} AND product BETWEEN 280000001 AND 350000000
GROUP BY query, date"""
    stmt_3 = """INSERT INTO query_products_daily
SELECT
    query,
    date,
    groupUniqArrayState(product) AS products
FROM request_product WHERE city = 1 AND date = {date} AND product BETWEEN 350000001 AND 500000000
GROUP BY query, date"""
    dates = [i for i in range(152)]
    stmts = [stmt, stmt_1, stmt_2, stmt_3]
    dates.reverse()
    async with get_async_connection() as client:
        for d in dates:
            logger.info(f"date: {d}")
            for s in stmts:
                st = s.format(date=d)
                await client.command(st)

if __name__ == '__main__':
    asyncio.run(main())
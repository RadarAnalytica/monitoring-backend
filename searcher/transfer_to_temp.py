import asyncio
from datetime import date, timedelta

from clickhouse_db.get_async_connection import get_async_connection
from server.funcs.upload_requests_data import recount_growth_by_date
from settings import logger

async def main():
    step = 50000000
    l = 1
    r = 500000001
    dates = [i for i in range(156)]
    dates.reverse()
    async with get_async_connection() as client:
        for d in dates:
            logger.info(f"date: {d}")
            for i in range(l, r, step):
                left = i
                right = i + step - 1
                logger.info(f"LEFT {left}, RIGHT {right}")
                stmt = f"""INSERT INTO query_products_daily
SELECT
    query,
    date,
    groupUniqArrayState(product) AS products,
    countDistinctIfState(product, advert = 'b') AS advert_b_count,
    countDistinctIfState(product, advert = 'c') AS advert_c_count
FROM request_product WHERE city = 1 AND date = {d} AND product BETWEEN {left} AND {right}
GROUP BY query, date;
"""
                await client.command(stmt)

if __name__ == '__main__':
    asyncio.run(main())
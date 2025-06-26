import asyncio
from datetime import date, timedelta

from clickhouse_db.get_async_connection import get_async_connection
from server.funcs.upload_requests_data import recount_growth_by_date
from settings import logger

async def main():
    step = 25000000
    l = 1
    r = 500000001
    dates = [143]
    async with get_async_connection() as client:
        for d in dates:
            logger.info(f"date: {d}")
            for i in range(l, r, step):
                left = i
                right = i + step - 1
                logger.info(f"LEFT {left}, RIGHT {right}")
                stmt = f"""
INSERT INTO query_products_daily
SELECT
    query,
    date,
    groupUniqArrayStateIf(product, place <= 30) AS products_30,
    groupUniqArrayStateIf(product, place <= 100) AS products_100,
    groupUniqArrayStateIf(product, place <= 300) AS products_300,
    countDistinctIfState(product, (advert = 'b') AND (place <= 100)) AS advert_b_count,
    countDistinctIfState(product, (advert = 'c') AND (place <= 100)) AS advert_c_count,
    uniqExactState(product) AS products_top_count
FROM request_product
WHERE city = 1
  AND date = {d}
  AND product BETWEEN {left} AND {right}
GROUP BY query, date;
"""
                await client.command(stmt)

if __name__ == '__main__':
    asyncio.run(main())
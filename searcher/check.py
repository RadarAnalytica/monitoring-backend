import asyncio
from datetime import datetime
from clickhouse_connect.driver import AsyncClient

from clickhouse_db.get_async_connection import get_async_connection
from settings import logger


async def check(searched_val, city):
    async with get_async_connection() as client:
        # client: AsyncClient = client
        query = f"""SELECT id, dest
        FROM city
        WHERE (updated = (SELECT max(updated) FROM city));"""
        res1 = await client.query(query)
        logger.info(res1)
        query = f"""SELECT id, query
                FROM request
                WHERE (updated = (SELECT max(updated) FROM request)) LIMIT 100;"""
        res2 = await client.query(query)
        logger.info(res2)
        # return res1.result_rows
        # json_result = [{"date": str(row[0]), "products": row[1]} for row in res.result_rows]
        # logger.info(res.result_rows)
        # start = datetime.now()
        # query = f"""SELECT sd.query, sd.quantity, groupArray((sd.date, sd.place)) AS date_info
        # FROM (SELECT rp.query, r.quantity, rp.date, rp.place
        # FROM request_product AS rp
        # JOIN (SELECT * FROM request FINAL) AS r ON r.query = rp.query
        # WHERE (rp.city = {city})
        # AND rp.product = {searched_val}
        # AND (rp.date >= toStartOfDay(now() - INTERVAL 7 DAY))
        # ORDER BY rp.date, r.quantity DESC
        # ) AS sd
        # GROUP BY sd.query, sd.quantity
        # ORDER BY sd.quantity DESC, sd.query;"""
        # query_result = await client.query(query)
        # result = [
        #     {
        #         "query": row[0],
        #         "quantity": row[1],
        #         "dates": {
        #             str(j_row[0]): j_row[1]
        #             for j_row in row[2]
        #         }
        #     }
        #     for row in query_result.result_rows
        # ]
        # logger.info(f"Result rdy in {(datetime.now() - start).total_seconds()}")
        # return result
        # query = f"""SELECT product
        #         FROM request_product
        #         WHERE city = {city}
        #         AND date = '{datetime.now().date().strftime("%Y-%m-%d")}'
        #         LIMIT 1000;"""
        # query_result = await client.query(query)
        # return [row[0] for row in query_result.result_rows]


logger.info(asyncio.run(check(230923798, -1257786)))

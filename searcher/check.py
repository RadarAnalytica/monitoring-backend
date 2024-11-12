import asyncio
from datetime import datetime
from clickhouse_connect.driver import AsyncClient

from clickhouse_db.get_async_connection import get_async_connection
from settings import logger


async def check(searched_val, city):
    async with get_async_connection() as client:
        # client: AsyncClient = client
        # query = f"""SELECT id, dest
        # FROM city
        # WHERE (updated = (SELECT max(updated) FROM city)) ORDER BY id;"""
        # res1 = await client.query(query)
        # logger.info(res1.result_rows)
        # query = f"""SELECT id, query
        #         FROM request
        #         WHERE (updated = (SELECT max(updated) FROM request))
        #         ORDER BY id
        #         LIMIT 100;"""
        # res2 = await client.query(query)
        # logger.info(res2.result_rows)
        # return res1.result_rows
        # json_result = [{"date": str(row[0]), "products": row[1]} for row in res.result_rows]
        # logger.info(res.result_rows)
        start = datetime.now()
        query = f"""SELECT r.query as query, r.quantity as quantity, rp.place as place
        FROM request_product AS rp
        JOIN (SELECT id, query, quantity FROM request FINAL) AS r ON r.id = rp.query
        WHERE (rp.product = {searched_val})
        ORDER BY rp.date, r.quantity DESC;"""
        query_result = await client.query(query)
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
        logger.info(f"Result rdy in {(datetime.now() - start).total_seconds()}")
        return query_result.result_rows
        # query = f"""SELECT *
        #         FROM request_product
        #         LIMIT 1000;"""
        # query_result = await client.query(query)
        # return query_result.result_rows


async def get_dates_data():
    async with get_async_connection() as client:
        query = "SELECT id, date FROM dates WHERE id = (SELECT max(id) FROM dates);"
        q = await client.query(query)
    return q.result_rows


logger.info(asyncio.run(check(610589, -1257786)))

import asyncio
from clickhouse_db.get_async_connection import get_async_connection
from settings import logger


async def check(searched_val, city):
    async with get_async_connection() as client:
        # client: AsyncClient = client
        # query = f"""SELECT sd.query, sd.quantity, groupArray((sd.date, indexOf(sd.products, {searched_val}) AS product_index)) AS date_info
        # FROM (SELECT rp.query, rp.date, r.quantity, rp.products
        # FROM request_product AS rp
        # JOIN (SELECT * FROM request FINAL) AS r ON r.query = rp.query
        # WHERE has(rp.products, {searched_val})
        # AND (rp.city = {city})
        # AND (rp.date BETWEEN '2024-10-29' AND '2024-10-31')
        # ORDER BY rp.date, r.quantity DESC
        # ) AS sd
        # GROUP BY sd.query, sd.quantity
        # ORDER BY sd.quantity DESC, sd.query;"""
        # # query = f"SELECT rp.query, r.quantity FROM request_product as rp JOIN request AS r ON r.query = rp.query WHERE rp.city = {city} AND arrayExists(x -> x IN {searched_val}, rp.products) ORDER BY r.quantity DESC;"
        # query = f"SELECT city, count(*) FROM request_product GROUP BY city;"
        # res = await client.query(query)
        # return res.result_rows
        # # json_result = [{"date": str(row[0]), "products": row[1]} for row in res.result_rows]
        # logger.info(res.result_rows)
        # query = f"""SELECT sd.query, sd.quantity, groupArray((sd.date, indexOf(sd.products, {searched_val}) AS product_index)) AS date_info
        # FROM (SELECT rp.query, rp.date, r.quantity, rp.products
        # FROM request_product AS rp
        # JOIN (SELECT * FROM request FINAL) AS r ON r.query = rp.query
        # WHERE has(rp.products, {searched_val})
        # AND (rp.city = {city})
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
        # return result
        query = f"""SELECT rp.query, r.quantity
                FROM request_product AS rp
                JOIN (SELECT * FROM request FINAL) AS r ON r.query = rp.query
                WHERE rp.city = {city} AND arrayExists(x -> x IN {searched_val}, rp.products)
                ORDER BY r.quantity DESC;"""
        query_result = await client.query(query)
        return dict(query_result.result_rows)


logger.info(asyncio.run(check([212296429], -1257786)))
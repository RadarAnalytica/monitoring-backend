from clickhouse_db.get_async_connection import get_async_connection
from clickhouse_connect.driver import AsyncClient


async def get_keywords_db_data(products, city=-1257786):
    params = {"v1": tuple(products), "v2": city}
    async with get_async_connection() as client:
        query = f"""SELECT r.query, r.quantity
        FROM request_product AS rp
        JOIN (SELECT * FROM request FINAL) AS r ON r.id = rp.query 
        JOIN city as c ON c.id = rp.city 
        WHERE rp.product IN %(v1)s AND c.dest = %(v2)s
        ORDER BY r.quantity DESC;"""
        query_result = await client.query(query, parameters=params)
        return query_result.result_rows


async def get_keywords_payload(products):
    query_result = await get_keywords_db_data(products)
    payload = dict(query_result)
    return payload

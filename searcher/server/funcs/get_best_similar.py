from datetime import datetime

from clickhouse_connect.driver import AsyncClient

from settings import logger
from clickhouse_db.get_async_connection import get_async_connection


async def get_best_similar_products(product_id, city=1, amount=5):
    async with get_async_connection() as client:
        client: AsyncClient = client
        start = datetime.now()
        query = f"""SELECT DISTINCT rp.query
        FROM request_product AS rp
        JOIN (SELECT id, query, quantity FROM request FINAL) AS r ON r.id = rp.query
        JOIN dates as d ON d.id = rp.date
        JOIN city as c ON c.id = rp.city
        WHERE (rp.product = {product_id})
        AND (c.id = {city})
        AND (d.date > toStartOfDay(now() - INTERVAL 7 DAY))
        ORDER BY r.quantity DESC LIMIT 10;"""
        query_result = await client.query(query)
        keywords = [str(kw[0]) for kw in query_result.result_rows]
        logger.info(f"Ключевые слова: {(datetime.now() - start).total_seconds()}s")
        query = f"""SELECT max(id) FROM dates"""
        query_result = await client.query(query)
        last_date = query_result.result_rows[0][0]
        logger.info(f"Дата: {(datetime.now() - start).total_seconds()}s")
        query = f"""SELECT DISTINCT product 
                FROM request_product
                WHERE (product != {product_id})
                AND (query IN ({','.join(keywords)}))
                ORDER BY place 
                LIMIT {amount};"""
        query_result = await client.query(query)
        result = [p[0] for p in query_result.result_rows]
        logger.info(f"Выполнено за: {(datetime.now() - start).total_seconds()}s")
    return result
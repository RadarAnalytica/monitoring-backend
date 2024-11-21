from datetime import datetime

from settings import logger
from clickhouse_db.get_async_connection import get_async_connection


async def get_best_similar_products(product_id, city=1, amount=25):
    async with get_async_connection() as client:
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
        query = f"""SELECT DISTINCT rp.product 
                FROM request_product
                WHERE (rp.product != {product_id})
                AND (city = {city})
                AND (date = (SELECT max(id) FROM dates))
                AND (query IN ({','.join(keywords)}))
                ORDER BY place LIMIT {amount};"""
        query_result = await client.query(query)
        result = [p[0] for p in query_result.result_rows]
        logger.info(f"Выполнено за: {(datetime.now() - start).total_seconds()}s")
    return result
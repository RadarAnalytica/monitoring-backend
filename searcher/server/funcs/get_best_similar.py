from clickhouse_db.get_async_connection import get_async_connection


async def get_best_similar_products(product_id, city=1, amount=25):
    async with get_async_connection() as client:
        query = f"""SELECT DISTINCT r.query, rp.query
        FROM request_product AS rp
        JOIN (SELECT id, query, quantity FROM request FINAL) AS r ON r.id = rp.query
        JOIN dates as d ON d.id = rp.date
        JOIN city as c ON c.id = rp.city
        WHERE (rp.product = {product_id})
        AND (c.id = {city})
        AND (d.date > toStartOfDay(now() - INTERVAL 7 DAY))
        ORDER BY r.quantity DESC LIMIT 10;"""
        query_result = await client.query(query)
        keywords = [f'"{kw[0]}"' for kw in query_result.result_rows]
        query = f"""SELECT DISTINCT rp.product 
                FROM request_product AS rp
                JOIN (SELECT id, query, quantity FROM request FINAL) AS r ON r.id = rp.query
                JOIN dates as d ON d.id = rp.date
                JOIN city as c ON c.id = rp.city
                WHERE (rp.product != {product_id})
                AND (c.id = {city})
                AND (d.date > toStartOfDay(now() - INTERVAL 7 DAY))
                AND (r.query IN ({', '.join(keywords)}))
                ORDER BY r.quantity DESC, rp.place LIMIT {amount};"""
        query_result = await client.query(query)
        result = [p[0] for p in query_result.result_rows]
    return result
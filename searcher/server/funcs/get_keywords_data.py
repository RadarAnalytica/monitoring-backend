from clickhouse_db.get_async_connection import get_async_connection
from clickhouse_connect.driver import AsyncClient


async def get_keywords_db_data(products, city=1):
    params = {"v1": tuple(products), "v2": city}
    async with get_async_connection() as client:
        query = f"""
        WITH query_ids AS (
            SELECT 
                DISTINCT query 
            FROM 
                request_product
            WHERE
                city = %(v2)s
            AND
                product IN %(v1)s
            AND date >= (SELECT id FROM dates WHERE date = today() - 30)
            ORDER BY query
        )
        SELECT r.query, rf.freq_sum
        FROM request AS r
        JOIN (
            SELECT query_id, sum(frequency) as freq_sum FROM request_frequency
            WHERE query_id IN query_ids
            AND date >= today() - 30
            GROUP BY query_id
        ) AS rf ON rf.query_id = r.id
        WHERE WHERE r.id IN query_ids
        ORDER BY rf.freq_sum DESC;"""
        query_result = await client.query(query, parameters=params)
        return query_result.result_rows


async def get_keywords_payload(products):
    query_result = await get_keywords_db_data(products)
    payload = dict(query_result)
    return payload

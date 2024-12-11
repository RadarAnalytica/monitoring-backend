from clickhouse_db.get_async_connection import get_async_connection


async def get_preset_db_data():
    async with get_async_connection() as client:
        query = f"""SELECT preset, norm_query, groupArray(r.query) AS query_info FROM preset as p
        JOIN (SELECT * FROM request FINAL) AS r ON r.id = p.query 
        WHERE (date = (SELECT max(date) FROM preset)) 
        GROUP BY preset, norm_query 
        ORDER BY preset, norm_query;"""
        q = await client.query(query)
    result = dict()
    for row in q.result_rows:
        result[row[0]] = {"name": row[1], "queries": list(row[2])}
    return result
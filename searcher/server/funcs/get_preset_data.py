from clickhouse_db.get_async_connection import get_async_connection


async def get_preset_db_data(city=2):
    async with get_async_connection() as client:
        query = f"""SELECT preset, groupArray(norm_query) AS query_info FROM preset 
        WHERE (date = MAX(SELECT date FROM preset)) 
        AND (city = {city}) 
        GROUP BY preset 
        ORDER BY preset;"""
        q = await client.query(query)
    result = dict()
    for row in q.result_rows:
        result[row[0]] = list(row[1])
    return result
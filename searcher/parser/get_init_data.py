from clickhouse_db.get_async_connection import get_async_connection


async def get_cities_data():
    async with get_async_connection() as client:
        query = "SELECT dest FROM city FINAL;"
        q = await client.query(query)
        cities = [city[0] for city in q.result_rows]
    return cities


async def get_requests_data():
    async with get_async_connection() as client:
        query = "SELECT * FROM request WHERE updated = (SELECT max(updated) FROM request) LIMIT 1000000;"
        q = await client.query(query)
        requests = [rr[0] for rr in q.result_rows]
    return requests

from datetime import date

from clickhouse_db.get_async_connection import get_async_connection


async def get_cities_data():
    async with get_async_connection() as client:
        query = "SELECT id, dest FROM city FINAL;"
        q = await client.query(query)
        cities = [(city[0], city[1]) for city in q.result_rows]
    return cities


async def get_requests_data():
    async with get_async_connection() as client:
        query = "SELECT id, query FROM request WHERE updated = (SELECT max(updated) FROM request) LIMIT 1000000;"
        q = await client.query(query)
        requests = [(r[0], r[1]) for r in q.result_rows]
    return requests


async def get_dates_data():
    async with get_async_connection() as client:
        query = "SELECT id, date FROM dates WHERE id = (SELECT max(id) FROM dates);"
        q = await client.query(query)
        last_date = [(dt[0], dt[1]) if dt else (0, None) for dt in q.result_rows]
    return last_date


async def write_new_date(date_data: tuple[int, date]):
    async with get_async_connection() as client:
        await client.insert("dates", [date_data], column_names=["id", "date"])

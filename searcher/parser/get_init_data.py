from datetime import date
from settings import logger
from clickhouse_db.get_async_connection import get_async_connection


async def get_cities_data(city_id):
    async with get_async_connection() as client:
        query = f"""SELECT id, dest
        FROM city
        WHERE id = {city_id} ORDER BY id;"""
        q = await client.query(query)
        result = q.result_rows
        logger.info(result)
    return result


async def get_requests_data():
    async with get_async_connection() as client:
        query = f"""SELECT id, query
                FROM request
                WHERE (updated = (SELECT max(updated) FROM request)) 
                ORDER BY quantity DESC LIMIT 1000000;"""
        q = await client.query(query)
    return q.result_rows


async def get_requests_id_download_data():
    async with get_async_connection() as client:
        query = f"""SELECT DISTINCT query, id FROM request ORDER BY id LIMIT 100;"""
        q = await client.query(query)
    return dict(q.result_rows)


async def get_dates_data():
    async with get_async_connection() as client:
        query = "SELECT id, date FROM dates WHERE id = (SELECT max(id) FROM dates);"
        q = await client.query(query)
    return q.result_rows


async def write_new_date(date_data: tuple[int, date]):
    async with get_async_connection() as client:
        await client.insert("dates", [date_data], column_names=["id", "date"])

from datetime import date, timedelta
from settings import logger
from clickhouse_db.get_async_connection import get_async_connection


async def get_cities_data(city_id):
    async with get_async_connection() as client:
        query = f"""SELECT id, dest, name 
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
        query = f"""SELECT query, id FROM request FINAL ORDER BY id;"""
        q = await client.query(query)
    return dict(q.result_rows)

async def get_requests_id_download_data_new(query: str):
    async with get_async_connection() as client:
        params = {
            "v1": query
        }
        query = f"""SELECT id FROM request WHERE query = %(v1)s;"""
        q = await client.query(query, parameters=params)
    return q.result_rows[0][0] if q.result_rows and q.result_rows[0] else None


async def get_request_frequency_download_data_new(query_id: int, new_date, client):
    start_date = new_date - timedelta(days=6)
    end_date = new_date - timedelta(days=1)
    params = {
        "v1": query_id,
    }
    query = f"""SELECT sum(frequency) FROM request_frequency WHERE query_id = %(v1)s AND date BETWEEN '{str(start_date)}' AND '{str(end_date)}';"""
    q = await client.query(query, parameters=params)
    return q.result_rows[0][0] if q.result_rows and q.result_rows[0] else None



async def get_requests_max_id():
    async with get_async_connection() as client:
        query = f"""SELECT max(id) FROM request FINAL"""
        q = await client.query(query)
    return q.result_rows[0][0] if q.result_rows and q.result_rows[0] else 0


async def get_dates_data():
    async with get_async_connection() as client:
        query = "SELECT id, date FROM dates WHERE id = (SELECT max(id) FROM dates);"
        q = await client.query(query)
    return q.result_rows


async def write_new_date(date_data: tuple[int, date]):
    async with get_async_connection() as client:
        await client.insert("dates", [date_data], column_names=["id", "date"])

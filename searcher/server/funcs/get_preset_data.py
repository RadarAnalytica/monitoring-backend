from datetime import datetime, timedelta

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


async def get_preset_by_id_db_data(preset_id: int):
    async with get_async_connection() as client:
        param = {
            "v1": preset_id
        }
        query = f"""SELECT preset, norm_query, groupArray(distinct r.query) AS query_info FROM preset as p
        JOIN (SELECT * FROM request FINAL) AS r ON r.id = p.query 
        WHERE (date = (SELECT max(date) FROM preset))
        AND preset = %(v1)s 
        GROUP BY preset, norm_query 
        ORDER BY preset, norm_query;"""
        q = await client.query(query, parameters=param)
    row = q.result_rows[0] if q.result_rows else None
    if not row:
        return dict()
    queries = sorted(list(row[2]))
    result = {"name": row[1], "cluster": row[1], "queries": sorted(list(row[2]))}
    return result


async def get_query_frequency_db(query: str):
    start_date = datetime.now().date() - timedelta(days=30)
    async with get_async_connection() as client:
        param = {
            "v1": query
        }
        query_preset = """SELECT p.preset, p.norm_query FROM preset as p JOIN request as r ON r.id = p.query WHERE r.query = %(v1)s GROUP BY p.preset, p.norm_query"""
        query_frequency = """SELECT rf.date, sum(rf.frequency) FROM request_frequency as rf JOIN request as r ON r.id = rf.query_id WHERE r.query = %(v1)s AND rf.date >= %(v2)s GROUP BY rf.date ORDER BY rf.date"""
        q_p = await client.query(query_preset, parameters=param)
        param["v2"] = start_date
        q_f = await client.query(query_frequency, parameters=param)
        preset_data = q_p.result_rows[0] if q_p.result_rows else None
        frequency_data = {str(row[0]): row[1] for row in q_f.result_rows}
    result = {
        "preset": preset_data[0] if preset_data else None,
        "norm_query": preset_data[1] if preset_data else None,
        "frequency": frequency_data
    }
    return result
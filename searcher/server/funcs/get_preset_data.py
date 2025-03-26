from collections import defaultdict
from datetime import datetime, timedelta

from clickhouse_db.get_async_connection import get_async_connection


MONTH_DICT = {
    1: "Январь",
    2: "Февраль",
    3: "Март",
    4: "Апрель",
    5: "Май",
    6: "Июнь",
    7: "Июль",
    8: "Август",
    9: "Сентябрь",
    10: "Октябрь",
    11: "Ноябрь",
    12: "Декабрь"

}

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


async def get_preset_by_id_db_data(query: str):
    start_date = datetime.now().date() - timedelta(days=30)
    async with get_async_connection() as client:
        param = {
            "v1": query,
        }
        queries_query = """SELECT query FROM preset WHERE preset IN (SELECT p.preset FROM preset as p JOIN request as r on r.id = p.query WHERE r.query = %(v1)s)"""
        q = await client.query(queries_query, parameters=param)
        queries = tuple((row[0] for row in q.result_rows))
        param_freq = {
            "v1": queries,
            "v2": start_date,
        }
        frequency_query = """SELECT r.query, groupArray((rf.date, rf.date_sum)), sum(rf.date_sum) as total FROM (
        SELECT query_id as query_id, date as date, sum(frequency) as date_sum 
        FROM request_frequency 
        WHERE query_id IN %(v1)s 
        AND date >= %(v2)s
        GROUP BY query_id, date
        ORDER BY query_id, date
        ) as rf 
        JOIN request as r ON r.id = rf.query_id 
        GROUP BY r.query
        ORDER BY total DESC
        """
        q_f = await client.query(frequency_query, parameters=param_freq)
        result = [{"query": row[0], "frequency": dict(row[1]), "total": row[2]} for row in q_f.result_rows]
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

async def get_query_frequency_all_time_db(query: str):
    start_date = datetime.now().date() - timedelta(days=30)
    async with get_async_connection() as client:
        param = {
            "v1": query
        }
        query_frequency = """SELECT toYear(rf.date) as y, toMonth(rf.date) as m, sum(rf.frequency) FROM request_frequency as rf JOIN request as r ON r.id = rf.query_id WHERE r.query = %(v1)s GROUP BY y, m ORDER BY y, m"""
        param["v2"] = start_date
        q_f = await client.query(query_frequency, parameters=param)
        frequency_data = []
        for row in q_f.result_rows:
            year = row[0]
            month = row[1]
            ym_string = f"{year} {MONTH_DICT.get(month)}"
            val = row[2]
            frequency_data.append({ym_string: val})
    result = {
        query: frequency_data
    }
    return result


async def get_preset_by_query_all_time_db_data(query: str):
    async with get_async_connection() as client:
        param = {
            "v1": query,
        }
        queries_query = """SELECT query FROM preset WHERE preset IN (SELECT p.preset FROM preset as p JOIN request as r on r.id = p.query WHERE r.query = %(v1)s)"""
        q = await client.query(queries_query, parameters=param)
        queries = tuple((row[0] for row in q.result_rows))
        param_freq = {
            "v1": queries,
        }
        frequency_query = """SELECT r.query, groupArray((y, m, rf.date_sum)), sum(rf.date_sum) as total FROM (
        SELECT query_id as query_id, toYear(date) as y, toMonth(date) as m, sum(frequency) as date_sum 
        FROM request_frequency 
        WHERE query_id IN %(v1)s 
        GROUP BY query_id, y, m
        ORDER BY query_id, y, m
        ) as rf 
        JOIN request as r ON r.id = rf.query_id 
        GROUP BY r.query
        ORDER BY total DESC
        """
        q_f = await client.query(frequency_query, parameters=param_freq)
        result = [{"query": row[0], "frequency": dict(row[1]), "total": row[2]} for row in q_f.result_rows]
    return result
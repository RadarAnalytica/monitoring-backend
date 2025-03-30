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


async def get_single_preset_db_data(query: str):
    async with get_async_connection() as client:
        param = {
            "v1": query,
        }
        query = f"""SELECT preset, norm_query FROM preset WHERE query = (SELECT max(id) FROM request WHERE query = %(v1)s)"""
        q = await client.query(query, parameters=param)
        q_result = list(q.result_rows)
        result = {
            "preset": q_result[0][0] if q_result and q_result[0] else None,
            "norm_query": q_result[0][1] if q_result and q_result[0] else None
        }
    return result


async def get_preset_by_id_db_data(query: str):
    start_date = datetime.now().date() - timedelta(days=30)
    async with get_async_connection() as client:
        param = {
            "v1": query,
        }
        queries_query = """SELECT norm_query, query FROM preset WHERE preset IN (SELECT p.preset FROM preset as p JOIN request as r on r.id = p.query WHERE r.query = %(v1)s) GROUP BY norm_query, query"""
        q = await client.query(queries_query, parameters=param)
        norm_query = None
        queries_list = []
        for row in q.result_rows:
            if not norm_query:
                norm_query = row[0]
            queries_list.append(row[1])
        if not norm_query:
            return dict()
        queries = tuple((row[1] for row in q.result_rows))
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
        JOIN (SELECT id as id, query as query FROM request WHERE id IN %(v1)s) as r ON r.id = rf.query_id 
        GROUP BY r.query
        ORDER BY total DESC
        """
        q_f = await client.query(frequency_query, parameters=param_freq)
        result = {
            "preset": norm_query,
            "queries": {
                row[0]: [{sub_row[0].strftime("%d.%m.%Y"): sub_row[1]} for sub_row in row[1]] for row in q_f.result_rows
            }
        }
    return result


async def get_query_frequency_db(query: str):
    start_date = datetime.now().date() - timedelta(days=29)
    async with get_async_connection() as client:
        param_q_id = {
            "v1": query
        }
        query_id_query = """SELECT id FROM request where query = %(v1)s"""
        q_id = await client.query(query_id_query, parameters=param_q_id)
        query_id = q_id.result_rows[0][0] if q_id.result_rows and q_id.result_rows[0] else None
        if not query_id:
            return {
                query: []
            }
        param_sums = {
            "v1": query_id,
            "v2": start_date
        }
        query_frequency = """SELECT rf.date, sum(rf.frequency) FROM request_frequency as rf  WHERE rf.query_id = %(v1)s AND date >= %(v2)s GROUP BY rf.date ORDER BY rf.date"""
        q_f = await client.query(query_frequency, parameters=param_sums)
        frequency_data = [{row[0].strftime("%d.%m.%Y"): row[1]} for row in q_f.result_rows]
    result = {
        query: frequency_data
    }
    return result

async def get_query_frequency_all_time_db(query: str):
    async with get_async_connection() as client:
        param_q_id = {
            "v1": query
        }
        query_id_query = """SELECT id FROM request where query = %(v1)s"""
        q_id = await client.query(query_id_query, parameters=param_q_id)
        query_id = q_id.result_rows[0][0] if q_id.result_rows and q_id.result_rows[0] else None
        if not query_id:
            return {
                query: []
            }
        param_sums = {
            "v1": query_id,
        }
        query_frequency = """SELECT toYear(rf.date) as y, toMonth(rf.date) as m, sum(rf.frequency) FROM request_frequency as rf  WHERE rf.query_id = %(v1)s GROUP BY y, m ORDER BY y, m"""
        q_f = await client.query(query_frequency, parameters=param_sums)
        frequency_data = [{f"{row[0]} {MONTH_DICT.get(row[1])}": row[2]} for row in q_f.result_rows]
    result = {
        query: frequency_data
    }
    return result


async def get_preset_by_query_all_time_db_data(query: str):
    async with get_async_connection() as client:
        param = {
            "v1": query,
        }
        queries_query = """SELECT norm_query, query FROM preset WHERE preset IN (SELECT p.preset FROM preset as p JOIN request as r on r.id = p.query WHERE r.query = %(v1)s) GROUP BY norm_query, query"""
        q = await client.query(queries_query, parameters=param)
        norm_query = None
        queries_list = []
        for row in q.result_rows:
            if not norm_query:
                norm_query = row[0]
            queries_list.append(row[1])
        if not norm_query:
            return dict()
        queries = tuple((row[1] for row in q.result_rows))
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
        JOIN (SELECT id as id, query as query FROM request WHERE id IN %(v1)s) as r ON r.id = rf.query_id 
        GROUP BY r.query
        ORDER BY total DESC
        """
        q_f = await client.query(frequency_query, parameters=param_freq)
        result = {
            "preset": norm_query,
            "queries": {
                row[0]: [{f"{sub_row[0]} {MONTH_DICT.get(sub_row[1])}": sub_row[2]} for sub_row in row[1]] for row in q_f.result_rows
            }
        }
    return result
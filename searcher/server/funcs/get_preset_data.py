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


# def unnest_subjects_list(subjects_list: list):
#     result = dict()
#     for subject_data in subjects_list:
#         s_id = str(subject_data.get("id"))
#         s_name = subject_data.get("name", "").strip().lower()
#         s_parent = str(subject_data.get("parent", 0))
#         children = subject_data.get("childs", [])
#         result[s_id] = dict()
#         result[s_id]["parent"] = s_parent
#         result[s_id]["name"] = s_name
#         children_dict = unnest_subjects_list(children)
#         result.update(unnest_subjects_list(children))
#         result[s_id]["children"] = [key for key in children_dict.keys()]
#     return result
#
#
# async def get_today_subjects_dict():
#     url = "https://static-basket-01.wbcontent.net/vol0/data/subject-base.json"
#     async with ClientSession() as http_session:
#         async with http_session.get(url) as resp:
#             result = await resp.json()
#     subjects_dict = unnest_subjects_list(result)
#     return subjects_dict
#
#
# async def get_subjects_dict():
#     subjects_dict = await redis.get("subjects_dict")
#     if not subjects_dict:
#         subjects_dict = await get_today_subjects_dict()
#         await redis.set("subjects_dict", dumps(subjects_dict), ex=60 * 60 * 24)
#     else:
#         subjects_dict = loads(subjects_dict)
#     return subjects_dict


async def get_preset_by_id_db_data(query: str = None, preset_id: int = None):
    if not query and not preset_id:
        return dict()
    start_date = datetime.now().date() - timedelta(days=30)
    async with get_async_connection() as client:
        param = {
            "v1": preset_id or query,
        }
        if preset_id:
            stmt = """SELECT query FROM request WHERE id IN (SELECT query FROM preset WHERE preset = %(v1)s) ORDER BY quantity DESC LIMIT 1;"""
        else:
            stmt = """SELECT query FROM request WHERE id IN 
            (
                SELECT query FROM preset WHERE preset = 
                    (
                        SELECT preset FROM preset WHERE query = 
                            coalesce((SELECT id FROM request WHERE query = %(v1)s LIMIT 1), 0)
                    )
            ) ORDER BY quantity DESC LIMIT 1;"""
        q = await client.query(stmt, parameters=param)
        norm_query_rows = list(q.result_rows)
        if not norm_query_rows:
            return {
                "preset": preset_id or query,
                "queries": dict()
            }
        norm_query = norm_query_rows[0][0]
        result = {
            "preset": norm_query,
            "queries": dict()
        }
        nq_stmt = f"%{norm_query}%"
        params = {
            "v1": nq_stmt
        }
        stmt = """SELECT id FROM request WHERE query LIKE %(v1)s ORDER BY quantity DESC LIMIT 1"""
        q = await client.query(stmt, parameters=params)
        queries_list = [row[0] for row in q.result_rows]
        param_freq = {
            "v1": queries_list,
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
        all_dates = set()
        for row in q_f.result_rows:
            query = row[0]
            result["queries"][query] = list()
            query_frequency = row[1]
            for sub_row in query_frequency:
                query_date = sub_row[0]
                all_dates.add(query_date)
                quantity = sub_row[1]
                result["queries"][query].append({query_date.strftime("%d.%m.%Y"): quantity})
        dates_list = [d.strftime("%d.%m.%Y") for d in sorted(all_dates)]
        result["dates"] = dates_list
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


async def get_preset_by_query_all_time_db_data(query: str = None, preset_id: int = None):
    if not query and not preset_id:
        return dict()
    async with get_async_connection() as client:
        param = {
            "v1": preset_id or query,
        }

        if preset_id:
            stmt = """SELECT query FROM request WHERE id IN (SELECT query FROM preset WHERE preset = %(v1)s) ORDER BY quantity DESC LIMIT 1;"""
        else:
            stmt = """SELECT query FROM request WHERE id IN 
            (
                SELECT query FROM preset WHERE preset = 
                    (
                        SELECT preset FROM preset WHERE query = 
                            coalesce((SELECT id FROM request WHERE query = %(v1)s LIMIT 1), 0)
                    )
            ) ORDER BY quantity DESC LIMIT 1;"""
        q = await client.query(stmt, parameters=param)
        norm_query_rows = list(q.result_rows)
        if not norm_query_rows:
            return {
                "preset": preset_id or query,
                "queries": dict()
            }
        norm_query = norm_query_rows[0][0]
        nq_stmt = f"%{norm_query}%"
        params = {
            "v1": nq_stmt
        }
        stmt = """SELECT id FROM request WHERE query LIKE %(v1)s ORDER BY quantity DESC LIMIT 100"""
        q = await client.query(stmt, parameters=params)
        queries_list = [row[0] for row in q.result_rows]
        param_q = {
            "v1": queries_list
        }
        stmt = """SELECT query_id
        FROM (
            SELECT
                r.id AS query_id,
                row_number() OVER (PARTITION BY p.preset ORDER BY r.quantity DESC) AS rn
            FROM preset p
            INNER JOIN request r ON p.query = r.id
            WHERE p.preset IN (
                SELECT DISTINCT preset
                FROM preset
                WHERE query IN %(v1)s
            )
        )
        WHERE rn <= 1"""
        q = await client.query(stmt, parameters=param_q)
        final_queries_list = [row[0] for row in q.result_rows]
        param_freq = {
            "v1": final_queries_list,
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
            "queries": dict()
        }
        all_dates = set()
        for row in q_f.result_rows:
            query = row[0]
            result["queries"][query] = list()
            query_frequency = row[1]
            for sub_row in query_frequency:
                query_year = sub_row[0]
                query_month = sub_row[1]
                all_dates.add((query_year, query_month))
                quantity = sub_row[2]
                result["queries"][query].append({f"{query_year} {MONTH_DICT.get(query_month)}": quantity})
        dates_list = [f"{query_year} {MONTH_DICT.get(query_month)}" for query_year, query_month in sorted(all_dates)]
        result["dates"] = dates_list
    return result

from asyncio import TaskGroup

from _datetime import datetime, timedelta
from copy import deepcopy

from clickhouse_db.get_async_connection import get_async_connection
from clickhouse_connect.driver import AsyncClient
from settings import logger


async def gen_dates(interval):
    now = datetime.now().date()
    dates = [now - timedelta(days=i) for i in range(interval)][-1::-1]
    return dates


async def get_product_db_data(product_id, city, interval):
    params = {
        "v1": product_id,
        "v2": city,
        "v3": interval
    }
    async with get_async_connection() as client:
        query = f"""SELECT sd.query, sd.quantity, groupArray((sd.date, sd.place, sd.advert, sd.natural_place, sd.cpm)) AS date_info
        FROM (SELECT r.query as query, r.quantity as quantity, d.date as date, rp.place as place, rp.advert as advert, rp.natural_place as natural_place, rp.cpm as cpm 
        FROM request_product AS rp
        JOIN (SELECT id, query, quantity FROM request FINAL) AS r ON r.id = rp.query
        JOIN dates as d ON d.id = rp.date
        JOIN city as c ON c.id = rp.city
        WHERE (rp.product = %(v1)s)
        AND (c.dest = %(v2)s)
        AND (d.date > toStartOfDay(now() - INTERVAL %(v3)s DAY))
        ORDER BY rp.date, r.quantity DESC
        ) AS sd
        GROUP BY sd.query, sd.quantity
        ORDER BY sd.quantity DESC, sd.query;"""
        async with TaskGroup() as tg:
            query_result = tg.create_task(client.query(query, parameters=params))
            dates = tg.create_task(gen_dates(interval))
        dates = dates.result()
        result = {"dates": dates, "queries": []}
        dates_dummy = {
            str(d): {
                "place": None,
                "ad": None,
                "nat": None,
                "cpm": None,
                "compare_flag": None,
            }
            for d in dates
        }
        for row in query_result.result().result_rows:
            prev_place = 0
            prev_date = None
            row_res = {
                "query": row[0],
                "quantity": row[1],
                "dates": deepcopy(dates_dummy)
            }
            for date_row in row[2]:
                if str(date_row[0]) not in row_res["dates"]:
                    prev_place = date_row[1]
                    continue
                if prev_date and (date_row[0] - prev_date).days > 1:
                    prev_place = 0
                row_res["dates"][str(date_row[0])] = {
                    "place": date_row[1],
                    "ad": date_row[2].decode() if date_row[2] != b"z" else None,
                    "nat": date_row[3] or None,
                    "cpm": date_row[4] or 0 if date_row[2] != b"z" else None,
                    "compare_flag": date_row[1] < prev_place if prev_place != 0 else True
                }
                prev_place = date_row[1]
                prev_date = date_row[0]
            result["queries"].append(row_res)
    return result


async def get_product_db_data_latest(product_id, city):
    now = datetime.now().date() - timedelta(days=1)
    params = {
        "v1": product_id,
        "v2": city,
        "v3": now
    }
    async with get_async_connection() as client:
        query = f"""SELECT r.query, r.quantity, rp.place, rp.advert, rp.natural_place, rp.cpm 
        FROM request_product AS rp
        JOIN dates as d ON d.id = rp.date 
        JOIN city as c ON c.id = rp.city 
        JOIN request AS r ON r.id = rp.query 
        WHERE (rp.product = %(v1)s)
        AND (c.dest = %(v2)s)
        AND (d.date = %(v3)s)
        ORDER BY r.quantity DESC;"""
        query_result = await client.query(query, parameters=params)
        result = {"queries": []}
        for row in query_result.result_rows:
            row_res = {
                "query": row[0],
                "quantity": row[1],
                "place": row[2],
                "ad_type": row[3].decode() if row[3] != b"z" else None,
                "nat_place": row[4] or 0 if row[3] != b"z" else None,
                "cpm": row[5] or 0 if row[3] != b"z" else None
            }
            result["queries"].append(row_res)
    return result


async def get_ex_ad(product_id):
    this_period = datetime.now().date() - timedelta(days=30)
    past_period_start = this_period - timedelta(days=30)
    past_period_end = this_period - timedelta(days=1)
    params = {
        "v1": str(product_id),
    }
    async with get_async_connection() as client:
        client: AsyncClient
        query = """SELECT id FROM request FINAL WHERE query = %(v1)s"""
        query_result = await client.query(query, parameters=params)
        query_id = query_result.result_rows[0][0] if query_result.result_rows and query_result.result_rows[0] else None
        if not query_id:
            return {"quantity": 0, "comparison": 0}
        rf_params = {
            "v1": query_id,
            "v2": this_period,
            "v3": past_period_start,
            "v4": past_period_end
        }
        query = """SELECT sum(trf.frequency), sum(prf.frequency) FROM (
        SELECT query_id as query_id, sum(frequency) as frequency FROM request_frequency 
        WHERE query_id = %(v1)s 
        AND date >= %(v2)s 
        GROUP BY query_id 
        ) AS trf
        JOIN (
        SELECT query_id as query_id, sum(frequency) as frequency FROM request_frequency 
        WHERE query_id = %(v1)s 
        AND date BETWEEN %(v3)s AND %(v4)s 
        GROUP BY query_id 
        ) AS prf ON trf.query_id = prf.query_id
        """
        query_fr_result = await client.query(query, parameters=rf_params)
        results = list(query_fr_result.result_rows)
        this_period_quantity, past_period_quantity = (results[0][0], results[0][1]) if results else (0, 0)
        delta = this_period_quantity - past_period_quantity
        logger.info(f"this {this_period_quantity}, that {past_period_quantity}, delta {delta}")
        percent = round(delta * 100 / past_period_quantity, 2) if past_period_quantity else 0
        result = {
            "quantity": this_period_quantity,
            "comparison": percent
        }
    return result




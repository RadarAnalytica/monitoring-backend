from asyncio import TaskGroup

from _datetime import datetime, timedelta
from copy import deepcopy

from clickhouse_db.get_async_connection import get_async_connection


async def gen_dates(interval):
    now = datetime.now().date()
    dates = [now - timedelta(days=i) for i in range(interval)][-1::-1]
    return dates


async def get_product_db_data(product_id, city, interval):
    async with get_async_connection() as client:
        query = f"""SELECT sd.query, sd.quantity, groupArray((sd.date, sd.place, sd.advert, sd.natural_place, sd.cpm)) AS date_info
        FROM (SELECT r.query as query, r.quantity as quantity, d.date as date, rp.place as place, rp.advert as advert, rp.natural_place as natural_place, rp.cpm as cpm 
        FROM request_product AS rp
        JOIN (SELECT id, query, quantity FROM request FINAL) AS r ON r.id = rp.query
        JOIN dates as d ON d.id = rp.date
        JOIN city as c ON c.id = rp.city
        WHERE (rp.product = {product_id})
        AND (c.dest = {city})
        AND (d.date > toStartOfDay(now() - INTERVAL {interval} DAY))
        ORDER BY rp.date, r.quantity DESC
        ) AS sd
        GROUP BY sd.query, sd.quantity
        ORDER BY sd.quantity DESC, sd.query;"""
        async with TaskGroup() as tg:
            query_result = tg.create_task(client.query(query))
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

        # result = {
        #     "dates": dates.result(),
        #     "queries":
        #     [
        #     {
        #         "query": row[0],
        #         "quantity": row[1],
        #         "dates": {
        #             str(j_row[0]): {"place": j_row[1], "ad": j_row[2].decode() if j_row[2] != b"z" else None,
        #                             "nat": j_row[3] or None, "cpm": j_row[4] or 0 if j_row[2] != b"z" else None}
        #             for j_row in row[2]
        #         }
        #     }
        #     for row in query_result.result().result_rows
        # ]}
    return result




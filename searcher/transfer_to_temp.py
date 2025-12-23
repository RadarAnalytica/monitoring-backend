import asyncio
import json
import math
from collections import defaultdict
from datetime import date, timedelta, datetime, time

from actions.requests_parse import transfer_aggregates, fire_requests
from clickhouse_db.get_async_connection import get_async_connection
from parser.aggregate_supplier import aggregate_supplier
from parser.collect_subjects import write_subjects_raw
from server.funcs.transfer_to_local import recount_oracle
from server.funcs.upload_requests_data import recount_growth_by_date
from service.suppliers_history.harvest import get_today_suppliers_data
from settings import logger
from actions.wb_report_task import download_wb_report_task

# def get_score(value, thresholds):
#     for i, (low, high, score) in enumerate(thresholds):
#         if low <= value < high:
#             return score
#     return thresholds[-1][2]
#
# def evaluate_niche(demand_coef, monopoly_pct, advert_pct, buyout_pct, revenue):
#     demand_thresholds = [
#         (40, float('inf'), 4),
#         (30, 40, 3),
#         (10, 30, 2),
#         (0, 10, 1),
#     ]
#     monopoly_thresholds = [
#         (0, 25, 4),
#         (25, 40, 3),
#         (40, 60, 2),
#         (60, float('inf'), 1),
#     ]
#     advert_thresholds = [
#         (0, 30, 4),
#         (30, 60, 3),
#         (60, 80, 2),
#         (80, float('inf'), 1),
#     ]
#     buyout_thresholds = [
#         (80, float('inf'), 4),
#         (40, 80, 3),
#         (20, 40, 2),
#         (0, 20, 1),
#     ]
#     revenue_thresholds = [
#         (1_000_000, float('inf'), 4),
#         (600_000, 1_000_000, 3),
#         (200_000, 600_000, 2),
#         (0, 200_000, 1),
#     ]
#
#     scores = [
#         get_score(demand_coef, demand_thresholds) * 2,
#         get_score(monopoly_pct, monopoly_thresholds) * 2,
#         get_score(advert_pct, advert_thresholds) * 0.5,
#         get_score(buyout_pct, buyout_thresholds) * 0.5,
#         get_score(revenue, revenue_thresholds) * 0.5,
#     ]
#
#     avg_score = sum(scores) / len(scores)
#     nice_level = round(avg_score)
#
#     return nice_level


def get_score(value, thresholds):
    for low, high, score in thresholds:
        if low <= value < high:
            return score
    return thresholds[-1][2]


def normalize(weights_and_scores):
    weighted = sum(w * s for w, s in weights_and_scores)
    min_score = sum(w * 1 for w, _ in weights_and_scores)
    max_score = sum(w * 4 for w, _ in weights_and_scores)
    normalized = 1 + (weighted - min_score) / (max_score - min_score) * 3
    return round(normalized)

def evaluate_niche(demand_coef, monopoly_pct, advert_pct, buyout_pct, revenue):
    # Пороговые значения
    demand_thresholds = [
        (40, float('inf'), 4),
        (30, 40, 3),
        (10, 30, 2),
        (0, 10, 1),
    ]
    monopoly_thresholds = [
        (0, 25, 4),
        (25, 40, 3),
        (40, 60, 2),
        (60, float('inf'), 1),
    ]
    advert_thresholds = [
        (0, 30, 4),
        (30, 60, 3),
        (60, 80, 2),
        (80, float('inf'), 1),
    ]
    buyout_thresholds = [
        (80, float('inf'), 4),
        (40, 80, 3),
        (20, 40, 2),
        (0, 20, 1),
    ]
    revenue_thresholds = [
        (10_000_000, float('inf'), 4),
        (1_000_000, 10_000_000, 3),
        (500_000, 1_000_000, 2),
        (0, 500_000, 1),
    ]

    # Основной рейтинг с 5 метриками
    full_weights_and_scores = [
        (2, get_score(demand_coef, demand_thresholds)),
        (2, get_score(monopoly_pct, monopoly_thresholds)),
        (0.5, get_score(advert_pct, advert_thresholds)),
        (0.5, get_score(buyout_pct, buyout_thresholds)),
        (0.5, get_score(revenue, revenue_thresholds)),
    ]

    # Рейтинг без buyout и revenue
    base_weights_and_scores = [
        (2, get_score(demand_coef, demand_thresholds)),
        (2, get_score(monopoly_pct, monopoly_thresholds)),
        (0.5, get_score(advert_pct, advert_thresholds)),
    ]

    niche_level = normalize(full_weights_and_scores)
    competition_level = normalize(base_weights_and_scores)

    return niche_level, competition_level

async def transfer_aggregates_to_local():
    stmt = """INSERT INTO wb_id_extended_local SELECT * FROM wb_id_extended"""
    async with get_async_connection(send_receive_timeout=3600) as client:
        await client.command(stmt)

async def main():
    logger.info("начинаем")
    stmt = """SELECT
        qpf2.q as query_id,
        r.query as query,
        qpf2.subject_id as subject_id,
        
        rf.sum_30 AS frequency_30,
        rf.sum_60 AS frequency_60,
        rf.sum_90 AS frequency_90,
        rg.g30 AS g30,
        rg.g60 AS g60,
        rg.g90 AS g90,
        
        qpf2.revenue_total as revenue_total,
        qpf2.top_300 as revenue_300,
        
        qpf2.lost_revenue_total as lost_revenue_total,
        qpf2.lost_revenue_300 as lost_revenue_300,
        
        qpf2.potential_revenue as potential_revenue,
        qpf2.potential_orders as potential_orders,
        
        round(if(qpf2.all_ids > 0, qpf2.revenue_total / qpf2.all_ids, 0), -2) as avg_revenue_total,
        round(if(qpf2.total_ids_300 > 0, qpf2.top_300 / qpf2.total_ids_300, 0), -2) as avg_revenue_300,
        
        qpf2.avg_with_sales as avg_with_sales_revenue,
        
        round(qpf2.revenue_total / 30, -2) as avg_daily_revenue,
        qpf2.avg_daily_wb_id_revenue,
        
        round(if(qpf2.revenue_total > 0, qpf2.lost_revenue_total * 100 / qpf2.revenue_total, 0)) as lost_revenue_percent_total,
        round(if(qpf2.top_300 > 0, qpf2.lost_revenue_300 * 100 / qpf2.top_300, 0)) as lost_revenue_percent_300,
        
        round(if(qpf2.top_100 > 0, qpf2.top_30 * 100 / qpf2.top_100, 0)) as monopoly_percent,
        
        qpf2.orders_total as orders_total,
        qpf2.top_300_orders orders_300,
        
        qpf2.lost_orders,
        round(if(qpf2.top_100 > 0, qpf2.lost_orders * 100 / qpf2.orders_total, 0)) as lost_orders_percent,
        
        
        qpf2.avg_price_total as avg_price_total,
        qpf2.avg_price_300 as avg_price_300,
        
        qpf2.median_price as median_price,
        qpf2.advert as advert_percent,
        qpf2.ex_advert as external_advert_percent,
        qh.total_products AS goods_quantity,
        qpf1.dpc as top_goods_quantity,        
        round(if(qpf1.dpc > 0, rf.sum_30 / qh.total_products, 0), 1) AS freq_per_good,
        
        round(if(qpf2.all_ids > 0, qpf2.with_sales_ids * 100 / qpf2.all_ids, 0)) as goods_with_sales_percent_total,
        qpf2.with_sales_ids as goods_with_sales_quantity_total,
        
        round(if(qpf2.total_ids_300 > 0, qpf2.with_sales_ids_300 * 100 / qpf2.total_ids_300, 0)) as goods_with_sales_percent_300,
        qpf2.total_ids_300 as goods_with_sales_quantity_300,
        
        length(qpf2.suppliers) as suppliers_quantity,
        
        
        qpf2.reviews as avg_reviews,
        qpf2.rating as avg_rating,
        
        qpf2.opr as order_per_review,
        qpf2.ratio as buyout_percent,
        
        qpf2.brands as brands_list,
        qpf2.subjects as subjects_list,
        qpf2.suppler_wb_id_revenue as suppler_revenue,
        
        qpf2.revenue_total_spp as revenue_total_spp,

        qpf2.top_300_spp as revenue_300_spp,

        round(if(qpf2.total_ids_300 > 0, qpf2.top_300_spp / qpf2.total_ids_300, 0), -2) as avg_revenue_300_spp,

        qpf2.avg_price_total_spp as avg_price_total_spp,

        qpf2.avg_price_300_spp as avg_price_300_spp

        
FROM
(
    SELECT
        qpf.query as q,
        round(avg(if(pd.wb_id_price > 0, pd.wb_id_price, NULL)), -2) AS avg_price_total,
        round(avg(if(pd.wb_id_price_spp > 0, pd.wb_id_price_spp, NULL)), -2) AS avg_price_total_spp,
        round(avg(if(qpf.place <= 300 AND pd.wb_id_price > 0, pd.wb_id_price, NULL)), -2) AS avg_price_300,
        round(avg(if(qpf.place <= 300 AND pd.wb_id_price_spp > 0, pd.wb_id_price_spp, NULL)), -2) AS avg_price_300_spp,
        round(median(if(pd.wb_id_price > 0, pd.wb_id_price, NULL)), -2) as median_price,
        round(sum(if(qpf.place <= 30, pd.wb_id_revenue, 0)), -2) AS top_30,
        round(sum(if(qpf.place <= 100, pd.wb_id_revenue, 0)), -2) AS top_100,
        round(sum(if(qpf.place <= 300, pd.wb_id_revenue, 0)), -2) AS top_300,
        round(sum(if(qpf.place <= 300, pd.wb_id_revenue_spp, 0)), -2) AS top_300_spp,
        sum(if(qpf.place <= 300, pd.wb_id_orders, 0)) AS top_300_orders,
        round(sum(pd.wb_id_avg_daily_revenue), -2) as avg_daily_revenue,
        round(sum(if(qpf.place <= 300, pd.wb_id_avg_daily_revenue, 0)), -2) as avg_daily_revenue_300,
        round(coalesce(avg(if(pd.wb_id_avg_daily_revenue > 0, pd.wb_id_avg_daily_revenue, NULL))), -2) as adr,
        round(coalesce(avg(if(pd.wb_id_revenue > 0, pd.wb_id_revenue, NULL)), 0), -2) as avg_with_sales,
        round(coalesce(avg(if(pd.wb_id_avg_daily_revenue > 0, pd.wb_id_avg_daily_revenue, NULL)), 0), -2) as avg_daily_wb_id_revenue,
        round(sum(pd.wb_id_revenue), -2) AS revenue_total,
        round(sum(pd.wb_id_revenue_spp), -2) AS revenue_total_spp,
        sum(pd.wb_id_orders) AS orders_total,
        round(sum(if(qpf.place <= 300, pd.wb_id_lost_revenue, 0)), -2) AS lost_revenue_300,
        round(sum(pd.wb_id_lost_revenue), -2) AS lost_revenue_total,
        round(sum(pd.wb_id_lost_orders)) AS lost_orders,
        sum(if(qpf.place <= 100, qpf.advert, 0)) AS advert,
        sum(if(qpf.place <= 100, rex.ex, 0)) AS ex_advert,
        round(coalesce(avg(if(pd.ratio > 0, pd.ratio, NULL)), 0)) AS ratio,
        round(coalesce(avg(if(pd.rating > 0, pd.rating, NULL)), 0), 1) AS rating,
        round(coalesce(avg(if(pd.root_feedbacks > 0, pd.root_feedbacks, NULL)), 0)) AS reviews,
        round(coalesce(avg(if(pd.wb_id_potential_revenue > 0, pd.wb_id_potential_revenue, NULL)), 0), -2) AS potential_revenue,
        round(coalesce(avg(if(pd.wb_id_potential_orders > 0, pd.wb_id_potential_orders, NULL)), 0)) AS potential_orders,
        round(sum(pd.wb_id_orders) / sum(pd.root_feedbacks), 1) AS opr,
        any(pd.sub_id) as subject_id,
        groupUniqArray(pd.sub_id) AS subjects,
        groupUniqArray(pd.supl_id) AS suppliers,
        groupUniqArray(pd.b_id) AS brands,
        sum(if(pd.wb_id_revenue > 0, 1, 0)) as with_sales_ids,
        count() as all_ids,
        round(sum(if(pd.wb_id_revenue > 0 and qpf.place <= 300, 1, 0)), -2) as with_sales_ids_300,
        sum(if(qpf.place <= 300, 1, 0)) as total_ids_300,
        groupArrayIf((pd.supl_id, pd.wb_id_revenue), qpf.place <= 100) as suppler_wb_id_revenue
    FROM radar.query_product_flat AS qpf
    INNER JOIN
    (
        SELECT
            wb_id,
            round(wb_id_revenue, -2) as wb_id_revenue,
            wb_id_orders,
            round(wb_id_price, -2) as wb_id_price,
            round(wb_id_revenue_spp, -2) as wb_id_revenue_spp,
            round(wb_id_price_spp, -2) as wb_id_price_spp,
            round(wb_id_avg_daily_revenue, -2) as wb_id_avg_daily_revenue,
            wb_id_avg_daily_orders,
            round(wb_id_lost_revenue, -2) as wb_id_lost_revenue,
            wb_id_lost_orders,
            round(wb_id_potential_revenue, -2) as wb_id_potential_revenue,
            wb_id_potential_orders,
            sub_id,
            supl_id,
            b_id,
            rating,
            root_feedbacks,
            ratio
        FROM wb_id_extended_local
        WHERE wb_id IN (
            SELECT DISTINCT product
            FROM query_product_flat
            WHERE query IN (
            SELECT DISTINCT query 
            FROM query_product_flat 
            WHERE date = (SELECT max(id) FROM dates) 
            AND query BETWEEN %(v1)s AND %(v2)s
        ) AND (date BETWEEN (SELECT id FROM dates WHERE date = yesterday() - 29) AND (SELECT id FROM dates WHERE date = yesterday()))
        )
    ) AS pd ON pd.wb_id = qpf.product
    LEFT OUTER JOIN (select query, 1 as ex from request WHERE match(query, '^[0-9]+$')) as rex on rex.query = toString(pd.wb_id) 
    WHERE ((qpf.query >= %(v1)s) AND (qpf.query <= %(v2)s)) AND (qpf.date = (SELECT id FROM dates WHERE date = yesterday()))
    GROUP BY qpf.query
    ORDER BY qpf.query ASC
) as qpf2 
INNER JOIN
    (
        SELECT
            query,
            countDistinct(product) AS dpc
        FROM radar.query_product_flat
        WHERE query IN (
            SELECT DISTINCT query 
            FROM query_product_flat 
            WHERE date = (SELECT max(id) FROM dates) 
            AND query BETWEEN %(v1)s AND %(v2)s
        ) AND (date BETWEEN (SELECT id FROM dates WHERE date = yesterday() - 29) AND (SELECT id FROM dates WHERE date = yesterday()))
        GROUP BY query
    ) AS qpf1 ON qpf2.q = qpf1.query
    INNER JOIN
    (
        SELECT
            query_id,
            sum(if(date >= (yesterday() - 30), frequency, 0)) AS sum_30,
            sum(if(date >= (yesterday() - 60), frequency, 0)) AS sum_60,
            sum(frequency) AS sum_90
        FROM request_frequency
        WHERE query_id IN (
            SELECT DISTINCT query 
            FROM query_product_flat 
            WHERE date = (SELECT max(id) FROM dates) 
            AND query BETWEEN %(v1)s AND %(v2)s
        ) AND (date >= yesterday() - 90)
        GROUP BY query_id
    ) AS rf ON rf.query_id = qpf2.q
    INNER JOIN
    (
        SELECT
            query_id,
            g30,
            g60,
            g90
        FROM request_growth
        WHERE query_id IN (
            SELECT DISTINCT query 
            FROM query_product_flat 
            WHERE date = (SELECT max(id) FROM dates) 
            AND query BETWEEN %(v1)s AND %(v2)s
        ) AND (date = yesterday() - 1)
    ) AS rg ON rg.query_id = qpf2.q
    INNER JOIN
    (
        SELECT
            id,
            query,
        FROM request
        WHERE id IN (
            SELECT DISTINCT query 
            FROM query_product_flat 
            WHERE date = (SELECT max(id) FROM dates) 
            AND query BETWEEN %(v1)s AND %(v2)s
        )
    ) AS r ON r.id = qpf2.q
    INNER JOIN
    (
        SELECT
            query,
            total_products
        FROM query_history
        WHERE query IN (
            SELECT DISTINCT query 
            FROM query_product_flat 
            WHERE date = (SELECT max(id) FROM dates) 
            AND query BETWEEN %(v1)s AND %(v2)s
        ) AND (date = yesterday())
    ) AS qh ON qh.query = qpf2.q
WHERE qpf2.ratio > 0
"""
    stmt_dia = """SELECT
    intDiv(rn - 1, 25000) AS group_num,
    min(id) AS min_id,
    max(id) AS max_id,
    count(*) AS cnt
FROM (
        SELECT
            query_id as id,
            row_number() OVER (ORDER BY id) AS rn
        FROM request_growth WHERE date = yesterday() - 1
    ) AS sorted_ids

GROUP BY group_num
ORDER BY group_num"""
    async with get_async_connection(send_receive_timeout=3600) as client:
        await client.command("""delete from wb_id_extended_local where wb_id_price > 30000000 and sub_id not in (
        8241,
        8827,
        8996,
        8668,
        8670,
        8665,
        8669,
        8662,
        8667,
        8661,
        8663,
        8696,
        8664,
        8666,
        8743,
        8897
        )""")
        q_dia = await client.query(stmt_dia)
        l_r = [(row[1], row[2]) for row in q_dia.result_rows]
        for left, right in l_r:
            logger.info(f"batch {left} - {right}")
            params = {
                "v1": left,
                "v2": right
            }
            q = await client.query(stmt, parameters=params)
            data = []
            for row in q.result_rows:
                query_id = row[0]
                query = row[1]
                subject_id = row[2]
                frequency_30 = row[3]
                frequency_60 = row[4]
                frequency_90 = row[5]
                g30 = row[6]
                g60 = row[7]
                g90 = row[8]
                revenue_total = row[9]
                revenue_300 = row[10]
                lost_revenue_total = row[11]
                lost_revenue_300 = row[12]
                potential_revenue = row[13]
                potential_orders = row[14]
                avg_revenue_total = row[15]
                avg_revenue_300 = row[16]
                avg_with_sales_revenue = row[17]
                avg_daily_revenue = row[18]
                avg_daily_wb_id_revenue = row[19]
                lost_revenue_percent_total = row[20]
                lost_revenue_percent_300 = row[21]
                monopoly_percent = row[22]
                orders_total = row[23]
                orders_300 = row[24]
                lost_orders = row[25]
                lost_orders_percent = row[26]
                avg_price_total = row[27]
                avg_price_300 = row[28]
                median_price = row[29]
                advert_percent = row[30]
                external_advert_percent = row[31]
                goods_quantity = row[32]
                top_goods_quantity = row[33]
                freq_per_good = row[34]
                goods_with_sales_percent_total = row[35]
                goods_with_sales_quantity_total = row[36]
                goods_with_sales_percent_300 = row[37]
                goods_with_sales_quantity_300 = row[38]
                suppliers_quantity = row[39]
                avg_reviews = row[40]
                avg_rating = row[41]
                order_per_review = row[42]
                buyout_percent = row[43]
                brands_list = row[44]
                subjects_list = row[45]
                supplier_revenue = row[46]
                revenue_total_spp = row[47]
                revenue_300_spp = row[48]
                avg_revenue_300_spp = row[49]
                avg_price_total_spp = row[50]
                avg_price_300_spp = row[51]
                suppliers_dict = defaultdict(int)
                for s_id_revenue in supplier_revenue:
                    s_id = s_id_revenue[0]
                    s_revenue = s_id_revenue[1]
                    suppliers_dict[s_id] += s_revenue
                supplier_revenue = list(suppliers_dict.values())
                supplier_revenue.sort(reverse=True)
                if supplier_revenue and len(supplier_revenue) > 0:
                    top_supplier_revenue = supplier_revenue[0]
                    zero_sales_suppliers = sum([1 for sr in supplier_revenue if not sr])
                    suppliers_with_sales_percent = round((len(supplier_revenue) - zero_sales_suppliers) * 100 / len(supplier_revenue))
                    all_suppliers_revenue = sum(supplier_revenue)
                    monopoly_percent = abs(round(top_supplier_revenue * 100 / all_suppliers_revenue)) if all_suppliers_revenue else 0
                else:
                    suppliers_with_sales_percent = 0
                    monopoly_percent = 0
                rating, competition_level = evaluate_niche(demand_coef=freq_per_good, monopoly_pct=monopoly_percent, advert_pct=advert_percent, buyout_pct=buyout_percent, revenue=revenue_300 / 100)
                data.append((
                    query_id,
                    query,
                    subject_id,
                    frequency_30,
                    frequency_60,
                    frequency_90,
                    g30,
                    g60,
                    g90,
                    revenue_total,
                    revenue_300,
                    lost_revenue_total,
                    lost_revenue_300,
                    potential_revenue,
                    potential_orders,
                    avg_revenue_total,
                    avg_revenue_300,
                    avg_with_sales_revenue,
                    avg_daily_revenue,
                    avg_daily_wb_id_revenue,
                    lost_revenue_percent_total,
                    lost_revenue_percent_300,
                    monopoly_percent,
                    orders_total,
                    orders_300,
                    lost_orders,
                    lost_orders_percent,
                    avg_price_total,
                    avg_price_300,
                    median_price,
                    advert_percent,
                    external_advert_percent,
                    goods_quantity,
                    top_goods_quantity,
                    freq_per_good,
                    goods_with_sales_percent_total,
                    goods_with_sales_quantity_total,
                    goods_with_sales_percent_300,
                    goods_with_sales_quantity_300,
                    suppliers_quantity,
                    avg_reviews,
                    avg_rating,
                    order_per_review,
                    buyout_percent,
                    brands_list,
                    subjects_list,
                    rating,
                    competition_level,
                    suppliers_with_sales_percent,
                    revenue_total_spp,
                    revenue_300_spp,
                    avg_revenue_300_spp,
                    avg_price_total_spp,
                    avg_price_300_spp,
                ))

            await client.insert(
                table="monitoring_oracle_stage",
                column_names=[
                    "query_id",
                    "query",
                    "subject_id",
                    "frequency_30",
                    "frequency_60",
                    "frequency_90",
                    "g30",
                    "g60",
                    "g90",
                    "revenue_total",
                    "revenue_300",
                    "lost_revenue_total",
                    "lost_revenue_300",
                    "potential_revenue",
                    "potential_orders",
                    "avg_revenue_total",
                    "avg_revenue_300",
                    "avg_with_sales_revenue",
                    "avg_daily_revenue",
                    "avg_daily_wb_id_revenue",
                    "lost_revenue_percent_total",
                    "lost_revenue_percent_300",
                    "monopoly_percent",
                    "orders_total",
                    "orders_300",
                    "lost_orders",
                    "lost_orders_percent",
                    "avg_price_total",
                    "avg_price_300",
                    "median_price",
                    "advert_percent",
                    "external_advert_percent",
                    "goods_quantity",
                    "top_goods_quantity",
                    "freq_per_good",
                    "goods_with_sales_percent_total",
                    "goods_with_sales_quantity_total",
                    "goods_with_sales_percent_300",
                    "goods_with_sales_quantity_300",
                    "suppliers_quantity",
                    "avg_reviews",
                    "avg_rating",
                    "order_per_review",
                    "buyout_percent",
                    "brands_list",
                    "subjects_list",
                    "niche_rating",
                    "competition_level",
                    "suppliers_with_sales_percent",
                    "revenue_total_spp",
                    "revenue_300_spp",
                    "avg_revenue_300_spp",
                    "avg_price_total_spp",
                    "avg_price_300_spp",
                ],
                data=data
            )
        await client.command("RENAME TABLE monitoring_oracle_new_2 TO monitoring_oracle_old")
        await client.command("RENAME TABLE monitoring_oracle_stage TO monitoring_oracle_new_2")
        await client.command("RENAME TABLE monitoring_oracle_old TO monitoring_oracle_stage")
        await client.command("TRUNCATE TABLE monitoring_oracle_stage")
        await client.command("""delete from monitoring_oracle_new_2 where hasAny(subjects_list, [
            6380,
            6383,
            6381,
            6375,
            6379,
            6376,
            6378,
            3030,
            6373,
            3032,
            6377,
            6387,
            6384,
            6374,
            6385,
            6382,
            3038,
            6388,
            3039,
            8970,
            3042,
            6386,
            6389,
            3044,
            3045
            ])""")


async def migrate_monitoring_oracle_data():
    async with get_async_connection(send_receive_timeout=3600) as client:
        query = "SELECT * FROM radar.monitoring_oracle_new"
        columns = await client.query("DESCRIBE TABLE radar.monitoring_oracle_new")
        column_names = [row[0] for row in columns.result_rows]
        idx = {col: i for i, col in enumerate(column_names)}
        rows = await client.query(query)
        new_rows = []
        new_column_names = column_names.copy()
        new_column_names.insert(idx['niche_rating'] + 1, 'competition_level')
        logger.info("BEGINNING")
        for row in rows.result_rows:
            demand = row[idx['freq_per_good']]
            monopoly = row[idx['monopoly_percent']]
            advert = row[idx['advert_percent']]
            buyout = row[idx['buyout_percent']]
            revenue = row[idx['avg_revenue_total']]

            niche_rating, competition_level = evaluate_niche(demand, monopoly, advert, buyout, revenue)

            row = list(row)
            row.pop(idx['niche_rating'])
            row.insert(idx['niche_rating'], niche_rating)
            row.insert(idx['niche_rating'] + 1, competition_level)

            new_rows.append(tuple(row))
            if len(new_rows) > 10000:
                logger.info("WRITE")
                await client.insert(
                    'radar.monitoring_oracle_new_2',
                    new_rows,
                    column_names=new_column_names
                )
                new_rows = []
        if new_rows:
            logger.info("WRITE")
            await client.insert(
                'radar.monitoring_oracle_new_2',
                new_rows,
                column_names=new_column_names
            )



async def form_lost_table():
    min_date = date(year=2023, month=1, day=25)
    max_date = date(year=2025, month=3, day=18)
    current_date = min_date
    async with get_async_connection(send_receive_timeout=3600) as client:
        q_id = await client.query("""SELECT max(id) FROM request""")
        max_id = list(q_id.result_rows)[0][0]
        while current_date <= max_date:
            logger.info(f"DATE {current_date}")
            stmt_f = f"""SELECT coalesce(r.id, 0), rfc.frequency, rfc.query FROM request_frequency_cleaned rfc LEFT OUTER JOIN request r on r.query = rfc.query WHERE date = '{current_date}'"""
            n_q = await client.query(stmt_f)
            r = list(n_q.result_rows)
            pd = current_date - timedelta(days=6)
            stmt_fh = f"""SELECT query_id, sum(frequency) FROM request_frequency_very_new WHERE date BETWEEN '{pd}' AND '{current_date}' GROUP BY query_id"""
            h_q = await client.query(stmt_fh)
            p_r = dict(h_q.result_rows) if h_q.result_rows else dict()
            new_rows = []
            new_queries = []
            logger.info("GOT FREQUENCY BOTH")
            for row in r:
                query_id = row[0]
                frequency_new = row[1]
                query = row[2]
                if not query_id:
                    query_id = max_id
                    updated = datetime.combine(current_date, time(hour=1, minute=0, second=0))
                    new_queries.append((query_id, query, frequency_new, 0, 0, updated))
                    max_id += 1
                sum_6 = p_r.get(query_id, 0)
                if not sum_6:
                    dates_list = [current_date - timedelta(days=i) for i in range(7)]
                    avg_f = round(frequency_new / 7)
                    for d in dates_list:
                        new_rows.append((query_id, d, avg_f))
                else:
                    new_f = frequency_new - sum_6
                    delimiter = round(frequency_new / 14)
                    delimiter_plus = round((frequency_new / 7) * 2)
                    if new_f <= 0 or new_f <= delimiter:
                        new_f = delimiter
                    if new_f >= delimiter_plus:
                        diff = round((new_f - delimiter_plus) / 7)
                        dates_list = [current_date - timedelta(days=i) for i in range(7)]
                        if diff > 0:
                            for d in dates_list:
                                new_rows.append((query_id, d, diff))
                        new_f = delimiter_plus
                    new_rows.append((query_id, current_date, new_f))
                if len(new_rows) > 20000:
                    logger.info("Запись в БД")
                    await client.insert(
                        table="request_frequency_very_new",
                        column_names=["query_id", "date", "frequency"],
                        data=new_rows
                    )
                    new_rows = []
            if new_rows:
                logger.info("Запись в БД")
                await client.insert(
                    table="request_frequency_very_new",
                    column_names=["query_id", "date", "frequency"],
                    data=new_rows
                )
            if new_queries:
                logger.info("Запись в БД квери")
                await client.insert(
                    table="request",
                    column_names=["id", "query", "quantity", "subject_id", "total_products", "updated"],
                    data=new_queries
                )
            logger.info(f"DATE FINISHED {current_date}")
            current_date += timedelta(days=1)


async def hot_patch():
    start_date = date.today() - timedelta(days=1)
    stmt = """INSERT INTO radar.supplier_query_aggregates
SELECT
    rp.supplier_id,
    d.date,

    uniqExactState(rp.query) AS uniq_query_state,
    uniqExactState(rp.product) AS uniq_wb_state,

    avgState(rp.place) AS avg_place_state,

    sumStateIf(f.frequency, rp.place <= 100) AS sum_frequency_state,
    avgStateIf(f.frequency, rp.place <= 100) AS avg_frequency_state,

    countIfState(rp.advert = 'b') AS count_b_state,
    countIfState(rp.advert = 'c') AS count_c_state,
    countIfState(rp.advert = 'z') AS count_z_state,

    sumState(rp.cpm) AS sum_cpm_state,
    avgState(rp.cpm) AS avg_cpm_state,

    sumIfState(rp.cpm, rp.advert = 'b') AS sum_cpm_b_state,
    avgIfState(rp.cpm, rp.advert = 'b') AS avg_cpm_b_state,

    sumIfState(rp.cpm, rp.advert = 'c') AS sum_cpm_c_state,
    avgIfState(rp.cpm, rp.advert = 'c') AS avg_cpm_c_state

FROM radar.request_product AS rp
LEFT JOIN (
    SELECT query_id, sum(frequency) AS frequency
    FROM radar.request_frequency
    WHERE date = toDate('%(v1)s')
    GROUP BY query_id
) AS f ON f.query_id = rp.query

JOIN (
    SELECT id, date
    FROM dates
    WHERE date = toDate('%(v1)s')
    LIMIT 1
) AS d ON d.id = rp.date

WHERE rp.date = (
    SELECT id
    FROM dates
    WHERE date = toDate('%(v1)s')
    LIMIT 1
)

GROUP BY
    rp.supplier_id,
    d.date;"""
    async with get_async_connection(send_receive_timeout=3600) as client:
        logger.info(f"DATE {start_date}")
        execute = stmt % {"v1": str(start_date)}
        await client.command(execute)


async def main_task():
    # await write_subjects_raw()
    await hot_patch()
    # await get_today_suppliers_data()


async def main_shit_2():
    async with get_async_connection() as client:
        await client.command("TRUNCATE TABLE request_subject_temp")
        await client.command("TRUNCATE TABLE request_growth_new")
        dates = [i for i in range(1, 224)]
        dates.sort(reverse=True)
        for d in dates:
            logger.info(f"DATE: {d}")
            await client.command(f"""INSERT INTO radar.request_subject_temp (query_id, date, subjects_list)
SELECT
    query       AS query_id,
    date        AS date,
    groupArrayDistinct(subject_id) AS subjects_list
FROM radar.request_product
WHERE date BETWEEN {d} - 30 AND {d} AND place <= 300
GROUP BY query, date;""")


async def main_shit_3():
    async with get_async_connection() as client:
        date_start = date.today() - timedelta(days=2)
        date_end = date(year=2023, month=4, day=29)
        dates = []
        curr = date_start
        stmt = """INSERT INTO request_growth_new
SELECT
    rg.query_id,
    rg.date,
    rg.g30,
    rg.g60,
    rg.g90,
    rg.sum30,
    coalesce(rf.sum60, 0) AS sum60,
    coalesce(rf.sum90, 0) AS sum90,
    rg.subject_id,
    coalesce(rs.subjects_list, []) AS subjects_list,
    now() AS updated
FROM
(
    SELECT 
        query_id,
        date,
        g30,
        g60,
        g90,
        sum30,
        sum60,
        sum90,
        subject_id
    FROM request_growth
    WHERE date = toDate('%(date_start)s')
) AS rg
LEFT JOIN
(
    SELECT
        rst.*,
        d.date AS iso_date
    FROM request_subject_temp AS rst
    JOIN dates AS d
        ON d.id = rst.date
    WHERE rst.date = (
        SELECT id
        FROM dates
        WHERE date = toDate('%(date_start)s')
    )
) AS rs
    ON rs.iso_date = rg.date
   AND rs.query_id = rg.query_id
LEFT JOIN
(
    SELECT
        query_id,
        sumIf(frequency, date >= toDate('%(date_start)s') - 59) AS sum60,
        sum(frequency) AS sum90
    FROM request_frequency
    WHERE query_id IN
    (
        SELECT DISTINCT query_id
        FROM request_growth
        WHERE date = toDate('%(date_start)s')
    )
      AND date BETWEEN toDate('%(date_start)s') - 89 AND toDate('%(date_start)s')
    GROUP BY query_id
) AS rf
    ON rf.query_id = rg.query_id;"""
        while curr >= date_end:
            dates.append(curr)
            curr -= timedelta(days=1)
        for d in dates:
            logger.info(f"DATE: {d}")
            await client.command(stmt % {"date_start": d})


async def main_shit_all():
    await main_shit_2()
    await main_shit_3()


async def new_horrible_shit():
    async with get_async_connection(send_receive_timeout=3600) as client:
        left = 0
        right = 12000000
        batch = 1000000
        for i in range(left, right, batch):
            l = i
            r = i + batch - 1

            sql = f"""WITH
    toStartOfMonth(today()) AS m0, addMonths(m0, -12) as m12
    INSERT INTO request_month_marks(query_id, months_grow, months_fall)
SELECT query_id, groupUniqArrayIf(toMonth(month), res > 0) as months_grow, groupUniqArrayIf(toMonth(month), res < 0) as months_fall from (SELECT query_id,
    month,
    s,
    round(avg_prev3),
    multiIf(
        isNull(avg_prev3) OR avg_prev3 = 0, 0,
        s / avg_prev3 > 1.25, 1,
        s / avg_prev3 < 0.75, -1,
        0
    ) AS res
FROM
(
    SELECT query_id, 
        month,
        s,
        avg(s) OVER (
            ORDER BY query_id, month
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_prev3
    FROM
    (
        SELECT query_id, 
            toStartOfMonth(date) AS month,
            sum(frequency) AS s
        FROM request_frequency
        WHERE query_id BETWEEN {l} AND {r}
          AND month >= addMonths(m0, -15)
          AND month < m0
        GROUP BY query_id, month
    )
) WHERE month >= m12
ORDER BY query_id, month) group by query_id"""
            await client.command(sql)


async def recount_suppliers():
    async with get_async_connection(send_receive_timeout=3600) as client:
        d_stmt = """SELECT DISTINCT date FROM dates ORDER BY date DESC"""
        d_q = await client.query(d_stmt)
        dates = list([row[0] for row in d_q.result_rows])
    for d in dates:
        print(f"COUNTING DATE: {d}")
        await aggregate_supplier(start_date=d)


if __name__ == '__main__':
    # asyncio.run(new_horrible_shit())
    # asyncio.run(recount_oracle())
    # transfer_aggregates.delay()
    fire_requests.delay(1, True)
    # download_wb_report_task.delay()
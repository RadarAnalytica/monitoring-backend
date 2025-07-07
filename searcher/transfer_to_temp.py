import asyncio
import json
import math
from datetime import date, timedelta, datetime

from clickhouse_db.get_async_connection import get_async_connection
from server.funcs.upload_requests_data import recount_growth_by_date
from settings import logger


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

async def main():
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
        
        round(if(qpf2.all_ids > 0, qpf2.revenue_total / qpf2.all_ids, 0)) as avg_revenue_total,
        round(if(qpf2.total_ids_300 > 0, qpf2.revenue_total / qpf2.total_ids_300, 0)) as avg_revenue_300,
        
        qpf2.avg_with_sales as avg_with_sales_revenue,
        
        round(qpf2.revenue_total / 30) as avg_daily_revenue,
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
        qpf2.subjects as subjects_list
        
FROM
(
    SELECT
        qpf.query as q,
        round(avg(if(pd.wb_id_price > 0, pd.wb_id_price, NULL))) AS avg_price_total,
        round(avg(if(qpf.place <= 300 AND pd.wb_id_price > 0, pd.wb_id_price, NULL))) AS avg_price_300,
        round(median(if(pd.wb_id_price > 0, pd.wb_id_price, NULL))) as median_price,
        sum(if(qpf.place <= 30, pd.wb_id_revenue, 0)) AS top_30,
        sum(if(qpf.place <= 100, pd.wb_id_revenue, 0)) AS top_100,
        sum(if(qpf.place <= 300, pd.wb_id_revenue, 0)) AS top_300,
        sum(if(qpf.place <= 300, pd.wb_id_orders, 0)) AS top_300_orders,
        sum(round(pd.wb_id_avg_daily_revenue)) as avg_daily_revenue,
        sum(round(if(qpf.place <= 300, pd.wb_id_avg_daily_revenue, 0))) as avg_daily_revenue_300,
        round(coalesce(avg(if(pd.wb_id_avg_daily_revenue > 0, pd.wb_id_avg_daily_revenue, NULL)))) as adr,
        round(coalesce(avg(if(pd.wb_id_revenue > 0, pd.wb_id_revenue, NULL)), 0)) as avg_with_sales,
        round(coalesce(avg(if(pd.wb_id_avg_daily_revenue > 0, pd.wb_id_avg_daily_revenue, NULL)), 0)) as avg_daily_wb_id_revenue,
        sum(pd.wb_id_revenue) AS revenue_total,
        sum(pd.wb_id_orders) AS orders_total,
        round(sum(if(qpf.place <= 300, pd.wb_id_lost_revenue, 0))) AS lost_revenue_300,
        round(sum(pd.wb_id_lost_revenue)) AS lost_revenue_total,
        round(sum(pd.wb_id_lost_orders)) AS lost_orders,
        sum(if(qpf.place <= 100, qpf.advert, 0)) AS advert,
        sum(if(qpf.place <= 100, rex.ex, 0)) AS ex_advert,
        round(coalesce(avg(if(pd.ratio > 0, pd.ratio, NULL)), 0)) AS ratio,
        round(coalesce(avg(if(pd.rating > 0, pd.rating, NULL)), 0), 1) AS rating,
        round(coalesce(avg(if(pd.root_feedbacks > 0, pd.root_feedbacks, NULL)), 0)) AS reviews,
        round(coalesce(avg(if(pd.wb_id_potential_revenue > 0, pd.wb_id_potential_revenue, NULL)), 0)) AS potential_revenue,
        round(coalesce(avg(if(pd.wb_id_potential_orders > 0, pd.wb_id_potential_orders, NULL)), 0)) AS potential_orders,
        round(sum(pd.wb_id_orders) / sum(pd.root_feedbacks), 1) AS opr,
        any(pd.sub_id) as subject_id,
        groupUniqArray(pd.sub_id) AS subjects,
        groupUniqArray(pd.supl_id) AS suppliers,
        groupUniqArray(pd.b_id) AS brands,
        sum(if(pd.wb_id_revenue > 0, 1, 0)) as with_sales_ids,
        count() as all_ids,
        sum(if(pd.wb_id_revenue > 0 and qpf.place <= 300, 1, 0)) as with_sales_ids_300,
        sum(if(qpf.place <= 300, 1, 0)) as total_ids_300
    FROM radar.query_product_flat AS qpf
    INNER JOIN
    (
        SELECT
            wb_id,
            wb_id_revenue,
            wb_id_orders,
            wb_id_price,
            wb_id_avg_daily_revenue,
            wb_id_avg_daily_orders,
            wb_id_lost_revenue,
            wb_id_lost_orders,
            wb_id_potential_revenue,
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
            WHERE ((query >= %(v1)s) AND (query <= %(v2)s)) AND (date BETWEEN 133 AND 162)
        )
    ) AS pd ON pd.wb_id = qpf.product
    LEFT OUTER JOIN (select query, 1 as ex from request WHERE match(query, '^[0-9]+$')) as rex on rex.query = toString(pd.wb_id) 
    WHERE ((qpf.query >= 1) AND (qpf.query <= %(v2)s)) AND (qpf.date = 162)
    GROUP BY qpf.query
    ORDER BY qpf.query ASC
) as qpf2 
INNER JOIN
    (
        SELECT
            query,
            countDistinct(product) AS dpc
        FROM radar.query_product_flat
        WHERE ((query >= %(v1)s) AND (query <= %(v2)s)) AND (date BETWEEN 133 AND 162)
        GROUP BY query
    ) AS qpf1 ON qpf2.q = qpf1.query
    INNER JOIN
    (
        SELECT
            query_id,
            sum(if(date >= (yesterday() - 29), frequency, 0)) AS sum_30,
            sum(if(date >= (yesterday() - 59), frequency, 0)) AS sum_60,
            sum(frequency) AS sum_90
        FROM request_frequency
        WHERE ((query_id >= %(v1)s) AND (query_id <= %(v2)s)) AND (date >= (yesterday() - 89))
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
        WHERE ((query_id >= %(v1)s) AND (query_id <= %(v2)s)) AND (date = yesterday() - 1)
    ) AS rg ON rg.query_id = qpf2.q
    INNER JOIN
    (
        SELECT
            id,
            query,
        FROM request
        WHERE ((id >= %(v1)s) AND (id <= %(v2)s))
    ) AS r ON r.id = qpf2.q
    INNER JOIN
    (
        SELECT
            query,
            total_products
        FROM query_history
        WHERE ((query >= %(v1)s) AND (query <= %(v2)s)) AND date = yesterday() - 1
    ) AS qh ON qh.query = qpf2.q
WHERE qpf2.ratio > 0
"""
    left = 8524676
    right = 10300000
    step = 100000
    async with get_async_connection(send_receive_timeout=3600) as client:
        for i in range(left, right, step):
            logger.info(f"batch {i}")
            params = {
                "v1": i,
                "v2": i + step - 1
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
                rating = evaluate_niche(demand_coef=freq_per_good, monopoly_pct=monopoly_percent, advert_pct=advert_percent, buyout_pct=buyout_percent, revenue=revenue_300 / 100)
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
                ))

            await client.insert(
                table="monitoring_oracle_new",
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
                ],
                data=data
            )

async def migrate_monitoring_oracle_data():
    async with get_async_connection(send_receive_timeout=3600) as client:
        # Получаем все строки
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


async def form_lost_table():
    min_date = date(year=2023, month=1, day=26)
    max_date = date(year=2025, month=3, day=18)
    current_date = min_date
    async with get_async_connection(send_receive_timeout=3600) as client:
        while current_date <= max_date:
            logger.info(str(current_date))
            stmt = f"""INSERT INTO request_frequency_temp
            SELECT 
                r.query, 
                max(rf.date) as date, 
                sum(rf.frequency) as frequency 
            FROM 
                request_frequency rf 
            JOIN 
                request r 
            ON 
                rf.query_id = r.id
            WHERE rf.date BETWEEN toDate('{min_date}') - 6 AND toDate({min_date})
            GROUP BY r.query
            HAVING frequency >= 45 AND date = '{min_date}'
            """
            await client.command(stmt)
            current_date += timedelta(days=1)


# Запуск
if __name__ == '__main__':
    asyncio.run(form_lost_table())
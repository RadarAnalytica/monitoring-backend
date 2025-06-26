import asyncio
import json
import math
from datetime import date, timedelta, datetime

from clickhouse_db.get_async_connection import get_async_connection
from server.funcs.upload_requests_data import recount_growth_by_date
from settings import logger


def get_score(value, thresholds):
    for i, (low, high, score) in enumerate(thresholds):
        if low <= value < high:
            return score
    return thresholds[-1][2]

def evaluate_niche(demand_coef, monopoly_pct, advert_pct, buyout_pct, revenue):
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
        (1_000_000, float('inf'), 4),
        (600_000, 1_000_000, 3),
        (200_000, 600_000, 2),
        (0, 200_000, 1),
    ]

    scores = [
        get_score(demand_coef, demand_thresholds),
        get_score(monopoly_pct, monopoly_thresholds),
        get_score(advert_pct, advert_thresholds),
        get_score(buyout_pct, buyout_thresholds),
        get_score(revenue, revenue_thresholds),
    ]

    avg_score = sum(scores) / len(scores)
    difficulty_level = round(avg_score)

    return difficulty_level

async def main():
    async with get_async_connection() as client:
        stmt = """SELECT r.id, qh.priority, qh.total_products, r.query FROM request r JOIN (select * from query_history where date = (select max(date) from query_history)) qh ON qh.query = r.id WHERE r.updated = (SELECT max(updated) FROM request) AND r.quantity >= 200"""
        q = await client.query(stmt)
        queries = list(q.result_rows)
        result = []
        start_date = date(year=2025, month=5, day=25)
        end_date = date(year=2025, month=6, day=24)
        counter = 0
        for i, subject, total_products, query in queries:
            start = datetime.now()
            counter += 1
            query_id = i
            subject_id = subject
            freq_stmt = f"""SELECT sum(if(date >= toDate('{end_date}') - 29, frequency, 0)) AS sum_30, sum(if(date >= toDate('{end_date}') - 59, frequency, 0)) AS sum_60, sum(if(date >= toDate('{end_date}') - 89, frequency, 0)) AS sum_90 FROM request_frequency WHERE query_id = {i} AND date BETWEEN toDate('{end_date}') - 89 AND toDate('{end_date}')"""
            f = await client.query(freq_stmt)
            f_result = list(f.result_rows)[0]
            print(f"Достаём частотность: {(datetime.now() - start).total_seconds()}")
            frequency_30 = f_result[0]
            frequency_60 = f_result[1]
            frequency_90 = f_result[2]
            grow_stmt = f"""SELECT sum(g30), sum(g60), sum(g90) from request_growth where query_id = {query_id} and date = '{end_date}'"""
            g = await client.query(grow_stmt)
            print(f"Достаём рост: {(datetime.now() - start).total_seconds()}")
            g_result = list(g.result_rows)[0] if g.result_rows and g.result_rows[0] else (0, 0, 0)
            g30 = g_result[0]
            g60 = g_result[1]
            g90 = g_result[2]
            qp_stmt = f"""
            SELECT
                groupUniqArrayMerge(products_30)     AS products_30,
                groupUniqArrayMerge(products_100)    AS products_100,
                groupUniqArrayMerge(products_300)    AS products_300,
                countDistinctMerge(advert_b_count)   AS advert_b_count,
                countDistinctMerge(advert_c_count)   AS advert_c_count,
                uniqExactMerge(products_top_count)   AS products_top_count
            FROM radar.query_products_daily
            WHERE query = {i} AND date BETWEEN 126 AND 155
            """
            qp = await client.query(qp_stmt)
            qp_result = list(qp.result_rows)[0] if qp.result_rows and qp.result_rows[0] else ([], [], [], 0, 0, 0)
            print(f"Достаём топы по кверям: {(datetime.now() - start).total_seconds()}")
            (top_30,
            top_100,
            top_300,
            advert_b_count,
            advert_c_count,
            top_1200_count) = qp_result
            if not any((top_30, top_100, top_300)):
                continue
            advert_total = advert_c_count + advert_b_count
            if advert_total > len(top_100):
                advert_total = len(top_100)
            external_params = {
                "v1": tuple(str(wb_id) for wb_id in top_100),
            }
            external_stmt = """SELECT count() FROM request WHERE query IN %(v1)s"""
            external_q = await client.query(external_stmt, parameters=external_params)
            external_result = external_q.result_rows[0][0] if external_q.result_rows and external_q.result_rows[0] else 0
            top_30_params = {
                "v1": top_30,
                "v2": start_date,
                "v3": end_date
            }
            print(f"Достаём external: {(datetime.now() - start).total_seconds()}")
            top_30_stmt = """select sum(price * orders) / 100 from product_data where wb_id in %(v1)s and date between %(v2)s and %(v3)s"""
            top_30_q = await client.query(top_30_stmt, parameters=top_30_params)
            top_30_revenue = top_30_q.result_rows[0][0] if top_30_q.result_rows and top_30_q.result_rows[0] else 0
            top_100_params = {
                "v1": top_100,
                "v2": start_date,
                "v3": end_date
            }
            print(f"Достаём выручку 30: {(datetime.now() - start).total_seconds()}")
            top_100_stmt = """select sum(price * orders) / 100 from product_data where wb_id in %(v1)s and date between %(v2)s and %(v3)s"""
            top_100_q = await client.query(top_100_stmt, parameters=top_100_params)
            top_100_revenue = top_100_q.result_rows[0][0] if top_100_q.result_rows and top_100_q.result_rows[0] else 0
            print(f"Достаём выручку 100: {(datetime.now() - start).total_seconds()}")
            top_300_params = {
                "v1": top_300,
                "v2": start_date,
                "v3": end_date
            }
            top_300_stmt = """SELECT 
    avg(pdd.id_prc) AS avg_price,
    sum(pdd.id_rev) AS full_revenue,
    avg(pdd.id_rev) AS avg_id_rev,
    sum(pdd.id_ord) AS full_orders,
    sum(pdd.id_lost_rev) AS lost_rev,
    sum(pdd.id_lost_ord) AS lost_ord,
    median(pdd.id_prc) AS median_price,
    groupUniqArray(pdd.sup) AS suppliers,
    groupUniqArray(pdd.sub) AS subjects,
    groupUniqArray(pdd.br) AS brands,
    coalesce(avg(if(pm.feedbacks > 0, pm.feedbacks, NULL)), 0) AS feedback,
    coalesce(avg(if(pm.rating > 0, pm.rating, NULL)), 0) AS ratings,
    coalesce(avg(if(pdd.potential_revenue > 0, pdd.potential_revenue, NULL)), 0) AS avg_potential_revenue,
    coalesce(avg(if(pdd.potential_orders > 0, pdd.potential_orders, NULL)), 0) AS avg_potential_orders,
    avg(round(pdd.id_day_rev, 0)) AS avg_daily_rev,
    avg(if(pm.period_feedbacks > 0, pdd.id_ord / pm.period_feedbacks, 0)) AS orders_per_feedback
FROM (
    SELECT 
        wb_id,
        avg(prc) AS id_prc,
        max(supplier) AS sup,
        max(subject) AS sub,
        max(brand) AS br,
        sum(rev) AS id_rev,
        sum(ord) AS id_ord,
        sum(avg_day_rev) * 30 AS potential_revenue,
        sum(avg_day_ord) * 30 AS potential_orders,
        sum(lost_rev) AS id_lost_rev,
        sum(lost_ord) AS id_lost_ord,
        sum(avg_day_rev) AS id_day_rev
    FROM (
        SELECT
            pd.wb_id,
            pd.size,
            max(pd.supplier_id) AS supplier,
            max(pd.subject_id) AS subject,
            max(pd.brand_id) AS brand,
            sum(pd.revenue) AS rev,
            sum(pd.a_orders) AS ord,
            avg(pd.a_price) AS prc,
            coalesce(avgIf(pd.revenue, toUInt8(pd.full_day = 1 AND pd.revenue > 0)), 0) AS avg_day_rev,
            coalesce(avgIf(pd.a_orders, toUInt8(pd.full_day = 1 AND pd.a_orders > 0)), 0) AS avg_day_ord,
            sum(pd.zero_day) * avgIf(pd.revenue, toUInt8(pd.full_day = 1)) AS lost_rev,
            sum(pd.zero_day) * avgIf(pd.a_orders, toUInt8(pd.full_day = 1)) AS lost_ord
        FROM (
            SELECT 
                wb_id,
                date,
                size,
                max(supplier_id) AS supplier_id,
                max(subject_id) AS subject_id,
                max(brand_id) AS brand_id,
                sum(price * orders) / 100 AS revenue,
                sum(orders) AS a_orders,
                sum(quantity) AS a_quantity,
                avg(price) AS a_price,
                if(sum(quantity) = 0, 1, 0) AS zero_day,
                if(sum(quantity) = 0, 0, 1) AS full_day
            FROM product_data
            WHERE 
                wb_id IN %(v1)s
                AND date BETWEEN %(v2)s AND %(v3)s
                AND price > 0
            GROUP BY wb_id, date, size
        ) AS pd
        GROUP BY pd.wb_id, pd.size
    ) AS pdd
    GROUP BY wb_id
) AS pdd
LEFT OUTER JOIN (
    SELECT 
        wb_id,
        max(rating) AS rating,
        max(root_feedbacks) AS feedbacks,
        max(root_feedbacks) - min(root_feedbacks) AS period_feedbacks
    FROM product_meta
    WHERE 
        date BETWEEN %(v2)s AND %(v3)s
        AND wb_id IN %(v1)s
    GROUP BY wb_id
) AS pm ON pm.wb_id = pdd.wb_id"""
            top_300_q = await client.query(top_300_stmt, parameters=top_300_params)
            top_300_res = list(top_300_q.result_rows)
            print(f"Достаём пиздец: {(datetime.now() - start).total_seconds()}")
            top_300_res = top_300_res[0] if top_300_res and top_300_res[0] else [0 for _ in range(16)]
            (
                avg_price,
                full_revenue,
                avg_id_rev,
                full_orders,
                lost_rev,
                lost_ord,
                median_price,
                suppliers,
                subjects,
                brands,
                feedback,
                ratings,
                avg_potential_revenue,
                avg_potential_orders,
                avg_daily_rev,
                orders_per_feedback
             ) = top_300_res
            revenue = round(full_revenue)
            lost_revenue = round(lost_rev) if not math.isnan(lost_rev) else 0
            potential_revenue = round(avg_potential_revenue)
            avg_revenue = round(avg_id_rev)
            avg_with_sales_revenue = round(avg_id_rev)
            avg_daily_revenue = round(avg_daily_rev) if not math.isnan(avg_daily_rev) else round(avg_potential_revenue / 30)
            lost_revenue_percent = round(lost_revenue * 100 / full_revenue) if full_revenue else 0
            monopoly_percent = round(top_30_revenue * 100 / top_100_revenue) if top_100_revenue else 0
            orders = full_orders
            lost_orders = lost_ord if not math.isnan(lost_ord) else 0
            lost_orders_percent = round(lost_orders * 100 / full_orders) if full_orders else 0
            potential_orders = round(avg_potential_orders)
            avg_price = round(avg_price)
            median_price = round(median_price)
            advert_percent = round(advert_total * 100 / len(top_100)) if top_100 else 0
            external_advert_percent = round(external_result * 100 / len(top_100)) if top_100 else 0
            goods_quantity = total_products
            top_goods_quantity = top_1200_count
            freq_per_good = round(frequency_30 / total_products, 1)
            goods_with_sales_quantity = len(top_300)
            goods_with_sales_percent = 100
            suppliers_with_sales_percent = 100
            suppliers = suppliers or []
            suppliers_quantity = len(suppliers)
            avg_rating = round(ratings, 1)
            brands_list = brands or []
            suppliers_list = suppliers or []
            subjects_list = subjects or []
            avg_reviews = round(feedback)
            order_per_review = round(orders_per_feedback)
            buyout_stmt = """SELECT coalesce(avg(if(ratio > 0, ratio, NULL)), 0) FROM supplier_history WHERE id IN %(v1)s AND date = %(v2)s"""
            buyout_params = {
                "v1": suppliers_list,
                "v2": end_date
            }
            buyout_q = await client.query(buyout_stmt, parameters=buyout_params)
            print(f"Достаём выкуп: {(datetime.now() - start).total_seconds()}")
            buyout_percent = buyout_q.result_rows[0][0] if buyout_q.result_rows and buyout_q.result_rows[0] else 0
            buyout_percent = buyout_percent if not math.isnan(buyout_percent) else 0
            fbo_commission = 0
            fbs_commission = 0
            dbs_commission = 0
            dbs_express_commission = 0
            rating = evaluate_niche(demand_coef=freq_per_good, monopoly_pct=monopoly_percent, advert_pct=advert_percent, buyout_pct=buyout_percent, revenue=revenue)
            i_res = {
                "query": query,
                "rating": rating,
                "subject_id": subject_id,
                "frequency_30": frequency_30,
                "frequency_60": frequency_60,
                "frequency_90": frequency_90,
                "g30": g30,
                "g60": g60,
                "g90": g90,
                "revenue": revenue,
                "lost_revenue": lost_revenue,
                "potential_revenue": potential_revenue,
                "avg_revenue": avg_revenue,
                "avg_with_sales_revenue": avg_with_sales_revenue,
                "avg_daily_revenue": avg_daily_revenue,
                "lost_revenue_percent": lost_revenue_percent,
                "monopoly_percent": monopoly_percent,
                "orders": orders,
                "lost_orders": lost_orders,
                "lost_orders_percent": lost_orders_percent,
                "potential_orders": potential_orders,
                "avg_price": avg_price,
                "median_price": median_price,
                "advert_percent": advert_percent,
                "external_advert_percent": external_advert_percent,
                "goods_quantity": goods_quantity,
                "top_goods_quantity": top_goods_quantity,
                "freq_per_good": freq_per_good,
                "goods_with_sales_quantity": goods_with_sales_quantity,
                "goods_with_sales_percent": goods_with_sales_percent,
                "suppliers_with_sales_percent": suppliers_with_sales_percent,
                "suppliers_quantity": suppliers_quantity,
                "avg_rating": avg_rating,
                "avg_reviews": avg_reviews,
                "buyout_percent": buyout_percent,
                "fbo_commision": fbo_commission,
                "fbs_commision": fbs_commission,
                "dbs_commision": dbs_commission,
                "dbs_express_commision": dbs_express_commission,
                "brands_list": brands_list,
                "subjects_list": subjects_list,
                "order_per_review": order_per_review,
            }
            logger.info(json.dumps(i_res, indent=2, ensure_ascii=False))
            if counter == 10:
                break

if __name__ == '__main__':
    asyncio.run(main())
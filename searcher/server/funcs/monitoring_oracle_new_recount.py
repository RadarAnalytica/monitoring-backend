from datetime import date, timedelta
from collections import defaultdict, Counter
from clickhouse_db.get_async_connection import get_async_connection
from service.log_alert import send_log_message
from settings import logger


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


async def transfer_aggregates_to_local_v2():
    """
    Агрегирует данные из product_data_id_final_v2 в wb_id_extended_local.
    - Батчи по 50000 товаров (по диапазону wb_id)
    - Двухэтапная агрегация: дедупликация по (wb_id, date), затем суммирование
    - Средние считаются по фактическому кол-ву дней с данными
    - Потенциальные значения = avg * 30
    - Staging-таблица со swap через RENAME
    - ratio из supplier_history
    - Прямой INSERT INTO ... SELECT без выгрузки в Python
    """
    await send_log_message(message="Начинается агрегация wb_id_extended_local из product_data_id_final_v2")
    
    end_date = date.today() - timedelta(days=1)  # вчера
    start_date = end_date - timedelta(days=29)   # 30 дней назад
    batch_size = 50_000

    insert_query = """
INSERT INTO wb_id_extended_local_stage (
    wb_id, wb_id_revenue, wb_id_orders, wb_id_price,
    wb_id_avg_daily_revenue, wb_id_avg_daily_orders,
    wb_id_lost_revenue, wb_id_lost_orders,
    wb_id_potential_revenue, wb_id_potential_orders,
    sub_id, supl_id, b_id, rating, root_feedbacks,
    ratio, wb_id_price_spp, wb_id_revenue_spp
)
WITH 
-- Этап 1: Дедупликация по (wb_id, date)
deduped AS (
    SELECT
        wb_id, date,
        argMax(revenue_total, updated) AS revenue_total,
        argMax(orders_total, updated) AS orders_total,
        argMax(lost_revenue, updated) AS lost_revenue,
        argMax(lost_orders, updated) AS lost_orders,
        argMax(revenue_total_spp, updated) AS revenue_total_spp,
        argMax(price, updated) AS price,
        argMax(price_spp, updated) AS price_spp,
        argMax(subject_id, updated) AS subject_id,
        argMax(supplier_id, updated) AS supplier_id,
        argMax(brand_id, updated) AS brand_id,
        argMax(rating, updated) AS rating,
        argMax(root_feedbacks, updated) AS root_feedbacks
    FROM product_data_id_final_v2
    WHERE wb_id BETWEEN %(batch_start)s AND %(batch_end)s
      AND date BETWEEN %(start_date)s AND %(end_date)s
    GROUP BY wb_id, date
),

-- Этап 2: Агрегация по датам с подсчетом дней
aggregated AS (
    SELECT
        wb_id,
        SUM(revenue_total) AS wb_id_revenue,
        SUM(orders_total) AS wb_id_orders,
        SUM(lost_revenue) AS wb_id_lost_revenue,
        SUM(lost_orders) AS wb_id_lost_orders,
        SUM(revenue_total_spp) AS wb_id_revenue_spp,
        count() AS days_count,
        argMax(price, date) AS wb_id_price,
        argMax(price_spp, date) AS wb_id_price_spp,
        argMax(subject_id, date) AS sub_id,
        argMax(supplier_id, date) AS supl_id,
        argMax(brand_id, date) AS b_id,
        argMax(rating, date) AS rating,
        argMax(root_feedbacks, date) AS root_feedbacks
    FROM deduped
    GROUP BY wb_id
),

-- Уникальные supplier_id для JOIN
supplier_ids AS (
    SELECT DISTINCT supl_id FROM aggregated
)

SELECT
    a.wb_id,
    a.wb_id_revenue,
    a.wb_id_orders,
    a.wb_id_price,
    a.wb_id_revenue / a.days_count AS wb_id_avg_daily_revenue,
    a.wb_id_orders / a.days_count AS wb_id_avg_daily_orders,
    a.wb_id_lost_revenue,
    a.wb_id_lost_orders,
    (a.wb_id_revenue / a.days_count) * 30 AS wb_id_potential_revenue,
    (a.wb_id_orders / a.days_count) * 30 AS wb_id_potential_orders,
    a.sub_id,
    a.supl_id,
    a.b_id,
    a.rating,
    a.root_feedbacks,
    coalesce(sh.ratio, 0) AS ratio,
    a.wb_id_price_spp,
    a.wb_id_revenue_spp
FROM aggregated a
LEFT JOIN (
    SELECT id, argMax(ratio, date) AS ratio
    FROM supplier_history
    WHERE id IN (SELECT supl_id FROM supplier_ids)
      AND date BETWEEN %(start_date)s AND %(end_date)s
    GROUP BY id
) sh ON sh.id = a.supl_id
"""

    async with get_async_connection(send_receive_timeout=3600) as client:
        # Очистить staging таблицу
        await client.command("TRUNCATE TABLE wb_id_extended_local_stage")
        
        # Получить max wb_id
        max_result = await client.query(
            f"SELECT max(wb_id) FROM product_data_id_final_v2 WHERE date = '{end_date}'"
        )
        max_wb_id = max_result.result_rows[0][0] if max_result.result_rows else 0
        
        if not max_wb_id:
            logger.warning("Нет данных в product_data_id_final_v2 за указанный период")
            await send_log_message(message="Агрегация прервана: нет данных")
            return
        
        logger.info(f"Начинаем агрегацию: {start_date} - {end_date}, max_wb_id={max_wb_id}")
        
        # Итерация по батчам с прямым INSERT INTO ... SELECT
        for batch_start in range(0, max_wb_id + 1, batch_size):
            batch_end = batch_start + batch_size - 1
            logger.info(f"Обработка батча {batch_start} - {batch_end}")
            
            params = {
                "batch_start": batch_start,
                "batch_end": batch_end,
                "start_date": start_date,
                "end_date": end_date
            }
            
            await client.command(insert_query, parameters=params)
            logger.info(f"Батч {batch_start}-{batch_end}: вставлено")
        
        # Swap таблиц через RENAME
        logger.info("Выполняем swap таблиц")
        await client.command("RENAME TABLE wb_id_extended_local TO wb_id_extended_local_old")
        await client.command("RENAME TABLE wb_id_extended_local_stage TO wb_id_extended_local")
        await client.command("RENAME TABLE wb_id_extended_local_old TO wb_id_extended_local_stage")
        await client.command("TRUNCATE TABLE wb_id_extended_local_stage")
        
    await send_log_message(message="Агрегация wb_id_extended_local завершена")


async def recount_oracle_v2():
    logger.info("начинаем recount_oracle_v2")
    
    stmt_data = """
    SELECT
        qpf2.q as query_id,
        r.query as query,
        qpf2.subject_id as subject_id,

        rg.sum_30 AS frequency_30,
        rg.sum_60 AS frequency_60,
        rg.sum_90 AS frequency_90,
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
        round(if(qpf2.orders_total > 0, qpf2.lost_orders * 100 / qpf2.orders_total, 0)) as lost_orders_percent,


        qpf2.avg_price_total as avg_price_total,
        qpf2.avg_price_300 as avg_price_300,

        qpf2.median_price as median_price,
        qpf2.advert as advert_percent,
        qpf2.ex_advert as external_advert_percent,
        qh.total_products AS goods_quantity,
        qpf1.dpc as top_goods_quantity,        
        round(if(qpf1.total_products > 0, rg.sum_30 / qh.total_products, 0), 3) AS freq_per_good,

        round(if(qpf2.all_ids > 0, qpf2.with_sales_ids * 100 / qpf2.all_ids, 0)) as goods_with_sales_percent_total,
        qpf2.with_sales_ids as goods_with_sales_quantity_total,

        round(if(qpf2.total_ids_300 > 0, qpf2.with_sales_ids_300 * 100 / qpf2.total_ids_300, 0)) as goods_with_sales_percent_300,
        qpf2.total_ids_300 as goods_with_sales_quantity_300,

        length(qpf2.suppliers) as suppliers_quantity,


        qpf2.reviews as avg_reviews,
        qpf2.rating as avg_rating,

        round(if(sum(pd.root_feedbacks) > 0, sum(pd.wb_id_orders) / sum(pd.root_feedbacks), 0), 1) AS order_per_review,
        qpf2.ratio as buyout_percent,

        qpf2.brands as brands_list,
        qpf2.subjects as subjects_list,
        qpf2.suppler_wb_id_revenue as suppler_revenue,

        qpf2.revenue_total_spp as revenue_total_spp,

        qpf2.top_300_spp as revenue_300_spp,

        round(if(qpf2.total_ids_300 > 0, qpf2.top_300_spp / qpf2.total_ids_300, 0), -2) as avg_revenue_300_spp,

        qpf2.avg_price_total_spp as avg_price_total_spp,

        qpf2.avg_price_300_spp as avg_price_300_spp,
        
        coalesce(rmm.months_grow, []) as months_grow,
        
        coalesce(rmm.months_fall, []) as months_fall


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
            
            any(pd.sub_id) as subject_id,
            groupArrayIf(pd.sub_id, qpf.place <= 300) AS subjects,
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
            ORDER BY updated DESC
            LIMIT 1 BY wb_id
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
            g30,
            g60,
            g90,
            sum30 as sum_30,
            sum60 as sum_60,
            sum90 as sum_90
        FROM request_growth
        WHERE query_id IN (
            SELECT DISTINCT query 
            FROM query_product_flat 
            WHERE date = (SELECT max(id) FROM dates) 
            AND query BETWEEN %(v1)s AND %(v2)s
        ) AND (date = yesterday() - 1)
        LIMIT 1 BY query_id
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
        LIMIT 1 BY id
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
        LIMIT 1 BY query
    ) AS qh ON qh.query = qpf2.q
    LEFT OUTER JOIN (SELECT * FROM request_month_marks WHERE query_id BETWEEN %(v1)s AND %(v2)s LIMIT 1 BY query_id) rmm ON rmm.query_id = qpf2.q
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
        await client.command("TRUNCATE TABLE monitoring_oracle_stage")
        await client.command("OPTIMIZE TABLE request_month_marks FINAL")
        
        # Note: Hardcoded subject_id list kept as requested
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
            q = await client.query(stmt_data, parameters=params)
            data = []
            for row in q.result_rows:
                if any([r is None for r in row]):
                    continue
                    
                # Mapping row data to variables
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
                avg_revenue_300 = row[16] or 0
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
                goods_with_sales_percent_total = int(row[35] if row[35] <= 100 else 100)
                
                if goods_with_sales_percent_total <= 0:
                    continue
                    
                goods_with_sales_quantity_total = row[36]
                goods_with_sales_percent_300 = int(row[37] if row[37] <= 100 else 100)
                
                if goods_with_sales_percent_300 <= 0:
                    continue
                    
                goods_with_sales_quantity_300 = row[38]
                suppliers_quantity = row[39]
                avg_reviews = row[40]
                avg_rating = row[41]
                order_per_review = row[42]
                buyout_percent = row[43]
                brands_list = row[44]
                subjects_list = row[45]
                
                # Subjects filtering logic
                total_subjects = len(subjects_list)
                counted_subjects = Counter(subjects_list)
                subjects_set = set(subjects_list)
                if len(subjects_set) == 1:
                    filtered_subjects = list(subjects_set)
                elif not subjects_set:
                    continue
                else:
                    filtered_subjects = [x for x in subjects_set if counted_subjects[x] / total_subjects >= 0.10]
                    filtered_subjects.sort()
                
                # Supplier revenue logic
                supplier_revenue_list = row[46]
                revenue_total_spp = row[47]
                revenue_300_spp = row[48]
                avg_revenue_300_spp = row[49] or 0 # Fix possible None
                avg_price_total_spp = row[50]
                avg_price_300_spp = row[51]
                months_grow = row[52]
                months_fall = row[53]
                
                suppliers_dict = defaultdict(int)
                for s_id_revenue in supplier_revenue_list:
                    s_id = s_id_revenue[0]
                    s_revenue = s_id_revenue[1]
                    suppliers_dict[s_id] += s_revenue
                
                supplier_revenue_values = list(suppliers_dict.values())
                supplier_revenue_values.sort(reverse=True)
                
                if supplier_revenue_values and len(supplier_revenue_values) > 0:
                    top_supplier_revenue = supplier_revenue_values[0]
                    zero_sales_suppliers = sum([1 for sr in supplier_revenue_values if not sr])
                    suppliers_with_sales_percent = round(
                        (len(supplier_revenue_values) - zero_sales_suppliers) * 100 / len(supplier_revenue_values))
                    all_suppliers_revenue = sum(supplier_revenue_values)
                    monopoly_percent = abs(
                        round(top_supplier_revenue * 100 / all_suppliers_revenue)) if all_suppliers_revenue else 0
                else:
                    suppliers_with_sales_percent = 0
                    monopoly_percent = 0
                
                rating, competition_level = evaluate_niche(
                    demand_coef=freq_per_good, 
                    monopoly_pct=monopoly_percent,
                    advert_pct=advert_percent, 
                    buyout_pct=buyout_percent,
                    revenue=revenue_300 / 100
                )
                
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
                    filtered_subjects,
                    rating,
                    competition_level,
                    suppliers_with_sales_percent,
                    revenue_total_spp,
                    revenue_300_spp,
                    avg_revenue_300_spp,
                    avg_price_total_spp,
                    avg_price_300_spp,
                    months_grow,
                    months_fall
                ))
            
            # Note: Keeping print as per legacy code, but standardizing to logger might be better
            print(len(data)) 
            
            await client.insert(
                table="monitoring_oracle_stage",
                column_names=[
                    "query_id", "query", "subject_id",
                    "frequency_30", "frequency_60", "frequency_90",
                    "g30", "g60", "g90",
                    "revenue_total", "revenue_300",
                    "lost_revenue_total", "lost_revenue_300",
                    "potential_revenue", "potential_orders",
                    "avg_revenue_total", "avg_revenue_300",
                    "avg_with_sales_revenue",
                    "avg_daily_revenue", "avg_daily_wb_id_revenue",
                    "lost_revenue_percent_total", "lost_revenue_percent_300",
                    "monopoly_percent",
                    "orders_total", "orders_300",
                    "lost_orders", "lost_orders_percent",
                    "avg_price_total", "avg_price_300", "median_price",
                    "advert_percent", "external_advert_percent",
                    "goods_quantity", "top_goods_quantity", "freq_per_good",
                    "goods_with_sales_percent_total", "goods_with_sales_quantity_total",
                    "goods_with_sales_percent_300", "goods_with_sales_quantity_300",
                    "suppliers_quantity",
                    "avg_reviews", "avg_rating", "order_per_review", "buyout_percent",
                    "brands_list", "subjects_list",
                    "niche_rating", "competition_level",
                    "suppliers_with_sales_percent",
                    "revenue_total_spp", "revenue_300_spp", "avg_revenue_300_spp",
                    "avg_price_total_spp", "avg_price_300_spp",
                    "months_grow", "months_fall"
                ],
                data=data
            )
            
        await client.command("RENAME TABLE monitoring_oracle_new_2 TO monitoring_oracle_old")
        await client.command("RENAME TABLE monitoring_oracle_stage TO monitoring_oracle_new_2")
        await client.command("RENAME TABLE monitoring_oracle_old TO monitoring_oracle_stage")
        await client.command("TRUNCATE TABLE monitoring_oracle_stage")
        
        # Hardcoded subjects for deletion
        await client.command("""delete from monitoring_oracle_new_2 where hasAny(subjects_list, [
            6380, 6383, 6381, 6375, 6379, 6376, 6378, 3030, 6373, 3032,
            6377, 6387, 6384, 6374, 6385, 6382, 3038, 6388, 3039, 8970,
            3042, 6386, 6389, 3044, 3045
            ])""")
        await client.command("DELETE FROM monitoring_oracle_new_2 WHERE lengthUTF8(query) < 3")

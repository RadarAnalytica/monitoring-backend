from datetime import date
from clickhouse_db.get_async_connection import get_async_connection

async def get_product_request_data(product_id: int, date_from: date, date_to: date):
    stmt_dates = """SELECT min(id), min(date), max(id), max(date) FROM dates WHERE date BETWEEN %(v1)s AND %(v2)s"""
    dates_params = {
        "v1": date_from,
        "v2": date_to
    }
    stmt_main = """SELECT
        d.date,
        COUNT(DISTINCT rp.query),
        round(AVG(rp.place), 0),
        MAX(if(rp.advert = 'b', rp.advert, NULL)),
        MAX(if(rp.advert = 'c', rp.advert, NULL)),
        round(MAX(coalesce(rf.avg_freq, 0.0)), 0),
        MAX(coalesce(rf_id.id_fr, 0)),
        MAX(coalesce(pts.pts_freq, 0))
    FROM
        request_product AS rp
    JOIN dates AS d ON d.id = rp.date
    LEFT OUTER JOIN (
        SELECT
            fdate,
            round(AVG(sumfr), 0) AS avg_freq
        FROM
            (
                SELECT
                    rf1.date AS fdate,
                    p.preset AS prst,
                    SUM(rf1.frequency) AS sumfr
                FROM
                    request_frequency AS rf1
                JOIN preset AS p ON p.query = rf1.query_id
                WHERE
                    p.query IN (
                        SELECT
                            DISTINCT query
                        FROM
                            request_product
                        WHERE
                            city = 1
                            AND date BETWEEN %(v2)s AND %(v3)s
                            AND product = %(v1)s
                    )
                AND rf1.date BETWEEN %(v4)s AND %(v5)s
                AND rf1.frequency > 0 
                GROUP BY
                    rf1.date,
                    p.preset
                ORDER BY
                    rf1.date
            )
        GROUP BY
            fdate
        ORDER BY
            fdate
    ) AS rf ON rf.fdate = d.date
    LEFT OUTER JOIN (
        SELECT 
            date,
            sum(frequency) AS id_fr
        FROM request_frequency
        WHERE query_id = (
            SELECT 
                id
            FROM 
                request
            WHERE 
                query = %(v6)s
            LIMIT  1
        )
        AND date BETWEEN %(v4)s AND %(v5)s
        GROUP  BY date
        ORDER  BY date DESC 
    ) as rf_id ON rf_id.date = d.date
    LEFT OUTER JOIN (
        SELECT 
            date, 
            sum(frequency) as pts_freq
        FROM request_frequency 
        WHERE 
            query_id IN (
                SELECT DISTINCT query 
                FROM request_product 
                WHERE city = 1 
                AND date BETWEEN %(v2)s AND %(v3)s  
                AND product = %(v1)s 
                AND place >= 100
            ) 
        AND date BETWEEN %(v4)s AND %(v5)s
        GROUP BY date 
        ORDER BY date
    ) as pts ON pts.date = d.date
    WHERE
        rp.city = 1
        AND rp.date BETWEEN %(v2)s AND %(v3)s
        AND rp.product = %(v1)s
    GROUP BY
        d.date
    ORDER BY
        d.date"""
    result = dict()
    async with get_async_connection() as client:
        dates_query = await client.query(stmt_dates, parameters=dates_params)
        min_date_id, min_date, max_date_id, max_date = dates_query.result_rows[0] if dates_query.result_rows else (None, None, None, None)
        if not any((min_date_id, max_date_id, min_date, max_date)):
            return result
        main_params = {
            "v1": product_id,
            "v2": min_date_id,
            "v3": max_date_id,
            "v4": min_date,
            "v5": max_date,
            "v6": str(product_id)
        }
        main_query = await client.query(stmt_main, parameters=main_params)
        result = {
            str(row[0]): {
                "queries_count": row[1],
                "avg_place": row[2],
                "ad_b": row[3],
                "ad_c": row[4],
                "avg_frequency": row[5],
                "id_frequency": row[6],
                "total_shows": row[7]
            }
            for row in main_query.result_rows
        }
    return result

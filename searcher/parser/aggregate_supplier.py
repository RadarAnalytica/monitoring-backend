from datetime import date, timedelta

from log_alert import log_alert
from settings import logger
from clickhouse_db.get_async_connection import get_async_connection


@log_alert(message="Начинаются аггрегаты поставщика", end_message="Завершены аггрегаты поставщика", track_error=True)
async def aggregate_supplier():
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
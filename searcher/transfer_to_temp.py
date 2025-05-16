import time

import clickhouse_connect
from settings import CLICKHOUSE_CONFING, logger


def transfer(l, r, step, city, date):
    client = clickhouse_connect.get_client(**CLICKHOUSE_CONFING)
    logger.info(f"CITY {city}, DATE {date}")
    for i in range(l, r, step):
        logger.info(f"Batch {i}")
        client.command(
            f"""INSERT INTO
    update_request_product(
        product,
        city,
        date,
        query,
        place,
        advert,
        natural_place,
        cpm,
        brand_id,
        subject_id,
        supplier_id
    )
SELECT
    rp.product,
    rp.city,
    rp.date,
    rp.query,
    rp.place,
    rp.advert,
    rp.natural_place,
    rp.cpm,
    coalesce(bp.id, 0),
    coalesce(sp.id, 0),
    coalesce(spp.id, 0)
FROM
    (
        SELECT
            product,
            city,
            date,
            query,
            place,
            advert,
            natural_place,
            cpm
        FROM
            request_product
        WHERE
            city = {city}
            AND date = {date}
            AND product BETWEEN {i} AND {i + step - 1}
    ) AS rp
    LEFT OUTER JOIN (
        SELECT
            id,
            product_id
        FROM
            brand_product
        WHERE
            product_id BETWEEN {i} AND {i + step - 1}
    ) AS bp ON bp.product_id = rp.product
    LEFT OUTER JOIN (
        SELECT
            id,
            wb_id
        FROM
            subject_product
        WHERE
            wb_id BETWEEN {i} AND {i + step - 1}
    ) AS sp ON sp.wb_id = rp.product
    LEFT OUTER JOIN (
        SELECT
            id,
            wb_id
        FROM
            supplier_product
        WHERE
            wb_id BETWEEN {i} AND {i + step - 1}
    ) AS spp ON spp.wb_id = rp.product"""
        )
        logger.info("SLEEPING")
        time.sleep(5)
        logger.info("WOKE UP")
    client.close()


dates = list(range(1, 40))
dates.sort(reverse=True)
print(dates[0], "-", dates[-1])

for i_ in dates:
    left = 1
    right = 410000000
    if i_ == 39:
        left = 220000000
    transfer(left, right, 3000000, 1, i_)
    time.sleep(5)


import time

import clickhouse_connect
from settings import CLICKHOUSE_CONFING, logger


def transfer(left, right, step, city, date):
    client = clickhouse_connect.get_client(**CLICKHOUSE_CONFING)
    logger.info(f"CITY {city}, DATE {date}")
    for i in range(left, right, step):
        logger.info(f"Batch {i}")
        client.command(f"""INSERT INTO 
            request_product_temp(product, city, date, query, place, advert, natural_place, cpm) 
        SELECT product, city, date, query, place, advert, natural_place, cpm 
        FROM request_product 
        WHERE 
            product BETWEEN {i} AND {i + step - 1} 
        AND 
            city = {city} 
        AND 
            date = {date}""")
        logger.info("SLEEPING")
        time.sleep(90)
        logger.info("WOKE UP")
    client.close()

rng = [i for i in range(3, 66)]
rng.sort(reverse=True)
for d in rng:
    s = 0
    if d == 65:
        s = 210000000
    transfer(s, 400000000, 5000000, 1, d)
    time.sleep(60)


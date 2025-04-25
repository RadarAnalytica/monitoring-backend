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
        SELECT product, city, CAST(66 AS UInt16), query, place, advert, natural_place, cpm 
        FROM request_product_temp
        WHERE 
            product BETWEEN {i} AND {i + step - 1} 
        AND 
            city = {city} 
        AND 
            date = {date}""")
        logger.info("SLEEPING")
        time.sleep(30)
        logger.info("WOKE UP")
    client.close()

transfer(20000000, 400000000, 5000000, 1, 65)


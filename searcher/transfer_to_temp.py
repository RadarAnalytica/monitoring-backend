import clickhouse_connect
from settings import CLICKHOUSE_CONFING, logger


def transfer(left, right, step, city, date):
    client = clickhouse_connect.get_client(**CLICKHOUSE_CONFING)
    for i in range(left, right, step):
        logger.info(f"Batch {i}")
        client.command(f"""INSERT INTO 
            request_product_temp(product, city, date, query, place, advert, natural_place, cpm) 
        SELECT product, city, date, query, place, advert, natural_place, cpm 
        FROM request_product 
        WHERE 
            product BETWEEN {i + 1} AND {i + step} 
        AND 
            city = {city} 
        AND 
            date = {date}""")
    client.close()


transfer(70000000, 400000000, 10000000, 1, 1)


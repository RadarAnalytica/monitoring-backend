import time

import clickhouse_connect
from settings import CLICKHOUSE_CONFIG, logger


def transfer(date):
    client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)
    logger.info(f"DATE {date}")
    client.command(
            f"""INSERT INTO radar.brand_aggregates
SELECT
    date,
    brand_id,
    avgState(place) AS place_avg_state,
    sumState(cpm) AS cpm_sum_state,
    sumState(if(advert = 'b', cpm, 0)) AS cpm_sum_b_state,
    sumState(if(advert = 'c', cpm, 0)) AS cpm_sum_c_state,
    avgState(if(cpm > 0, toNullable(cpm), NULL)) AS cpm_avg_state,
    avgState(if(advert = 'b', toNullable(cpm), NULL)) AS cpm_avg_b_state,
    avgState(if(advert = 'c', toNullable(cpm), NULL)) AS cpm_avg_c_state,
    uniqExactState(product) AS unique_products_state
FROM radar.request_product where city = 1 and date = {date}
GROUP BY date, brand_id;"""
        )
    client.close()


# dates = list(range(1, 126))
# dates.sort(reverse=True)
# print(dates[0], "-", dates[-1])
#
# for i in dates:
#     time.sleep(5)
#     transfer(i)

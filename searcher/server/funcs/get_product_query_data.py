from clickhouse_db.get_async_connection import get_async_connection

async def get_product_db_data(product_id, city, interval):
    async with get_async_connection() as client:
        query = f"""SELECT sd.query, sd.quantity, groupArray((sd.date, sd.place, sd.advert, sd.natural_place, sd.cpm)) AS date_info
        FROM (SELECT r.query as query, r.quantity as quantity, d.date as date, rp.place as place, rp.advert as advert, rp.natural_place as natural_place, rp.cpm as cpm 
        FROM request_product AS rp
        JOIN (SELECT id, query, quantity FROM request FINAL) AS r ON r.id = rp.query
        JOIN dates as d ON d.id = rp.date
        JOIN city as c ON c.id = rp.city
        WHERE (rp.product = {product_id})
        AND (c.dest = {city})
        AND (d.date >= toStartOfDay(now() - INTERVAL {interval} DAY))
        ORDER BY rp.date, r.quantity DESC
        ) AS sd
        GROUP BY sd.query, sd.quantity
        ORDER BY sd.quantity DESC, sd.query;"""
        query_result = await client.query(query)
        result = [
            {
                "query": row[0],
                "quantity": row[1],
                "dates": {
                    str(j_row[0]): {"place": j_row[1], "ad": j_row[2].decode() if j_row[2] != b"z" else None,
                                    "nat": j_row[3] or None, "cpm": j_row[4] or 0}
                    for j_row in row[2]
                }
            }
            for row in query_result.result_rows
        ]
    return result




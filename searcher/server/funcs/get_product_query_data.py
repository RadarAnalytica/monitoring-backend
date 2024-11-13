from clickhouse_db.get_async_connection import get_async_connection


async def get_product_db_data():
    async with get_async_connection() as client:
        query = f"""SELECT count(*) 
                        FROM request_product;"""
        query_result = await client.query(query)
    return query_result.result_rows


async def get_product_query_payload(product_id, interval, city):
    query_result = await get_product_db_data()
    payload = [
        {
            "query": row[0],
            "quantity": row[1],
            "dates": {
                str(j_row[0]): j_row[1]
                for j_row in row[2]
            }
        }
        for row in query_result
    ]
    return payload




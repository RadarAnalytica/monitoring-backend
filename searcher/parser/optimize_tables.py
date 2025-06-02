from clickhouse_db.get_async_connection import get_async_connection
from service.log_alert import send_log_message


async def optimize_table_final(table_name, partition=None):
    if isinstance(partition, str):
        partition = f"'{partition}'"
    await send_log_message(message=f"Оптимизируется таблица {table_name}{f', partition: {partition}' if partition else ''}")
    async with get_async_connection(send_receive_timeout=3600) as client:
        await client.command(f"OPTIMIZE TABLE `{table_name}` {f'PARTITION {partition}' if partition else ''}")




async def get_latest_date_id():
    async with get_async_connection() as client:
        q = await client.query("select max(id) from dates")
        res = list(q.result_rows)
        date_id = res[0][0]
    return date_id


async def optimize_request_product_partition(city=1):
    last_date = await get_latest_date_id()
    await optimize_table_final(table_name="request_product", partition=(city, last_date))




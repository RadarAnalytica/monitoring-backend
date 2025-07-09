from clickhouse_db.get_async_connection import get_async_connection

async def transfer_aggregates_to_local():
    stmt = """INSERT INTO wb_id_extended_local SELECT * FROM wb_id_extended"""
    async with get_async_connection(send_receive_timeout=3600) as client:
        await client.command(stmt)
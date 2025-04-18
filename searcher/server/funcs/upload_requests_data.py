from datetime import datetime

from clickhouse_db.get_async_connection import get_async_connection
from server.funcs.prepare_csv_contents import prepare_request_frequency
from settings import logger



async def upload_requests_worker(requests_slice: list[list[int, str, int, datetime]], client):
    await client.insert('request', requests_slice, column_names=["id", "query", "quantity", "updated"])
    logger.info("Start of part DB renewal - request")


async def upload_request_frequency_worker(requests_slice: list[list[int, int, datetime]], client):
    await client.insert('request_frequency', requests_slice, column_names=["query_id", "frequency", "date"])
    logger.info("Start of part DB renewal - request_frequency")


async def upload_requests_csv_bg(requests_data: list[list[int, str, int, datetime]]):
    logger.info("Uploading requests data")
    async with get_async_connection() as client:
        slice_1 = requests_data[:250000]
        slice_2 = requests_data[250000:500000]
        slice_3 = requests_data[500000:750000]
        slice_4 = requests_data[750000:]

        await upload_requests_worker(slice_1, client)
        await upload_requests_worker(slice_2, client)
        await upload_requests_worker(slice_3, client)
        await upload_requests_worker(slice_4, client)
        await client.command("OPTIMIZE TABLE request FINAL")
        logger.info("Requests uploaded")
        frequency_rows_1 = await prepare_request_frequency(slice_1, client)
        if frequency_rows_1:
            await upload_request_frequency_worker(frequency_rows_1, client)
        logger.info("Slice 1 ready")
        frequency_rows_2 = await prepare_request_frequency(slice_2, client)
        if frequency_rows_2:
            await upload_request_frequency_worker(frequency_rows_2, client)
        logger.info("Slice 2 ready")
        frequency_rows_3 = await prepare_request_frequency(slice_3, client)
        if frequency_rows_3:
            await upload_request_frequency_worker(frequency_rows_3, client)
        logger.info("Slice 3 ready")
        frequency_rows_4 = await prepare_request_frequency(slice_4, client)
        if frequency_rows_4:
            await upload_request_frequency_worker(frequency_rows_4, client)
        logger.info("Slice 4 ready")
    logger.warning("DB renewal complete")

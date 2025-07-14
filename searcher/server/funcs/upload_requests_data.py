from datetime import datetime, date

from clickhouse_db.get_async_connection import get_async_connection
from server.funcs.prepare_csv_contents import prepare_request_frequency, recount_request_frequency, \
    get_request_frequency_by_date, prepare_request_frequency_excel
from settings import logger


async def upload_requests_worker(
    requests_slice: list[list | tuple], client
):
    await client.insert(
        "request", requests_slice, column_names=["id", "query", "quantity", "subject_id", "total_products", "updated"]
    )
    logger.info("Start of part DB renewal - request")


async def upload_request_frequency_worker(
    requests_slice: list[list | tuple], client
):
    await client.insert(
        "request_frequency",
        requests_slice,
        column_names=["query_id", "frequency", "date"],
    )
    logger.info("Start of part DB renewal - request_frequency")

async def upload_request_growth_worker(
    requests_slice: list[list | tuple], client
):
    await client.insert(
        "request_growth",
        requests_slice,
        column_names=["query_id", "date", "g30", "g60", "g90", "sum30", "subject_id"],
    )
    logger.info("Start of part DB renewal - request_growth")


async def upload_requests_csv_bg(requests_data: list):
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
        frequency_rows_1, growth_rows_1 = await prepare_request_frequency(slice_1, client)
        if frequency_rows_1:
            await upload_request_frequency_worker(frequency_rows_1, client)
            await upload_request_growth_worker(client=client, requests_slice=growth_rows_1)
        logger.info("Slice 1 ready")
        frequency_rows_2, growth_rows_2 = await prepare_request_frequency(slice_2, client)
        if frequency_rows_2:
            await upload_request_frequency_worker(frequency_rows_2, client)
            await upload_request_growth_worker(client=client, requests_slice=growth_rows_2)
        logger.info("Slice 2 ready")
        frequency_rows_3, growth_rows_3 = await prepare_request_frequency(slice_3, client)
        if frequency_rows_3:
            await upload_request_frequency_worker(frequency_rows_3, client)
            await upload_request_growth_worker(client=client, requests_slice=growth_rows_3)
        logger.info("Slice 3 ready")
        frequency_rows_4, growth_rows_4 = await prepare_request_frequency(slice_4, client)
        if frequency_rows_4:
            await upload_request_frequency_worker(frequency_rows_4, client)
            await upload_request_growth_worker(client=client, requests_slice=growth_rows_4)
        logger.info("Slice 4 ready")
    logger.warning("DB renewal complete")


async def update_test_request_frequency_worker(
    requests_slice: list[list[int, int, datetime]], client
):
    await client.insert(
        "request_frequency_test",
        requests_slice,
        column_names=["query_id", "frequency", "date"],
    )
    logger.info("Start of part DB renewal - request_frequency_test")


async def recount_requests_csv_bg(requests_data: list, new_requests: list):
    logger.info("Uploading requests data")
    async with get_async_connection() as client:
        if new_requests:
            await upload_requests_worker(requests_slice=new_requests, client=client)
            logger.info("Requests uploaded")
        slice_1 = requests_data[:250000]
        slice_2 = requests_data[250000:500000]
        slice_3 = requests_data[500000:750000]
        slice_4 = requests_data[750000:]
        frequency_rows_1 = await recount_request_frequency(slice_1, client)
        if frequency_rows_1:
            await update_test_request_frequency_worker(frequency_rows_1, client)
        logger.info("Slice 1 ready")
        frequency_rows_2 = await recount_request_frequency(slice_2, client)
        if frequency_rows_2:
            await update_test_request_frequency_worker(frequency_rows_2, client)
        logger.info("Slice 2 ready")
        frequency_rows_3 = await recount_request_frequency(slice_3, client)
        if frequency_rows_3:
            await update_test_request_frequency_worker(frequency_rows_3, client)
        logger.info("Slice 3 ready")
        frequency_rows_4 = await recount_request_frequency(slice_4, client)
        if frequency_rows_4:
            await update_test_request_frequency_worker(frequency_rows_4, client)
        logger.info("Slice 4 ready")
    logger.warning("DB renewal complete")


async def recount_growth_by_date(date_: date):
    logger.info("Recounting growth requests data")
    async with get_async_connection() as client:
        await client.command("OPTIMIZE TABLE request FINAL")
        logger.info("GETTING GROWTH ROWS")
        growth_rows = await get_request_frequency_by_date(date_=date_, client=client)
        logger.info("GROWTH ROWS ACQUIRED")
        await upload_request_growth_worker(client=client, requests_slice=growth_rows)
    logger.info(f"GROWTH ROWS FOR DATE {date_} UPLOADED ")




async def upload_requests_excel_bg(requests_data: list):
    logger.info("Uploading requests data")
    async with get_async_connection() as client:
        slice_1 = requests_data[:250000]
        slice_2 = requests_data[250000:500000]
        slice_3 = requests_data[500000:750000]
        slice_4 = requests_data[750000:]

        if slice_1:
            await upload_requests_worker(slice_1, client)
        if slice_2:
            await upload_requests_worker(slice_2, client)
        if slice_3:
            await upload_requests_worker(slice_3, client)
        if slice_4:
            await upload_requests_worker(slice_4, client)
        await client.command("OPTIMIZE TABLE request FINAL")
        logger.info("Requests uploaded")
        if slice_1:
            frequency_rows_1, growth_rows_1 = await prepare_request_frequency_excel(slice_1, client)
            if frequency_rows_1:
                await upload_request_frequency_worker(frequency_rows_1, client)
                await upload_request_growth_worker(client=client, requests_slice=growth_rows_1)
            logger.info("Slice 1 ready")
        if slice_2:
            frequency_rows_2, growth_rows_2 = await prepare_request_frequency_excel(slice_2, client)
            if frequency_rows_2:
                await upload_request_frequency_worker(frequency_rows_2, client)
                await upload_request_growth_worker(client=client, requests_slice=growth_rows_2)
            logger.info("Slice 2 ready")
        if slice_3:
            frequency_rows_3, growth_rows_3 = await prepare_request_frequency_excel(slice_3, client)
            if frequency_rows_3:
                await upload_request_frequency_worker(frequency_rows_3, client)
                await upload_request_growth_worker(client=client, requests_slice=growth_rows_3)
            logger.info("Slice 3 ready")
        if slice_4:
            frequency_rows_4, growth_rows_4 = await prepare_request_frequency_excel(slice_4, client)
            if frequency_rows_4:
                await upload_request_frequency_worker(frequency_rows_4, client)
                await upload_request_growth_worker(client=client, requests_slice=growth_rows_4)
        logger.info("Slice 4 ready")
    logger.warning("DB renewal complete")
from aiohttp import ClientSession
from parser.get_single_query_data import get_query_data
import asyncio
import gc
from clickhouse_db.get_async_connection import get_async_connection

from clickhouse_connect.driver import AsyncClient
from settings import logger


async def http_worker(http_session: ClientSession, http_queue: asyncio.Queue, save_queue: asyncio.Queue):
    while True:
        item = await http_queue.get()
        if item is None:
            await http_queue.put(None)
            break
        query_string, query_id = item[0], item[1]
        item_result = await get_query_data(http_session=http_session, query_string=query_string, page=1, limit=3, dest=-1257786, rqa=3)
        if item_result:
            products = item_result.get("data", dict()).get("products", [])
            if products:
                first = products[0]
                subject_id = first.get("subjectId", 0)
                await save_queue.put((query_id, subject_id))


async def save_to_db(
    queue: asyncio.Queue, table, fields, client: AsyncClient
):
    counter = 0
    while True:
        items = []
        item = []
        while len(items) < 10000:
            item = await queue.get()
            if item is None:
                break
            items.append(item)
        if items:
            try:
                await client.insert(table, items, column_names=fields)
                counter += 1
                if counter == 10:
                    counter = 0
                    logger.info(
                        f"Запись в БД"
                    )
                gc.collect()
            except Exception as e:
                logger.critical(f"{e}, {items}")
        if item is None:
            await queue.put(item)
            return


async def get_queries_subjects(queries_slice: list[tuple[str, int]]):
    http_queue = asyncio.Queue(10)
    save_queue = asyncio.Queue(10)
    async with ClientSession() as http_session:
        http_tasks = [
            asyncio.create_task(http_worker(http_session=http_session, http_queue=http_queue, save_queue=save_queue))
            for _ in range(10)
        ]
        async with get_async_connection() as client:
            save_db_task = asyncio.create_task(save_to_db(
                queue=save_queue,
                table="request_subject",
                fields=["query_id", "subject_id"],
                client=client,
            ))
            while queries_slice:
                item = queries_slice.pop(0)
                await http_queue.put(item)

            await http_queue.put(None)
            await save_queue.put(None)
            await asyncio.gather(*http_tasks, save_db_task)
    logger.info("Slice request done")
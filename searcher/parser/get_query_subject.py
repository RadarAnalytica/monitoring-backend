from aiohttp import ClientSession
from parser.get_single_query_data import get_query_data
import asyncio
import gc
from clickhouse_db.get_async_connection import get_async_connection
from datetime import date

from clickhouse_connect.driver import AsyncClient
from settings import logger


async def http_worker(http_session: ClientSession, http_queue: asyncio.Queue, save_queue: asyncio.Queue):
    while True:
        item = await http_queue.get()
        if item is None:
            await http_queue.put(None)
            return
        query_string: str = item[0]
        query_id = item[1]
        item_result = await get_query_data(http_session=http_session, query_string=query_string, page=1, limit=3, dest=-1257786, rqa=3)
        if item_result:
            if query_string.isdigit():
                total = 1
            else:
                total = item_result.get("data", dict()).get("total", 0)
            products = item_result.get("data", dict()).get("products", [])
            if products:
                first = products[0]
                subject_id = first.get("subjectId", 0)
                if total or subject_id:
                    await save_queue.put((query_id, subject_id, total))


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


async def get_queries_subjects(left, right):
    http_queue = asyncio.Queue(20)
    save_queue = asyncio.Queue(20)
    async with ClientSession() as http_session:
        http_tasks = [
            asyncio.create_task(http_worker(http_session=http_session, http_queue=http_queue, save_queue=save_queue))
            for _ in range(10)
        ]
        async with get_async_connection() as client:
            q = await client.query(fr"SELECT trim(BOTH ' !#' FROM query), id FROM request FINAL WHERE id BETWEEN {left + 1} AND {right} AND (subject_id = 0 OR total_products = 0) ORDER BY id")
            queries_slice = list(q.result_rows)
            save_db_task = asyncio.create_task(save_to_db(
                queue=save_queue,
                table="request_subject",
                fields=["query_id", "subject_id", "total_products"],
                client=client,
            ))
            counter = 0
            while queries_slice:
                counter += 1
                if counter % 100 == 0:
                    logger.info(f"REQUESTS {counter} BATCH")
                item = queries_slice.pop(0)
                logger.info(item)
                await http_queue.put(item)

            await http_queue.put(None)
            await asyncio.gather(*http_tasks)
            await save_queue.put(None)
            await save_db_task
    logger.info("Slice request done")


async def get_query_prio_subject(http_session: ClientSession, query_string: str):
    subject_id = 0
    try:
        item_result = await get_query_data(http_session=http_session, query_string=query_string, page=1, limit=3,
                                       dest=-1257786, rqa=3)
        if item_result:
            products = item_result.get("data", dict()).get("products", [])
            if products:
                first = products[0]
                subject_id = first.get("subjectId", 0)
    except:
        return subject_id
    return subject_id


async def get_query_prio_subject_and_total(http_session: ClientSession, query_data: tuple[int, str, date, int]):
    subject_id = 0
    query_string = query_data[1]
    query_id = query_data[0]
    total_products = 0
    new_date = query_data[2]
    quantity = query_data[3]
    try:
        item_result = await get_query_data(http_session=http_session, query_string=query_string, page=1, limit=3,
                                       dest=-1257786, rqa=1 if query_string.isdigit() else 3, upload=True)
        if item_result:
            products = item_result.get("data", dict()).get("products", [])
            total_products = item_result.get("data", dict()).get("total", 0)
            if products:
                first = products[0]
                subject_id = first.get("subjectId", 0)
    except:
        return query_id, query_string, quantity, subject_id, total_products, new_date
    return query_id, query_string, quantity, subject_id, total_products, new_date



async def get_query_total(http_session: ClientSession, query_data: tuple[int, str, date, int, int]):
    query_string = query_data[1]
    query_id = query_data[0]
    total_products = 0
    new_date = query_data[2]
    quantity = query_data[3]
    subject_id = query_data[4]
    try:
        item_result = await get_query_data(http_session=http_session, query_string=query_string, page=1, limit=3,
                                       dest=-1257786, rqa=1 if query_string.isdigit() else 3, upload=True)
        if item_result:
            total_products = item_result.get("data", dict()).get("total", 0)
    except:
        return query_id, query_string, quantity, subject_id, total_products, new_date
    return query_id, query_string, quantity, subject_id, total_products, new_date



async def get_query_list_prio_subjects(http_session: ClientSession, queries: list[tuple[int, str, date, int]]):
    tasks = [
        asyncio.create_task(get_query_prio_subject_and_total(http_session=http_session, query_data=query_data))
        for query_data in queries
    ]
    result = await asyncio.gather(*tasks)
    return result


async def get_query_list_prio_subjects_batched(http_session: ClientSession, queries: list[tuple[int, str, date, int]], batch_size=50):
    batches = []
    for i in range(0, len(queries) + 1, batch_size):
        batch = queries[i: i + batch_size]
        if batch:
            batches.append(batch)
    result = []
    for batch in batches:
        batch_result = await get_query_list_prio_subjects(http_session=http_session, queries=batch)
        if batch_result:
            result.extend(batch_result)
    return result



async def get_query_list_totals(http_session: ClientSession, queries: list[tuple[int, str, date, int, int]]):
    tasks = [
        asyncio.create_task(get_query_total(http_session=http_session, query_data=query_data))
        for query_data in queries
    ]
    result = await asyncio.gather(*tasks)
    return result



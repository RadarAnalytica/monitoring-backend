import asyncio
import gc

import psutil
from aiohttp import ClientSession
from psutil import swap_memory

from clickhouse_db.get_async_connection import get_async_connection, get_sync_connection
from parser.get_single_query_data import get_query_data
from settings import logger
from parser.save_to_db_worker import save_to_db


async def get_r_data_q(
    queue: asyncio.Queue, city, date, http_session
):
    while True:
        r = await queue.get()
        if r is None:
            await queue.put(r)
            break
        await get_r_data(
            r=r,
            city=city,
            date=date,
            http_session=http_session,
        )
        queue.task_done()


async def try_except_query_data(query_string, dest, limit, page, http_session, rqa=5):
    try:
        x = await get_query_data(
            http_session=http_session,
            query_string=query_string,
            dest=dest,
            limit=limit,
            page=page,
            rqa=rqa,
            timeout=5,
        )
    except ValueError:
        x = {"products": []}
    return x


async def get_r_data(r, city, date, http_session, db_queue=None):
    while True:
        try:
            full_res = []
            tasks = [
                asyncio.create_task(
                    try_except_query_data(
                        query_string=r[1],
                        dest=city[1],
                        limit=250,
                        page=i,
                        rqa=3,
                        http_session=http_session,
                    )
                )
                for i in range(1, 3)
            ]
            result = await asyncio.gather(*tasks)
            for res in result:
                full_res.extend(res.get("products", []))
            if not full_res:
                full_res = []
            request_products = []
            for i, p in enumerate(full_res, 1):
                log = p.get("log", {})
                natural_place = log.get("position", 0)
                if natural_place > 65535:
                    natural_place = 65535
                cpm = log.get("cpm", 0)
                if cpm > 65535:
                    cpm = 65535
                request_products.append((p.get("id"), city[0], date[0], r[0], i, log.get("tp", "z"), natural_place, cpm))
            return request_products
        except Exception as e:
            logger.critical(f"{e}")
            break


async def get_city_result(city, date, requests, request_batch_no, client=None):
    logger.info(f"Город {city} старт, batch: {request_batch_no}")
    process_ = psutil.Process()
    requests_list = [r for r in requests if not r[1].isdigit() or "javascript" not in r[1]]
    del requests
    logger.info("Запросы есть")
    batch_size = 10
    prev = 0
    full_res = []
    async with ClientSession() as http_session:
        async with get_async_connection() as client:
            for batch_i in range(batch_size, len(requests_list) + 1, batch_size):
                request_batch = requests_list[prev:batch_i]
                requests_tasks = [
                    asyncio.create_task(
                        get_r_data(
                            r=r,
                            city=city,
                            date=date,
                            http_session=http_session,
                        )
                    )
                    for r in request_batch
                ]
                product_batches: tuple = await asyncio.gather(*requests_tasks)
                prev = batch_i
                for batch in product_batches:
                    full_res.extend(batch)
                if len(full_res) > 8000:
                    await save_to_db(
                        items=full_res,
                        table="request_product",
                        fields=["product", "city", "date", "query", "place", "advert", "natural_place", "cpm"],
                        client=client
                    )
                    full_res.clear()
                    gc.collect()
                logger.info(f"Память: {process_.memory_info().rss}")

# def run_pool_threads(func, *args, **kwargs):
#     try:
#         asyncio.run(func(*args, **kwargs))
#     except Exception as e:
#         logger.critical(f"Сбор данных не начался! Причина: {e}")


# async def get_results():
#     start_time = datetime.now()
#     logger.info("Вход в программу")
#     today = datetime.now().date()
#     cities = await get_cities_data()
#     with Pool(len(cities)) as p:
#         tasks = [
#             p.apply_async(run_pool_threads, args=[get_city_result, city, today])
#             for city in cities
#         ]
#         p.close()
#         p.join()
#     end_time = datetime.now()
#     delta = (start_time - end_time).seconds
#     logger.info(
#         f"Старт парса: {start_time.strftime('%H:%M %d.%m.%Y')}\n"
#         f"Завершение парса: {end_time.strftime('%H:%M %d.%m.%Y')}\n"
#         f"Выполнено за: {delta // 60 // 60} часов, {delta // 60} минут"
#     )

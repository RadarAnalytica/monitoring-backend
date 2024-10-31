import asyncio
from datetime import datetime
from multiprocessing import Pool
from aiohttp import ClientSession

from parser.get_init_data import get_requests_data, get_cities_data
from parser.get_single_query_data import get_query_data
from settings import logger
from parser.save_to_db_worker import save_to_db


async def get_r_data_q(
    queue: asyncio.Queue, city, date, http_session, request_product_queue=None
):
    logger.info("Очередь запросов поехала")
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
            request_product_queue=request_product_queue,
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


async def get_r_data(r, city, date, http_session, request_product_queue=None):
    while True:
        try:
            logger.info("Запрос пошёл!")
            full_res = []
            tasks = [
                asyncio.create_task(
                    try_except_query_data(
                        query_string=r,
                        dest=city,
                        limit=300,
                        page=i,
                        rqa=3,
                        http_session=http_session,
                    )
                )
                for i in range(1, 4)
            ]
            result = await asyncio.gather(*tasks)
            for res in result:
                full_res.extend(res.get("products", []))
            if not full_res:
                full_res = []
            request_product = [city, r, [p.get("id") for p in full_res], date]
            await request_product_queue.put(request_product)
            return
        except Exception as e:
            logger.critical(f"{e}")
            break


async def get_city_result(city, date):
    logger.info(f"Город {city} старт")
    requests = [r for r in await get_requests_data() if not r.isdigit()]
    logger.info("Запросы есть")
    request_product_queue = asyncio.Queue()
    workers_queue = asyncio.Queue()
    request_product_save_task = [
        asyncio.create_task(
            save_to_db(
                request_product_queue,
                "request_product",
                ["city", "query", "products", "date"],
            )
        )
        for _ in range(5)
    ]
    logger.info("Задачи на запись созданы")
    async with ClientSession() as http_session:
        requests_tasks = [
            asyncio.create_task(
                get_r_data_q(
                    queue=workers_queue,
                    city=city,
                    date=date,
                    http_session=http_session,
                    request_product_queue=request_product_queue,
                )
            )
            for _ in range(45)
        ]
        while requests:
            try:
                await workers_queue.put(requests.pop())
                await workers_queue.put(requests.pop())
                await workers_queue.put(requests.pop())
                await workers_queue.put(requests.pop())
            except Exception as e:
                logger.error(f"{e}")
                pass
        await workers_queue.put(None)
        await asyncio.gather(*requests_tasks)
        await request_product_queue.put(None)
        await asyncio.gather(*request_product_save_task)


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

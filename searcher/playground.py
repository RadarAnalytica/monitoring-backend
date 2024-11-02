import asyncio
from datetime import datetime
from itertools import count
from multiprocessing import Pool
from aiohttp import ClientSession
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from parser.get_single_query_data import get_query_data
from db.base import async_session_maker
from db import City, Request, RequestProduct
from settings import logger


async def get_cities_data():
    async with async_session_maker() as s:
        q = await s.execute(select(City).limit(1))
        cities = q.scalars()
    return cities


async def get_requests_data():
    async with async_session_maker() as s:
        q = await s.execute(select(Request).order_by(Request.quantity.desc()))
        requests = q.scalars()
    return requests


async def save_to_db(queue, model, update=False):
    while True:
        items = []
        item = None
        while len(items) < 500:
            item = await queue.get()
            if item is None:
                break
            if isinstance(item, list):
                items.extend(item)
            else:
                items.append(item)
            queue.task_done()
        if items:
            async with async_session_maker() as session:
                if not update:
                    await session.execute(insert(model).values(items))
                else:
                    primary_fields = [
                        col.name for col in model.__table__.columns if col.primary_key
                    ]
                    try:
                        await session.execute(
                            insert(model)
                            .values(items)
                            .on_conflict_do_nothing(
                                index_elements=primary_fields,
                            )
                        )
                    except Exception as e:
                        logger.error(f"{e}")
                await session.commit()
        if item is None:
            await queue.put(item)
            break


async def get_r_data_q(
    queue: asyncio.Queue, city, date, http_session, request_product_queue=None
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
            timeout=10,
        )
    except ValueError:
        x = {"products": []}
    return x


async def get_r_data(r, city, date, http_session, request_product_queue=None):
    while True:
        try:
            full_res = []
            tasks = [
                asyncio.create_task(
                    try_except_query_data(
                        query_string=r.query,
                        dest=city.dest,
                        limit=300,
                        page=i,
                        rqa=4,
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
            counter = 0
            for res in full_res:
                if res.get("log"):
                    counter += 1
                    if counter >= 2:
                        logger.critical(f"{res.get('log')}")
                    else:
                        logger.info(f"{res.get('log')}")
            request_product = {
                "city": city.id,
                "query": r.query,
                "products": [p.get("id") for p in full_res],
                "natural_positions": [
                    p.get("log", {}).get("position", 0) for p in full_res
                ],
                "date": date,
            }
            await request_product_queue.put(request_product)
            return
        except Exception as e:
            logger.critical(f"{e}")
            break


async def get_city_result(city, date):
    requests = [r for r in await get_requests_data() if not r.query.isdigit()]
    request_product_queue = asyncio.Queue()
    workers_queue = asyncio.Queue()
    request_product_save_task = [
        asyncio.create_task(save_to_db(request_product_queue, RequestProduct))
        for _ in range(5)
    ]
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
            for _ in range(20)
        ]
        while requests:
            await workers_queue.put(requests.pop())
        await workers_queue.put(None)
        await asyncio.gather(*requests_tasks)
        await request_product_queue.put(None)
        await asyncio.gather(*request_product_save_task)


def run_pool_threads(func, *args, **kwargs):
    try:
        asyncio.run(func(*args, **kwargs))
    except Exception as e:
        logger.critical(f"Сбор данных не начался! Причина: {e}")


async def get_results():
    start_time = datetime.now()
    logger.info("Вход в программу")
    today = datetime.now().date()
    cities = await get_cities_data()
    cities = list(cities)
    with Pool(len(cities)) as p:
        tasks = [
            p.apply_async(run_pool_threads, args=[get_city_result, city, today])
            for city in cities
        ]
        p.close()
        p.join()
    end_time = datetime.now()
    delta = (start_time - end_time).seconds
    logger.info(
        f"Старт парса: {start_time.strftime('%H:%M %d.%m.%Y')}\n"
        f"Завершение парса: {end_time.strftime('%H:%M %d.%m.%Y')}\n"
        f"Выполнено за: {delta // 60 // 60} часов, {delta // 60} минут"
    )


asyncio.run(get_results())

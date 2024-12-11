import asyncio

from aiohttp import ClientSession

from clickhouse_db.get_async_connection import get_async_connection
from parser.get_single_query_data import get_query_data
from service.log_alert import send_log_message
from settings import logger
from parser.save_to_db_worker import save_to_db, save_to_db_single


async def get_r_data_q(
    http_queue: asyncio.Queue, db_queue, city, date, http_session, client=None
):
    while True:
        r = await http_queue.get()
        if r is None:
            await http_queue.put(r)
            break
        await get_r_data(
            r=r,
            city=city,
            date=date,
            http_session=http_session,
            db_queue=db_queue,
            client=client
        )



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


async def get_r_data(r, city, date, http_session, db_queue=None, client=None):
    count = 0
    while count <= 3:
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
                for i in range(1, 4)
            ]
            result = await asyncio.gather(*tasks)
            for res in result:
                full_res.extend(res.get("data").get("products", []))
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
            if client:
                preset = result[0].get("metadata", dict()).get("catalog_value", "").replace("preset=", "")
                try:
                    preset = int(preset) if preset else None
                    norm_query = result[0].get("metadata", dict()).get("normquery", None)
                except (ValueError, TypeError):
                    preset = None
                    norm_query = None
                if preset and norm_query:
                    await save_to_db_single(
                        client=client,
                        table="preset",
                        fields=["preset", "norm_query", "query", "date"],
                        data=((preset, norm_query, r[0], date[1]), )
                    )
            await db_queue.put(request_products)
            return
        except Exception as e:
            count += 1
            logger.critical(f"{e}")


async def get_city_result(city, date, requests, request_batch_no, get_preset=False):
    logger.info(f"Город {city} старт, batch: {request_batch_no}")
    await send_log_message(f"Начался сбор данных по городу:\n{city}")
    requests_list = [r for r in requests if not r[1].isdigit() or "javascript" not in r[1]]
    del requests
    db_queue = asyncio.Queue(2)
    http_queue = asyncio.Queue(3)
    logger.info("Запросы есть")
    async with ClientSession() as http_session:
        async with get_async_connection() as client:
            db_worker = asyncio.create_task(
                save_to_db(
                    queue=db_queue,
                    table="request_product",
                    fields=["product", "city", "date", "query", "place", "advert", "natural_place", "cpm"],
                    client=client
                )
            )
            requests_tasks = [
                asyncio.create_task(
                    get_r_data_q(
                        city=city,
                        date=date,
                        http_session=http_session,
                        db_queue=db_queue,
                        http_queue=http_queue,
                        client=client if get_preset else None
                    )
                )
                for _ in range(5)
            ]
            counter = 0
            while requests_list:
                try:
                    counter += 1
                    await http_queue.put(requests_list.pop())
                    if not (counter % 1000):
                        logger.info(f"Осталось запросов в батче: {len(requests_list)}")
                except Exception as e:
                    logger.error(f"{e}")
            await http_queue.put(None)
            await asyncio.gather(*requests_tasks)
            await db_queue.put(None)
            await asyncio.gather(db_worker)
    await send_log_message(f"Завершен сбор данных по городу: {city}")
    return


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

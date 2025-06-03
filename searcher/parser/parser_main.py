import asyncio
from datetime import date as Date
from aiohttp import ClientSession

from clickhouse_db.get_async_connection import get_async_connection
from parser.get_single_query_data import get_query_data
from service.log_alert import send_log_message
from settings import logger
from parser.save_to_db_worker import save_to_db


async def get_r_data_q(
    http_queue: asyncio.Queue,
    db_queue,
    city,
    date,
    http_session,
    preset_queue=None,
    query_history_queue=None,
    today_date=None,
):
    while True:
        r = await http_queue.get()
        if r is None:
            await http_queue.put(r)
            break
        page = r[0]
        query = r[1]
        await get_r_data(
            r=query,
            page=page,
            city=city,
            date=date,
            http_session=http_session,
            db_queue=db_queue,
            preset_queue=preset_queue,
            query_history_queue=query_history_queue,
            today_date=today_date,
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
        x = {"data": {"products": []}}
    return x


async def get_r_data(
    r,
    page,
    city,
    date,
    http_session,
    db_queue=None,
    preset_queue=None,
    query_history_queue=None,
    today_date=None,
    limit=300
):
    count = 0
    while count <= 3:
        try:
            result = await try_except_query_data(
                        query_string=r[1],
                        dest=city[1],
                        limit=limit,
                        page=page,
                        rqa=3,
                        http_session=http_session,
                    )
            full_res = result.get("data", dict()).get("products", [])
            request_products = []
            page_increment = (page - 1) * limit
            for i, p in enumerate(full_res, 1):
                if not p.get("id"):
                    continue
                log = p.get("log", {})
                brand_id = abs(p.get("brandId", 0) or 0)
                subject_id = abs(p.get("subjectId", 0) or 0)
                supplier_id = abs(p.get("supplierId", 0) or 0)
                natural_place = log.get("position", 0) or 0
                if natural_place > 65535:
                    natural_place = 65535
                cpm = log.get("cpm", 0)
                if cpm > 65535:
                    cpm = 65535
                request_products.append(
                    (
                        p.get("id"),
                        city[0],
                        date[0],
                        r[0],
                        i + page_increment,
                        log.get("tp", "z"),
                        natural_place,
                        cpm,
                        brand_id,
                        subject_id,
                        supplier_id,
                    )
                )
            if page == 1 and preset_queue:
                preset = (
                    result
                    .get("metadata", dict())
                    .get("catalog_value", "")
                    .replace("preset=", "")
                )
                try:
                    preset = int(preset) if preset else None
                    norm_query = (
                        result.get("metadata", dict()).get("normquery", None)
                    )
                except (ValueError, TypeError):
                    preset = None
                    norm_query = None
                if preset and norm_query:
                    await preset_queue.put([(preset, norm_query, r[0])])
            if page == 1 and query_history_queue:
                total = result.get("data", dict()).get("total", 0)
                top_product = full_res[0]
                priority = top_product.get("subjectId", 0)
                if total:
                    await query_history_queue.put([(r[0], today_date, total, priority)])
            await db_queue.put(request_products)
            return
        except Exception as e:
            count += 1
            logger.critical(f"{e}")


async def get_city_result(city, date, requests, request_batch_no, get_preset=False, test=False):
    if test:
        get_preset = False
    logger.info(f"Город {city} старт, batch: {request_batch_no}")
    today_date = Date.today()
    await send_log_message(
        f"Начался сбор данных{'(ТЕСТОВЫЙ)' if test else ''} по городу:\n{city[2]}\nbatch: {request_batch_no}"
    )
    requests_list = [r for r in requests if not r[1].isdigit()]
    del requests
    preset_queue = None
    query_history_queue = None
    if get_preset:
        query_history_queue = asyncio.Queue(2)
        preset_queue = asyncio.Queue(2)
    db_queue = asyncio.Queue(2)
    http_queue = asyncio.Queue(15)
    logger.info("Запросы есть")
    async with ClientSession() as http_session:
        async with get_async_connection() as client:
            db_worker = asyncio.create_task(
                save_to_db(
                    queue=db_queue,
                    table="request_product" if not test else "update_request_product",
                    fields=[
                        "product",
                        "city",
                        "date",
                        "query",
                        "place",
                        "advert",
                        "natural_place",
                        "cpm",
                        "brand_id",
                        "subject_id",
                        "supplier_id",
                    ],
                    client=client,
                    batch_no=request_batch_no,
                )
            )
            if get_preset:
                preset_worker = asyncio.create_task(
                    save_to_db(
                        queue=preset_queue,
                        table="preset",
                        fields=["preset", "norm_query", "query"],
                        client=client,
                    )
                )
                query_history_worker = asyncio.create_task(
                    save_to_db(
                        queue=query_history_queue,
                        table="query_history",
                        fields=["query", "date", "total_products", "priority"],
                        client=client,
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
                        preset_queue=preset_queue,
                        query_history_queue=query_history_queue,
                        today_date=today_date,
                    )
                )
                for _ in range(15)
            ]
            counter = 0
            while requests_list:
                try:
                    counter += 1
                    query = requests_list.pop(0)
                    for i in range(1, 5):
                        await http_queue.put((i, query))
                    if not (counter % 1000):
                        logger.info(
                            f"Осталось запросов в батче {request_batch_no}: {len(requests_list)}"
                        )
                except Exception as e:
                    logger.error(f"{e}")
            await http_queue.put(None)
            await asyncio.gather(*requests_tasks)
            await db_queue.put(None)
            await asyncio.gather(db_worker)
            if get_preset:
                await preset_queue.put(None)
                await asyncio.gather(preset_worker)
                await query_history_queue.put(None)
                await asyncio.gather(query_history_worker)
    await send_log_message(
        f"Завершен сбор данных по городу:\n{city[2]}\nbatch: {request_batch_no}"
    )
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
# """INSERT INTO product_data_temp(wb_id, date, size, warehouse, price, basic_price, quantity, orders, supplier_id, subject_id, brand_id, root_id)
# SELECT
#     pd.wb_id,
#     pd.date,
#     pd.size,
#     pd.warehouse,
#     pd.price,
#     pd.basic_price,
#     pd.quantity,
#     pd.orders,
#     COALESCE(sp.id, pd.supplier_id) AS supplier_id,
#     COALESCE(sub.id, pd.subject_id) AS subject_id,
#     COALESCE(br.id, pd.brand_id) AS brand_id,
#     COALESCE(rt.id, pd.root_id) AS root_id
# FROM product_data AS pd
# LEFT OUTER JOIN supplier_product AS sp ON pd.wb_id = sp.wb_id
# LEFT OUTER JOIN subject_product AS sub ON pd.wb_id = sub.wb_id
# LEFT OUTER JOIN brand_product AS br ON pd.wb_id = br.product_id
# LEFT OUTER JOIN root_product AS rt ON pd.wb_id = rt.wb_id;
# """
import asyncio
from datetime import date as Date
from typing import TYPE_CHECKING

from aiohttp import ClientSession, BasicAuth

from clickhouse_db.get_async_connection import get_async_connection
from parser.get_single_query_data import get_query_data
from service.log_alert import send_log_message
from settings import logger
from parser.save_to_db_worker import save_to_db

if TYPE_CHECKING:
    from parser.db_config_loader import ProxyConfig


async def get_r_data_q(
    http_queue: asyncio.Queue,
    db_queue,
    city,
    date,
    http_session,
    preset_queue=None,
    query_history_queue=None,
    today_date=None,
    task_no=None,
    worker_no=1,
    auth_token=None,
    proxy: "ProxyConfig" = None
):
    """
    HTTP-воркер для обработки запросов из очереди.
    
    Args:
        proxy: Конфигурация прокси для этого воркера.
    """
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
            task_no=task_no,
            worker_no=worker_no,
            auth_token=auth_token,
            proxy=proxy
        )


async def try_except_query_data(
    query_string,
    dest,
    limit,
    page,
    http_session,
    rqa=5,
    task_no=None,
    worker_no=1,
    auth_token=None,
    proxy: "ProxyConfig" = None
):
    """
    Обёртка для get_query_data с обработкой исключений.
    
    Args:
        proxy: Конфигурация прокси для запроса.
    """
    try:
        x = await get_query_data(
            http_session=http_session,
            query_string=query_string,
            dest=dest,
            limit=limit,
            page=page,
            rqa=rqa,
            timeout=5,
            task_no=task_no,
            worker_no=worker_no,
            auth_token=auth_token,
            proxy=proxy
        )
    except ValueError:
        x = {"products": []}
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
    limit=300,
    task_no=None,
    worker_no=1,
    auth_token=None,
    proxy: "ProxyConfig" = None
):
    """
    Обработка одного запроса для одной страницы.
    
    Args:
        proxy: Конфигурация прокси для запроса.
    """
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
                task_no=task_no,
                worker_no=worker_no,
                auth_token=auth_token,
                proxy=proxy
            )
            full_res = result.get("products", [])
            request_products = []
            page_increment = (page - 1) * limit
            for i, p in enumerate(full_res, 1):
                if not p.get("id"):
                    continue
                log = p.get("logs", None)
                brand_id = abs(p.get("brandId", 0) or 0)
                subject_id = abs(p.get("subjectId", 0) or 0)
                supplier_id = abs(p.get("supplierId", 0) or 0)
                natural_place = 0
                cpm = 0
                request_products.append(
                    (
                        p.get("id"),
                        city[0],
                        date[0],
                        r[0],
                        i + page_increment,
                        "b" if log else "z",
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
            if page == 1 and query_history_queue and full_res:
                total = result.get("total", 0)
                top_product = full_res[0]
                priority = top_product.get("subjectId", 0)
                if total:
                    await query_history_queue.put([(r[0], today_date, total, priority)])
            await db_queue.put(request_products)
            return
        except Exception as e:
            count += 1
            logger.critical(f"{e}")


async def get_city_result(
    city,
    date,
    requests,
    task_no,
    token: str = None,
    proxies: list["ProxyConfig"] = None,
    get_preset=False,
    test=False
):
    """
    Основная функция сбора данных по городу.
    
    Args:
        city: Кортеж (id, dest, name) города.
        date: Кортеж (id, date) даты.
        requests: Список запросов для обработки.
        task_no: Номер таски (1-4).
        token: Bearer токен для авторизации.
        proxies: Список прокси для HTTP-воркеров.
        get_preset: Флаг сбора preset данных.
        test: Флаг тестового режима.
    """
    if test:
        get_preset = False
    logger.info(f"Город {city} старт, task: {task_no}, воркеров: {len(proxies) if proxies else 0}")
    today_date = Date.today()
    await send_log_message(
        f"Начался сбор данных{'(ТЕСТОВЫЙ)' if test else ''} по городу:\n{city[2]}\ntask: {task_no}\nворкеров: {len(proxies) if proxies else 0}"
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
            # Создаём HTTP-воркеры по числу прокси (каждый воркер использует свой прокси)
            num_workers = len(proxies) if proxies else 1
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
                        task_no=task_no,
                        worker_no=i,
                        auth_token=token,
                        proxy=proxies[i] if proxies else None
                    )
                )
                for i in range(num_workers)
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
                            f"Осталось запросов в task {task_no}: {len(requests_list)}"
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
        f"Завершен сбор данных по городу:\n{city[2]}\ntask: {task_no}"
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
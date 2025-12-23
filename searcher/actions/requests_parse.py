import asyncio
from datetime import datetime, date
import pytz
from celery.exceptions import SoftTimeLimitExceeded

from parser.aggregate_supplier import aggregate_supplier
from parser.collect_subjects import collect_subject_ids_names
from parser.get_init_data import (
    get_cities_data,
    get_dates_data,
    write_new_date,
    get_requests_data, get_requests_id_download_data,
)
from parser.get_query_subject import get_queries_subjects
from parser.parser_main import get_city_result
from parser.optimize_tables import optimize_table_final, optimize_request_product_partition
from parser.db_config_loader import (
    load_proxies_from_db,
    load_tokens_from_db,
    distribute_proxies,
    split_requests,
    ProxyConfig,
)
from celery_main import celery_app
from server.funcs.transfer_to_local import transfer_aggregates_to_local
from settings import logger, NUM_SEARCH_TASKS


@celery_app.task(name="process_search_task", time_limit=3600 * 18)
def process_search_task(
    city: tuple,
    date_: tuple,
    requests: list,
    task_no: int,
    token: str,
    proxies: list[dict],
    test: bool = False
):
    """
    Celery-таска для сбора данных поисковой выдачи.
    
    Args:
        city: Кортеж (id, dest, name) города.
        date_: Кортеж (id, date) даты.
        requests: Список запросов для обработки.
        task_no: Номер таски (1-4).
        token: Bearer токен для авторизации.
        proxies: Список прокси для HTTP-воркеров.
        test: Флаг тестового режима.
    """
    start_time = datetime.now()
    logger.info(f"Вход в search task {task_no}: {city}, запросов: {len(requests)}, прокси: {len(proxies)}")
    
    # Преобразуем словари обратно в ProxyConfig
    proxy_configs = [
        ProxyConfig(
            proxy_url=p["proxy_url"],
            proxy_user=p["proxy_user"],
            proxy_pass=p["proxy_pass"]
        )
        for p in proxies
    ]
    
    try:
        asyncio.run(
            get_city_result(
                city,
                date_,
                requests,
                task_no,
                token=token,
                proxies=proxy_configs,
                get_preset=True if city[0] == 1 else False,
                test=test
            )
        )
        end_time = datetime.now()
        delta = (end_time - start_time).seconds
        logger.info(
            f"Task {task_no} - Старт парса: {start_time.strftime('%H:%M %d.%m.%Y')}\n"
            f"Завершение парса: {end_time.strftime('%H:%M %d.%m.%Y')}\n"
            f"Выполнено за: {delta // 60 // 60} часов, {delta // 60 % 60} минут"
        )
    except SoftTimeLimitExceeded:
        logger.info(f"Превышен лимит времени: город: {city} таска: {task_no}")


@celery_app.task(name="fire_requests")
def fire_requests(city_no, test):
    """
    Главная функция запуска сбора данных.
    Загружает прокси и токены из БД, разделяет запросы между 4 тасками.
    """
    # === Логика работы с датами (сохранена) ===
    today = datetime.now(tz=pytz.utc).date()
    last_date = asyncio.run(get_dates_data())
    if not last_date:
        today_date = (1, today)
        asyncio.run(write_new_date(today_date))
    else:
        if isinstance(last_date[0][1], str):
            last_date[0][1] = date.fromisoformat(last_date[0][1])
        if last_date[0][1] == today:
            today_date = last_date[0]
        else:
            today_date = (last_date[0][0] + 1, today)
            asyncio.run(write_new_date(today_date))
    
    # === Загрузка данных ===
    cities = asyncio.run(get_cities_data(city_no))
    city = cities[0]
    
    # Загрузка запросов (лимит 1 млн, сохранён)
    requests = asyncio.run(get_requests_data())
    logger.info(f"Загружено {len(requests)} запросов")
    
    # Загрузка прокси и токенов из ClickHouse
    proxies = asyncio.run(load_proxies_from_db())
    tokens = asyncio.run(load_tokens_from_db(limit=NUM_SEARCH_TASKS))
    
    if len(tokens) < NUM_SEARCH_TASKS:
        logger.error(f"Недостаточно токенов: {len(tokens)} < {NUM_SEARCH_TASKS}")
        return
    
    if not proxies:
        logger.error("Нет доступных прокси!")
        return
    
    # === Распределение ресурсов между тасками ===
    # Разделить запросы на 4 равные части
    request_batches = split_requests(requests, NUM_SEARCH_TASKS)
    
    # Распределить прокси между тасками (по ~25 на каждую)
    proxy_batches = distribute_proxies(proxies, NUM_SEARCH_TASKS)
    
    logger.info(
        f"Распределение: {NUM_SEARCH_TASKS} тасок, "
        f"запросов на таску: {[len(b) for b in request_batches]}, "
        f"прокси на таску: {[len(b) for b in proxy_batches]}"
    )
    
    # === Запуск Celery-тасок ===
    for task_no in range(NUM_SEARCH_TASKS):
        if request_batches[task_no]:
            # Преобразуем ProxyConfig в dict для сериализации Celery
            proxy_dicts = [
                {
                    "proxy_url": p.proxy_url,
                    "proxy_user": p.proxy_user,
                    "proxy_pass": p.proxy_pass
                }
                for p in proxy_batches[task_no]
            ]
            
            process_search_task.delay(
                city=city,
                date_=today_date,
                requests=request_batches[task_no],
                task_no=task_no + 1,  # 1-based numbering
                token=tokens[task_no],
                proxies=proxy_dicts,
                test=test
            )


@celery_app.task(name="optimize_table", time_limit=3600 * 2)
def optimize_table(table_name):
    asyncio.run(optimize_table_final(table_name=table_name))


@celery_app.task(name="optimize_request_product", time_limit=3600 * 2)
def optimize_request_product():
    asyncio.run(optimize_request_product_partition())



@celery_app.task(name="process_request_batch", time_limit=3600 * 8)
def process_request_batch(left, right):
    start_time = datetime.now()
    logger.info(f"Вход в search subjects")
    try:
        asyncio.run(
            get_queries_subjects(
                left=left,
                right=right,
            )
        )
        end_time = datetime.now()
        delta = (end_time - start_time).seconds
        logger.info(
            f"Старт парса: {start_time.strftime('%H:%M %d.%m.%Y')}\n"
            f"Завершение парса: {end_time.strftime('%H:%M %d.%m.%Y')}\n"
            f"Выполнено за: {delta // 60 // 60} часов, {delta // 60 % 60} минут"
        )
    except SoftTimeLimitExceeded:
        logger.info(f"Превышен лимит времени")



@celery_app.task(name="fire_request_subject")
def fire_requests_subject():
    process_request_batch.delay(0, 2000000)
    process_request_batch.delay(2000000, 4000000)
    process_request_batch.delay(4000000, 6000000)
    process_request_batch.delay(6000000, 8000000)
    process_request_batch.delay(8000000, 12000000)


@celery_app.task(name="get_today_subjects_dict")
def get_today_subjects_dict():
    asyncio.run(collect_subject_ids_names())


@celery_app.task(name="transfer_aggregates")
def transfer_aggregates():
    asyncio.run(transfer_aggregates_to_local())



@celery_app.task(name="aggregate_supplier_task")
def aggregate_supplier_task(start_date=None):
    asyncio.run(aggregate_supplier(start_date=start_date))


import asyncio
from datetime import datetime, date
import pytz
from celery.exceptions import SoftTimeLimitExceeded
from parser.get_init_data import get_cities_data, get_dates_data, write_new_date, get_requests_data
from parser.parser_main import get_city_result
from celery_main import celery_app
from settings import logger


@celery_app.task(name="process_city", time_limit=3600 * 5)
def process_city(city, date_, requests, batch_no):
    start_time = datetime.now()
    logger.info(f"Вход в search: {city}")
    try:
        asyncio.run(get_city_result(city, date_, requests, batch_no, get_preset=True if city[0] == 1 else False))
        end_time = datetime.now()
        delta = (end_time - start_time).seconds
        logger.info(
            f"Старт парса: {start_time.strftime('%H:%M %d.%m.%Y')}\n"
            f"Завершение парса: {end_time.strftime('%H:%M %d.%m.%Y')}\n"
            f"Выполнено за: {delta // 60 // 60} часов, {delta // 60 % 60} минут"
        )
    except SoftTimeLimitExceeded:
        logger.info(f"Превышен лимит времени: город: {city} батч: {batch_no}")


@celery_app.task(name="fire_requests")
def fire_requests(city_no):
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
    cities = asyncio.run(get_cities_data(city_no))
    requests = asyncio.run(get_requests_data())
    request_batches = []
    batch_size = 250000
    prev = 0
    for r_id in range(batch_size, len(requests) + batch_size, batch_size):
        request_batches.append(requests[prev:r_id])
        prev = r_id
    city = cities[0]
    for i, r_batch in enumerate(request_batches, 1):
        process_city.delay(city, today_date, r_batch, i)

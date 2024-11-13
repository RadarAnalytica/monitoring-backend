import asyncio
from datetime import datetime
import pytz

from parser.get_init_data import get_cities_data, get_dates_data, write_new_date, get_requests_data
from parser.parser_main import get_city_result
from celery_main import celery_app
from settings import logger


@celery_app.task(name="process_city")
def process_city(city, date, requests, batch_no):
    start_time = datetime.now()
    logger.info(f"Вход в search: {city}")
    asyncio.run(get_city_result(city, date, requests, batch_no))
    end_time = datetime.now()
    delta = (end_time - start_time).seconds
    logger.info(
        f"Старт парса: {start_time.strftime('%H:%M %d.%m.%Y')}\n"
        f"Завершение парса: {end_time.strftime('%H:%M %d.%m.%Y')}\n"
        f"Выполнено за: {delta // 60 // 60} часов, {delta // 60 % 60} минут"
    )


@celery_app.task(name="fire_requests")
def fire_requests():
    today = datetime.now(tz=pytz.utc).date()
    last_date = asyncio.run(get_dates_data())
    if not last_date:
        today_date = (1, today)
        asyncio.run(write_new_date(today_date))
    else:
        if last_date[0][1] == today:
            today_date = last_date[0]
        else:
            today_date = (last_date[0][0] + 1, today)
            asyncio.run(write_new_date(today_date))
    cities = asyncio.run(get_cities_data())
    requests = asyncio.run(get_requests_data())
    request_batches = (tuple(requests[:500000]), tuple(requests[500000:]))
    for city in cities:
        for i, r_batch in enumerate(request_batches, 1):
            process_city.delay(city, today_date, r_batch, i)

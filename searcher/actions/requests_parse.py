import asyncio
from datetime import datetime
import pytz
from parser.get_init_data import get_cities_data, get_dates_data, write_new_date
from parser.parser_main import get_city_result
from celery_main import celery_app
from settings import logger


@celery_app.task(name="process_city")
def process_city(city, date):
    start_time = datetime.now()
    logger.info(f"Вход в search: {city}")
    asyncio.run(get_city_result(city, date))
    end_time = datetime.now()
    delta = (end_time - start_time).seconds
    logger.info(
        f"Старт парса: {start_time.strftime('%H:%M %d.%m.%Y')}\n"
        f"Завершение парса: {end_time.strftime('%H:%M %d.%m.%Y')}\n"
        f"Выполнено за: {delta // 60 // 60} часов, {delta // 60 % 60} минут"
    )


@celery_app.task(name="fire_requests")
def fire_requests():
    today = datetime.now(tz=pytz.timezone("Europe/Moscow")).date()
    last_date = asyncio.run(get_dates_data())
    if not last_date:
        today = (1, today)
    else:
        if last_date[0][1] == today:
            today = last_date
        else:
            today = (last_date[0] + 1, today)
            asyncio.run(write_new_date(today))
    cities = asyncio.run(get_cities_data())
    for city in cities:
        process_city.delay(city, today)

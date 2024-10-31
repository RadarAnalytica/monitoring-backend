import asyncio
from datetime import datetime
import pytz
from parser.get_init_data import get_cities_data
from parser.parser_main import get_city_result
from celery_main import celery_app
from settings import logger


@celery_app.task(name="process_city")
def process_city(city, date):
    start_time = datetime.now()
    logger.info(f"Вход в search: {city}")
    asyncio.run(get_city_result(city, date))
    end_time = datetime.now()
    delta = (start_time - end_time).seconds
    logger.info(
        f"Старт парса: {start_time.strftime('%H:%M %d.%m.%Y')}\n"
        f"Завершение парса: {end_time.strftime('%H:%M %d.%m.%Y')}\n"
        f"Выполнено за: {delta // 60 // 60} часов, {delta // 60} минут"
    )


@celery_app.task(name="fire_requests")
def fire_requests():
    today = datetime.now(tz=pytz.timezone("Europe/Moscow")).date()
    cities = asyncio.run(get_cities_data())
    for city in cities:
        process_city.delay(city, today)

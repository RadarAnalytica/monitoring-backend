import asyncio
from datetime import datetime
import pytz
from parser.get_init_data import get_cities_data, get_requests_data
from parser.parser_main import get_city_result
from celery_main import celery_app
from settings import logger


@celery_app.task(name="process_city")
def process_city(city, requests, requests_batch_no, date):
    start_time = datetime.now()
    logger.info(f"Вход в search: {city}, batch: {requests_batch_no}")
    asyncio.run(get_city_result(city, requests, date))
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
    cities = asyncio.run(get_cities_data())
    requests = [r for r in asyncio.run(get_requests_data()) if not r.isdigit()]
    requests_batches = [requests[:250000], requests[250000:500000], requests[500000:750000], requests[750000:]]
    for city in cities:
        for i, batch in enumerate(requests_batches):
            process_city.delay(city, batch, i, today)

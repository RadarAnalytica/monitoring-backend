import os

from celery import Celery
from celery.schedules import crontab
from settings import REDIS_HOST, REDIS_PORT

celery_app = Celery(
    "searcher",
    include=[
        "actions.requests_parse",
    ],
)

celery_app.conf.broker_url = os.environ.get(
    "CELERY_BROKER_URL", f"redis://{REDIS_HOST}:{REDIS_PORT}"
)
celery_app.conf.result_backend = os.environ.get(
    "CELERY_RESULT_BACKEND", f"redis://{REDIS_HOST}:{REDIS_PORT}"
)
celery_app.conf.broker_connection_retry_on_startup = True

celery_app.conf.beat_schedule = {
    "parse_search_moscow": {
        "task": "fire_requests",
        "schedule": crontab(hour="22", minute="40",),
        "args": (1,)
    },
    # "parse_search_krasnodar": {
    #     "task": "fire_requests",
    #     "schedule": crontab(hour="23", minute="55",),
    #     "args": (2,)
    # },
    # "parse_search_ekaterinburg": {
    #     "task": "fire_requests",
    #     "schedule": crontab(hour="12", minute="0",),
    #     "args": (3,)
    # },
    # "parse_search_vladivostok": {
    #     "task": "fire_requests",
    #     "schedule": crontab(hour="6", minute="0",),
    #     "args": (4,)
    # }

}

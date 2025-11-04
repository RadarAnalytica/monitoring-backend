import os
import asyncio
from celery import Celery, signals
from celery.schedules import crontab
from settings import REDIS_HOST, REDIS_PORT
from service.log_alert import send_log_message
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
celery_app.conf.result_expires = 15
celery_app.conf.broker_connection_retry_on_startup = True
celery_app.conf.task_default_expires = 300

celery_app.conf.beat_schedule = {
    "parse_search_moscow": {
        "task": "fire_requests",
        "schedule": crontab(
            hour="16",
            minute="25",
        ),
        "args": (1, False),
    },
    "optimize_preset": {
        "task": "optimize_table",
        "schedule": crontab(
            hour="7",
            minute="35",
        ),
        "args": ("preset",),
    },
    "optimize_latest_partition": {
        "task": "optimize_request_product",
        "schedule": crontab(
            hour="8",
            minute="0",
        ),
    },
    # "test_parse_search_moscow": {
    #     "task": "fire_requests",
    #     "schedule": crontab(
    #         hour="15",
    #         minute="14",
    #     ),
    #     "args": (1, True),
    # },
    # "parse_search_krasnodar": {
    #     "task": "fire_requests",
    #     "schedule": crontab(hour="18", minute="0",),
    #     "args": (2,)
    # },
    # "parse_search_ekaterinburg": {
    #     "task": "fire_requests",
    #     "schedule": crontab(hour="12", minute="0",),
    #     "args": (3,)
    # },
    # "parse_search_vladivostok": {
    #     "task": "fire_requests",
    #     "schedule": crontab(hour="0", minute="1",),
    #     "args": (4,)
    # }
    "get_today_subjects_dict": {
        "task": "get_today_subjects_dict",
        "schedule": crontab(hour="5", minute="40",),
    },
}

@signals.task_failure.connect
def task_failure_handler(*other_args, sender=None, task_id=None, exception=None, args=None, kwargs=None, **other_kwargs):
    message = f"Задача '{sender.name}' task_id: {task_id}\nзавершилась с ошибкой:\n{exception}\n\nАргументы:\n{args}\n\nКлючевые аргументы:\n{kwargs}"
    asyncio.run(send_log_message(message))


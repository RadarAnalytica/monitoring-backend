from functools import wraps
from aiogram import Bot
from settings import BOT_TOKEN, ADMINS, logger, SERVICE_NAME, LOG_CHAT_ID
from httpx import AsyncClient
from asyncio import sleep as asleep
import traceback


if not BOT_TOKEN:
    logger.warning("BOT_TOKEN is not set, log alerts will be disabled.")


async def send_log_message(message: str, ex: Exception | None = None, chat_type: str = 'discussion'):
    try:
        message = f"Сервер: {SERVICE_NAME}\n\n{message}"
        if BOT_TOKEN:
            if ex:
                message = f"{message}\nОшибка:\n{ex}"
            async with AsyncClient() as client:
                await client.post(
                    url=f'https://api.pachca.com/api/shared/v1/messages',
                    headers={
                        'Authorization': f'Bearer {BOT_TOKEN}',
                        'Content-Type': 'application/json'
                    },
                    json={
                        'message': {
                            'entity_type': chat_type,
                            'entity_id': LOG_CHAT_ID,
                            'content': message
                        }
                    },
                    timeout=5.0
                )
        else:
            pass
    except:
        print(traceback.format_exc())



def log_alert(message=None, track_error=False, end_message=None):

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                await asleep(1)
            except:
                pass
            if message and isinstance(message, str):
                await send_log_message(message)
            try:
                result = await func(*args, **kwargs)
            except Exception as e:
                try:
                    if track_error:
                        await send_log_message(
                            f"❗Исключение❗\n\nФункция: {func.__module__}.{func.__name__}\n\n",
                            ex=e,
                        )
                except Exception as ex:
                    logger.exception(ex)
                    pass
                raise e
            if end_message and isinstance(end_message, str):
                await send_log_message(end_message)
            return result

        return wrapper

    return decorator


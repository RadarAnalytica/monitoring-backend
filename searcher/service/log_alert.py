from functools import wraps
from aiogram import Bot
from settings import BOT_TOKEN, ADMINS

if BOT_TOKEN:
    bot = Bot(BOT_TOKEN)
else:
    bot = None


async def send_log_message(message: str, ex: Exception | None = None):
    if bot:
        for admin in ADMINS:
            if ex:
                await bot.send_message(admin, f"Мониторинг\n{message}\nОшибка: {ex}")
            else:
                await bot.send_message(admin, f"Мониторинг\nСообщение: {message}")
    return


def log_alert(message=None, track_error=False):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            if message:
                await send_log_message(message)
            try:
                result = await func(*args, **kwargs)
            except Exception as e:
                if track_error:
                    await send_log_message(f"❗Исключение❗\n\nФункция: {func.__module__}.{func.__name__}\n\n", ex=e)
                raise e
            return result

        return wrapper

    return decorator
from functools import wraps
from aiogram import Bot
from settings import BOT_TOKEN, ADMINS, logger
from asyncio import sleep as asleep


async def send_log_message(message: str, ex: Exception | None = None):
    """
    Отправляет сообщение администраторам через Telegram.
    Бот создаётся локально для совместимости с Celery (избегаем проблем с закрытой сессией).
    """
    if not BOT_TOKEN:
        return
    
    async with Bot(BOT_TOKEN) as bot:
        for admin in ADMINS:
            try:
                if ex:
                    await bot.send_message(
                        admin, f"Мониторинг\n{message}\nОшибка: {ex}"
                    )
                else:
                    await bot.send_message(admin, f"Мониторинг\nСообщение: {message}")
            except Exception as e:
                logger.error(f"Ошибка отправки сообщения админу {admin}: {e}")
    return


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


import asyncio
import traceback
from json import JSONDecodeError
from typing import TYPE_CHECKING

from aiohttp import ClientSession, ContentTypeError, client_exceptions, BasicAuth

from settings import SEARCH_URL, logger

if TYPE_CHECKING:
    from parser.db_config_loader import ProxyConfig


async def get_query_data(
    query_string,
    dest,
    limit,
    page,
    rqa=5,
    timeout=10,
    upload=False,
    task_no=None,
    worker_no=None,
    auth_token=None,
    proxy: "ProxyConfig" = None
):
    """
    Выполняет запрос к API Wildberries для получения данных поисковой выдачи.
    
    Args:
        query_string: Поисковый запрос.
        dest: ID города/назначения.
        limit: Лимит результатов на страницу.
        page: Номер страницы.
        rqa: Количество попыток.
        timeout: Таймаут запроса.
        upload: Флаг загрузки (влияет на retry логику).
        task_no: Номер Celery-таски.
        worker_no: Номер воркера.
        auth_token: Bearer токен для авторизации.
        proxy: Конфигурация прокси (ProxyConfig).
    
    Returns:
        Словарь с данными ответа API.
    """
    _data = {"products": []}
    counter = 0
    headers = {
        "Authorization": auth_token,
    }
    
    # Настройка прокси
    proxy_url = None
    proxy_auth = None
    if proxy:
        proxy_url = proxy.proxy_url
        proxy_auth = BasicAuth(proxy.proxy_user, proxy.proxy_pass)
    else:
        logger.warning(f"Proxy is None for worker {worker_no}, task {task_no}")
    
    while len(_data.get("products", [])) < 2 and counter < rqa:
        counter += 1
        try:
            # Новая сессия на каждый запрос для избежания fingerprinting
            async with ClientSession() as http_session:
                async with http_session.get(
                    url=SEARCH_URL,
                    params={
                        "resultset": "catalog",
                        "query": query_string,
                        "limit": limit,
                        "dest": dest,
                        "page": page,
                        "ab_testing": "false",
                        "appType": 64
                    },
                    headers=headers if auth_token else None,
                    timeout=timeout,
                    proxy=proxy_url,
                    proxy_auth=proxy_auth,
                ) as response:
                    if response.status == 200:
                        try:
                            _data = await response.json(content_type="text/plain")
                            products_count = len(_data.get("products", []))
                            if products_count >= 2:
                                logger.debug(f"Успешно получено {products_count} продуктов")
                        except (ContentTypeError, JSONDecodeError):
                            logger.critical("ОШИБКА КОНТЕНТ ТАЙП!!!")
                            return _data
                    else:
                        logger.warning(f"HTTP {response.status}: {response.reason}")
                        await asyncio.sleep(1)
                        continue
        except (TypeError, asyncio.TimeoutError) as e:
            logger.critical(f"ОШИБКА, {type(e)}")
            await asyncio.sleep(1)
            continue
        except client_exceptions.ServerDisconnectedError:
            logger.info("SERVER DISCONNECT")
            if not upload:
                counter -= 1
            await asyncio.sleep(1)
            continue
        except:
            await asyncio.sleep(1)
            print(traceback.format_exc())

    return _data


async def test():
    async with ClientSession() as session:
        res = await get_query_data(session, "джинсы женские", -1257786, 300, 4, batch_no=1)
        p = res.get("products", [])
        print(len(p))
        if len(p) == 0:
            print(res)
        for product in res.get("products"):
            print(f"{product.get("id")}: {bool(product.get("logs"))},")


# взято с https://user-geo-data.wildberries.ru/get-geo-info?latitude=[ШИРОТА float]&longitude=[ДОЛГОТА float]
# {"Москва": -1257786, "Краснодар": 12358063, "Екатеринбург": -5817698, "Владивосток": 123587791}

if __name__ == "__main__":
    asyncio.run(test())

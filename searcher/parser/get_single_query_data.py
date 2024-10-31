import asyncio
from json import JSONDecodeError

from aiohttp import ClientSession, ContentTypeError, client_exceptions

from settings import SEARCH_URL, logger


async def get_query_data(
    http_session: ClientSession, query_string, dest, limit, page, rqa=5, timeout=5
):
    _data = {"data": {"products": []}}
    counter = 0
    while len(_data.get("data", {}).get("products", [])) < 2 and counter < rqa:
        counter += 1
        try:
            async with http_session.get(
                url=SEARCH_URL,
                params={
                    "resultset": "catalog",
                    "query": query_string,
                    "limit": limit,
                    "dest": dest,
                    "page": page,
                },
                timeout=timeout,
            ) as response:
                if response.ok:
                    try:
                        _data = await response.json(content_type="text/plain")
                    except (ContentTypeError, JSONDecodeError):
                        return _data.get("data")
                else:
                    # logger.critical("response not ok")
                    continue
        except (TypeError, asyncio.TimeoutError) as e:
            # logger.critical(f"ОШИБКА, {type(e)}")
            continue
        except client_exceptions.ServerDisconnectedError:
            counter -= 1
            continue

    return _data.get("data")


# взято с https://user-geo-data.wildberries.ru/get-geo-info?latitude=[ШИРОТА float]&longitude=[ДОЛГОТА float]
# Москва -1257786
# Краснодар 12358063
# Екатеринбург -5817698
# Владивосток 123587791

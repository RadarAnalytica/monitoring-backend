import asyncio
import traceback
from json import JSONDecodeError

from aiohttp import ClientSession, ContentTypeError, client_exceptions, BasicAuth

from settings import SEARCH_URL, logger, PROXY_AUTH, PROXIES


async def get_query_data(
    http_session: ClientSession,
    query_string,
    dest,
    limit,
    page,
    rqa=5,
    timeout=10,
    upload=False,
    batch_no=None,
    worker_no=None,
    auth_token=None
):
    _data = {"products": []}
    counter = 0
    headers = {
        "Authorization": auth_token,
    }
    while len(_data.get("products", [])) < 2 and counter < rqa:
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
                    "ab_testing": "false",
                    "appType": 64
                },
                headers=headers if auth_token else None,
                timeout=timeout,
                proxy=PROXIES[(batch_no - 1) * 20 + page + (4 * worker_no)] if not (worker_no is None) else None,
                proxy_auth=BasicAuth(PROXY_AUTH["username"], PROXY_AUTH["password"]) if not (worker_no is None) else None,
            ) as response:
                print(response.status, response.reason)
                if response.status == 200:
                    try:
                        _data = await response.json(content_type="text/plain")
                    except (ContentTypeError, JSONDecodeError):
                        logger.critical("ОШИБКА КОНТЕНТ ТАЙП!!!")
                        return _data
                else:
                    # logger.critical("response not ok")
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

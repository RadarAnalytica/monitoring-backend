import asyncio
import traceback
from json import JSONDecodeError

from aiohttp import ClientSession, ContentTypeError, client_exceptions

from settings import SEARCH_URL, logger, WB_AUTH_TOKENS


async def get_query_data(
    http_session: ClientSession, query_string, dest, limit, page, rqa=5, timeout=5, upload=False, batch_no=None
):
    _data = {"products": []}
    counter = 0
    headers = {
        "Authorization": f"Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3NjE5MTkxNTUsInVzZXIiOiIxMTI0NjIxNjAiLCJzaGFyZF9rZXkiOiIxNyIsImNsaWVudF9pZCI6IndiIiwic2Vzc2lvbl9pZCI6IjRjZmNhN2ExMjEwNjQ3MzRhYjNiZTU2ZmQyMmNkMWViIiwidmFsaWRhdGlvbl9rZXkiOiI1OGNmNDQyNTA4MTNkM2ZmZDhkNmI0YzI4NmZmNGQyMTg5ZjlkMTY3NjBmNWZiYmJlN2Y3ZDY3YjY1MDNmNjlkIiwicGhvbmUiOiJ1NXlqZThTN29FelpONVE0dFNiVmt3PT0iLCJ1c2VyX3JlZ2lzdHJhdGlvbl9kdCI6MTY4MzgwNTc4NCwidmVyc2lvbiI6Mn0.c4cptVXS5x_pj1t62eB3vTNBRrWUYgxxVpzDIy0eWABN95lqQ81_nCNMtgy5utjG57qcoqeJR3mgAJW8uT3crdMMvmnKfHkWSUcqeYueAR9xkuuFG80Mpex019UJSww9q533noDF0PtFPgEMYtsi7f2AAC0jf_jBKNG_6PtIeq1IcrrfNFKP0yfkCD9CW_Gws1XpFpq_hozpxRXuyA9FMrWe-osl72aM1aNw8Dl66lrDr8LQAUN1pTPxnJxQRWEirqjZl-UScPZpJ1xBWgB1VQUGnvEgqU5mPFfzJ-sVQGE_hI-TqvpRtPoUI3mR3FS234e4zzshtCGPGd28k6zdhw",
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
                headers=headers,
                timeout=timeout,
            ) as response:
                if response.status == 200:
                    try:
                        _data = await response.json(content_type="text/plain")
                    except (ContentTypeError, JSONDecodeError):
                        logger.critical("ОШИБКА КОНТЕНТ ТАЙП!!!")
                        return _data
                else:
                    # logger.critical("response not ok")
                    continue
        except (TypeError, asyncio.TimeoutError) as e:
            logger.critical(f"ОШИБКА, {type(e)}")
            continue
        except client_exceptions.ServerDisconnectedError:
            logger.info("SERVER DISCONNECT")
            if not upload:
                counter -= 1
            continue
        except:
            print(traceback.format_exc())

    return _data


async def test():
    async with ClientSession() as session:
        res = await get_query_data(session, "джинсы женские", -1257786, 20, 1, batch_no=1)
        for product in res.get("products"):
            print(f"{product.get("id")}: {bool(product.get("logs"))},")


# взято с https://user-geo-data.wildberries.ru/get-geo-info?latitude=[ШИРОТА float]&longitude=[ДОЛГОТА float]
# {"Москва": -1257786, "Краснодар": 12358063, "Екатеринбург": -5817698, "Владивосток": 123587791}

if __name__ == "__main__":
    asyncio.run(test())

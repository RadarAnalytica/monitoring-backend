import json

from aiohttp import ClientSession

from clickhouse_db.get_async_connection import get_async_connection
from settings import logger

def unnest_subjects_list(subjects_list: list):
    result = list()
    for subject_data in subjects_list:
        s_id = subject_data.get("id")
        s_name = subject_data.get("name", "")
        children = subject_data.get("childs", [])
        result.append((s_id, s_name))
        result.extend(unnest_subjects_list(children))
    return result


async def get_today_subjects_id_name():
    url = "https://static-basket-01.wbcontent.net/vol0/data/subject-base.json"
    async with ClientSession() as http_session:
        async with http_session.get(url) as resp:
            result = await resp.json()
    subjects_list = unnest_subjects_list(result)
    return subjects_list


async def collect_subject_ids_names():
    logger.info("Загружаю предметы")
    subjects_data_list = await get_today_subjects_id_name()
    async with get_async_connection() as client:
        await client.insert(table="subjects_dict", column_names=["id", "name"], data=subjects_data_list)
        await client.command("OPTIMIZE TABLE subjects_dict FINAL")
    logger.info("Предметы есть")


async def get_today_subjects_raw():
    url = "https://static-basket-01.wbcontent.net/vol0/data/subject-base.json"
    async with ClientSession() as http_session:
        async with http_session.get(url) as resp:
            result = await resp.json()
    return result


async def write_subjects_raw():
    subjects = await get_today_subjects_raw()
    data = (('subjects', json.dumps(subjects)),)
    async with get_async_connection() as client:
        await client.insert(table="json_store_string", data=data, column_names=["name", "data"])
    logger.info("RAW SUBJECTS FINISH")
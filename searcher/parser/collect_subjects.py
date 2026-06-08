import json

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


async def _get_subjects_raw():
    async with get_async_connection() as client:
        q = await client.query(
            "SELECT data FROM json_store_string WHERE name = 'subjects' "
            "ORDER BY created_at DESC LIMIT 1"
        )
        if not q.result_rows:
            return []
    return json.loads(q.result_rows[0][0])


async def get_today_subjects_id_name():
    result = await _get_subjects_raw()
    return unnest_subjects_list(result)


async def collect_subject_ids_names():
    logger.info("Загружаю предметы")
    subjects_data_list = await get_today_subjects_id_name()
    async with get_async_connection() as client:
        await client.insert(
            table="subjects_dict", column_names=["id", "name"], data=subjects_data_list
        )
        await client.command("OPTIMIZE TABLE subjects_dict FINAL")
    logger.info("Предметы есть")


async def get_today_subjects_raw():
    return await _get_subjects_raw()


async def write_subjects_raw():
    subjects = await get_today_subjects_raw()
    data = (("subjects", json.dumps(subjects)),)
    async with get_async_connection() as client:
        await client.insert(
            table="json_store_string", data=data, column_names=["name", "data"]
        )
        await client.command("OPTIMIZE TABLE json_store_string FINAL")
    logger.info("RAW SUBJECTS FINISH")

import aiohttp
import asyncio
import os
import pandas as pd
from datetime import datetime, timedelta, date
from clickhouse_db.get_async_connection import get_async_connection
from clickhouse_connect.driver import AsyncClient

PUBLIC_URL = "https://disk.yandex.ru/d/MOPKk8rA-MO9sw"
SAVE_DIR = "downloads_patch"
TABLE_NAME = "request_frequency"

os.makedirs(SAVE_DIR, exist_ok=True)

async def get_files_list():
    url = "https://cloud-api.yandex.net/v1/disk/public/resources"
    params = {"public_key": PUBLIC_URL, "limit": 1000}  # Можно увеличить limit
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as response:
            if response.status == 200:
                data = await response.json()
                items = data.get("_embedded", {}).get("items", [])
                filenames = {item["name"]: item["file"] for item in items if item["type"] == "file"}
                return filenames
            else:
                print(f"❌ Ошибка {response.status}: {await response.text()}")
                return dict()

async def get_direct_link(session, filename):
    url = "https://cloud-api.yandex.net/v1/disk/public/resources/download"
    params = {"public_key": PUBLIC_URL, "path": filename}
    async with session.get(url, params=params) as response:
        if response.status == 200:
            return filename, (await response.json())["href"]
        else:
            print(f"❌ Ошибка при получении ссылки {filename}: {await response.text()}")
            raise ValueError

async def download_file(session, filename, direct_link):
    save_path = os.path.join(SAVE_DIR, filename)
    async with session.get(direct_link) as response:
        if response.status == 200:
            with open(save_path, "wb") as f:
                async for chunk in response.content.iter_chunked(1024):
                    f.write(chunk)
            return filename
    print(f"❌ Ошибка при скачивании {filename}: {response.status}")
    return None

async def load_to_clickhouse(filename: str):
    file_path = os.path.join(SAVE_DIR, filename)
    if not os.path.exists(file_path):
        return
    file_date = date.fromisoformat(filename.replace(".csv", ""))
    update_time = datetime(year=file_date.year, month=file_date.month, day=file_date.day, hour=0, minute=0, second=0, microsecond=0)
    start_week = file_date - timedelta(days=6)
    df = pd.read_csv(file_path)
    df["parse_date"] = pd.to_datetime(df["parse_date"]).dt.date
    print(filename)
    new_records = []
    new_queries = []
    main_dict = dict()
    print("getting queries")
    for _, row in df.iterrows():
        request_str = str(row["search_words"]).strip().lower()
        if not request_str:
            continue
        total_weekly = row["request_count"]
        if not total_weekly:
            continue
        file_date = row["parse_date"]
        main_dict[request_str] = (total_weekly, file_date)
    queries = tuple(main_dict.keys())
    queries_parts = []
    step = 1000
    for i in range(1001):
        queries_parts.append(queries[i * step: (step * i) + step])
    query_1 = f"""SELECT r.query, r.id, rf.frequency 
        FROM request as r 
        JOIN (
            SELECT query_id as query_id, sum(frequency) as frequency 
            FROM {TABLE_NAME} 
            WHERE 
                date BETWEEN '{str(start_week)}' 
                AND '{str(file_date - timedelta(days=1))}' 
            GROUP BY query_id
        ) as rf ON rf.query_id = r.id 
        WHERE query IN %(v1)s"""
    query_2 = "SELECT max(id) FROM request"
    print("getting query ids")
    query_ids = dict()
    async with get_async_connection() as client:
        client: AsyncClient = client
        for queries_part in queries_parts:
            if not queries_part:
                continue
            params = {
                "v1": queries_part
            }
            query_ids_query = await client.query(query_1, parameters=params)
            query_ids_temp = {row[0]: (row[1], row[2]) for row in query_ids_query.result_rows}
            query_ids.update(query_ids_temp)
        max_query_id_query = await client.query(query_2)
        max_query_id = max_query_id_query.result_rows[0][0] + 1 if max_query_id_query.result_rows and max_query_id_query.result_rows[0] else 1
        print(max_query_id)
    new_query_id_increment = 0
    for request_str, dt_tuple in main_dict.items():
        total_weekly, file_date = dt_tuple
        db_tuple = query_ids.get(request_str, (None, None))
        query_id = db_tuple[0]
        prev_total = db_tuple[1]
        if not query_id:
            query_id = max_query_id + new_query_id_increment
            new_query_id_increment += 1
        new_queries.append((query_id, request_str, total_weekly, update_time))
        if not prev_total:
            mid_quantity = total_weekly // 7
            new_records.extend(
                [
                    (start_week + timedelta(days=i), query_id, mid_quantity or 0) for i in range(7)
                ]
            )
        else:
            day_value = total_weekly - prev_total
            if day_value < 0:
                day_value = 0
            new_records.append((file_date, query_id, day_value or 0))

    async with get_async_connection() as client:
        if new_records:
            await client.insert(TABLE_NAME, new_records, column_names=["date", "query_id", "frequency"])
        else:
            print(f"⚠️ Пустой файл: {filename}")
        if new_queries:
            await client.insert("request", new_queries, column_names=["id", "query", "quantity", "updated"])
        print(f"✅ Загружено в ClickHouse: {filename}")

async def main():
    filenames = await get_files_list()
    downloaded_files = []
    async with aiohttp.ClientSession() as session:
        files_to_dl = list(filenames.items())
        files_to_dl_parts = []
        step = 3
        for i in range(0, 1000, step):
            part = files_to_dl[i: i + step]
            if part:
                files_to_dl_parts.append(part)
        for part in files_to_dl_parts:
            print(f"DOWNLOADING {part}")
            tasks = [
                asyncio.create_task(
                    download_file(session=session, filename=fn, direct_link=url)
                ) for fn, url in part
            ]
            fns = await asyncio.gather(*tasks)
            downloaded_files.extend(fns)
    for fn in downloaded_files:
        await load_to_clickhouse(fn)

asyncio.run(main())
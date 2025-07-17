import asyncio
from datetime import datetime, date, timedelta

from parser.get_init_data import (
    get_requests_id_download_data,
    get_requests_id_download_data_new,
    get_requests_max_id,
    get_request_frequency_download_data_new, get_requests_id_download_data_excel,
)
from parser.get_query_subject import get_query_prio_subject, get_query_list_prio_subjects, get_query_list_totals
from settings import logger
from aiohttp import ClientSession
import unicodedata


def unnest_subjects_list(subjects_list: list):
    result = dict()
    for subject_data in subjects_list:
        s_id = str(subject_data.get("id"))
        s_name = strip_invisible(subject_data.get("name", "").strip().lower())
        children = subject_data.get("childs", [])
        result[s_name] = s_id
        result.update(unnest_subjects_list(children))
    return result


async def get_today_subjects_dict():
    url = "https://static-basket-01.wbcontent.net/vol0/data/subject-base.json"
    async with ClientSession() as http_session:
        async with http_session.get(url) as resp:
            result = await resp.json()
    subjects_dict = unnest_subjects_list(result)
    return subjects_dict


def strip_invisible(s):
    return ''.join(
        c for c in s if not unicodedata.category(c).startswith('Cf')
    )

async def get_missing_queries_subjects_batch(rows, http_session):
    for row in rows:
        pass


async def get_missing_queries_subjects_rows(rows, http_session):
    queries = [row[0] for row in rows]
    step = len(queries) // 40
    queries_batches = []
    for i in range(0, len(queries) + step, step):
        slc = queries[i: i + step]
        if slc:
            queries_batches.append(slc)
    del queries
    tasks = [
        asyncio.create_task(get_missing_queries_subjects_batch(rows=batch, http_session=http_session))
        for batch in queries_batches
    ]
    tasks_result = await asyncio.gather(*tasks)
    result = []
    for rows in tasks_result:
        result.extend(rows)
    return result

async def prepare_csv_contents(contents: list[tuple[str, int]], filename: str):
    file_date = date.fromisoformat(filename.strip().replace(".csv", ""))
    now_date = datetime(
        year=file_date.year,
        month=file_date.month,
        day=file_date.day,
        hour=1,
        minute=2,
        second=0,
        microsecond=0,
    )
    max_query_id = await get_requests_max_id()
    queries_dict = await get_requests_id_download_data()
    requests_data = []
    error_rows = []
    new_queries = []
    new_query_scaler = 1
    async with ClientSession() as http_session:
        for row in contents:
            query = strip_invisible(str(row[0]).strip().strip('!#').strip().lower())
            if not query:
                continue
            quantity = row[1]
            try:
                query_id, subject_id, total_products = queries_dict.get(query, (0, 0, 0))
                if not query_id:
                    query_id = max_query_id + new_query_scaler
                    new_query_scaler += 1
                    logger.info(f"GETTING SUBJECT FOR {query}")
                    new_queries.append((query_id, query, now_date, quantity))
                else:
                    requests_data.append((query_id, query, quantity, subject_id, total_products, now_date))
            except (ValueError, TypeError, IndexError):
                error_rows.append(row)
        new_queries_meta = await get_query_list_prio_subjects(http_session=http_session, queries=new_queries)
        requests_data.extend(new_queries_meta)
    logger.info("Data prepared")
    if len(requests_data) < 750000:
        raise ValueError
    return requests_data, error_rows


async def prepare_request_frequency(rows, client):
    frequency_rows = []
    growth_rows = []
    queries_ids = tuple(sorted([row[0] for row in rows]))
    queries_parts = []
    step = 1000
    new_date: date = rows[0][5].date()
    start_week = new_date - timedelta(days=6)
    end_week = new_date - timedelta(days=1)
    days_179 = new_date - timedelta(days=179)
    days_119 = new_date - timedelta(days=119)
    days_90 = new_date - timedelta(days=90)
    days_89 = new_date - timedelta(days=89)
    days_60 = new_date - timedelta(days=60)
    days_59 = new_date - timedelta(days=59)
    days_30 = new_date - timedelta(days=30)
    days_29 = new_date - timedelta(days=29)
    for i in range(300):
        queries_parts.append(queries_ids[i * step : (step * i) + step])
    query_1 = f"""SELECT 
                query_id, 
                (sum(if(date between '{str(start_week)}' and '{str(end_week)}', frequency, 0)) as freq_last,
                sum(if(date between '{str(days_179)}' and '{str(days_90)}', frequency, 0)) as freq_old_90,
                sum(if(date between '{str(days_89)}' and '{str(new_date)}', frequency, 0)) as freq_new_90,
                sum(if(date between '{str(days_119)}' and '{str(days_60)}', frequency, 0)) as freq_old_60,
                sum(if(date between '{str(days_59)}' and '{str(new_date)}', frequency, 0)) as freq_new_60,
                sum(if(date between '{str(days_59)}' and '{str(days_30)}', frequency, 0)) as freq_old_30,
                sum(if(date between '{str(days_29)}' and '{str(new_date)}', frequency, 0)) as freq_new_30)
            FROM request_frequency
            WHERE query_id IN %(v1)s 
            AND date BETWEEN '{str(days_179)}' AND '{str(new_date)}'
            GROUP BY query_id"""
    queries_frequency = dict()
    print("getting query ids")
    for queries_part in queries_parts:
        if not queries_part:
            continue
        params = {"v1": queries_part}
        query_ids_query = await client.query(query_1, parameters=params)
        query_ids_temp = {row[0]: row[1] for row in query_ids_query.result_rows}
        queries_frequency.update(query_ids_temp)
    for row in rows:
        query_id = int(row[0])
        week_frequency = int(row[2])
        subject_id = int(row[3])
        try:
            (
                prev_query_sum,
                freq_old_90,
                freq_new_90,
                freq_old_60,
                freq_new_60,
                freq_old_30,
                freq_new_30
            ) = queries_frequency.get(query_id, (0, 0, 0, 0, 0, 0, 0))
            if not prev_query_sum:
                start_date = new_date - timedelta(days=6)
                avg_freq = week_frequency // 7
                for i in range(7):
                    frequency_rows.append(
                        (query_id, avg_freq, start_date + timedelta(days=i))
                    )
                sum_30 = week_frequency
                g30 = 100 if sum_30 > 0 else 0
                g60 = 100 if sum_30 > 0 else 0
                g90 = 100 if sum_30 > 0 else 0
            else:
                new_freq = week_frequency - prev_query_sum
                if new_freq < 0:
                    new_freq = week_frequency // 14
                freq_new_30 += new_freq
                freq_new_60 += new_freq
                freq_new_90 += new_freq
                sum_30 = freq_new_30
                g30 = (int((freq_new_30 - freq_old_30) * 100 / freq_old_30) if freq_old_30 else 100) if sum_30 > 0 else 0
                g60 = (int((freq_new_60 - freq_old_60) * 100 / freq_old_60) if freq_old_60 else 100) if sum_30 > 0 else 0
                g90 = (int((freq_new_90 - freq_old_90) * 100 / freq_old_90) if freq_old_90 else 100) if sum_30 > 0 else 0
                frequency_rows.append((query_id, new_freq, new_date))
            growth_rows.append((query_id, new_date, g30, g60, g90, sum_30, subject_id))
        except (ValueError, TypeError, IndexError):
            logger.error("SHIT REQUESTS OMGGGG")
    return frequency_rows, growth_rows



async def prepare_request_frequency_excel(rows, client):
    frequency_rows = []
    growth_rows = []
    queries_ids = tuple(sorted([row[0] for row in rows]))
    queries_parts = []
    step = 1000
    new_date: date = rows[0][5].date()
    start_week = new_date - timedelta(days=6)
    end_week = new_date - timedelta(days=1)
    days_179 = new_date - timedelta(days=179)
    days_119 = new_date - timedelta(days=119)
    days_90 = new_date - timedelta(days=90)
    days_89 = new_date - timedelta(days=89)
    days_60 = new_date - timedelta(days=60)
    days_59 = new_date - timedelta(days=59)
    days_30 = new_date - timedelta(days=30)
    days_29 = new_date - timedelta(days=29)
    for i in range(300):
        queries_parts.append(queries_ids[i * step : (step * i) + step])
    query_1 = f"""SELECT 
                query_id, 
                (sum(if(date between '{str(start_week)}' and '{str(end_week)}', frequency, 0)) as freq_last,
                sum(if(date between '{str(days_179)}' and '{str(days_90)}', frequency, 0)) as freq_old_90,
                sum(if(date between '{str(days_89)}' and '{str(new_date)}', frequency, 0)) as freq_new_90,
                sum(if(date between '{str(days_119)}' and '{str(days_60)}', frequency, 0)) as freq_old_60,
                sum(if(date between '{str(days_59)}' and '{str(new_date)}', frequency, 0)) as freq_new_60,
                sum(if(date between '{str(days_59)}' and '{str(days_30)}', frequency, 0)) as freq_old_30,
                sum(if(date between '{str(days_29)}' and '{str(new_date)}', frequency, 0)) as freq_new_30)
            FROM request_frequency
            WHERE query_id IN %(v1)s 
            AND date BETWEEN '{str(days_179)}' AND '{str(new_date)}'
            GROUP BY query_id"""
    queries_frequency = dict()
    print("getting query ids")
    for queries_part in queries_parts:
        if not queries_part:
            continue
        params = {"v1": queries_part}
        query_ids_query = await client.query(query_1, parameters=params)
        query_ids_temp = {row[0]: row[1] for row in query_ids_query.result_rows}
        queries_frequency.update(query_ids_temp)
    for row in rows:
        query_id = int(row[0])
        new_freq = int(row[2])
        subject_id = int(row[3])
        try:
            (
                prev_query_sum,
                freq_old_90,
                freq_new_90,
                freq_old_60,
                freq_new_60,
                freq_old_30,
                freq_new_30
            ) = queries_frequency.get(query_id, (0, 0, 0, 0, 0, 0, 0))
            if not prev_query_sum:
                sum_30 = new_freq
                g30 = 100 if sum_30 > 0 else 0
                g60 = 100 if sum_30 > 0 else 0
                g90 = 100 if sum_30 > 0 else 0
            else:
                freq_new_30 += new_freq
                freq_new_60 += new_freq
                freq_new_90 += new_freq
                sum_30 = freq_new_30
                g30 = (int((freq_new_30 - freq_old_30) * 100 / freq_old_30) if freq_old_30 else 100) if sum_30 > 0 else 0
                g60 = (int((freq_new_60 - freq_old_60) * 100 / freq_old_60) if freq_old_60 else 100) if sum_30 > 0 else 0
                g90 = (int((freq_new_90 - freq_old_90) * 100 / freq_old_90) if freq_old_90 else 100) if sum_30 > 0 else 0
            frequency_rows.append((query_id, new_freq, new_date))
            growth_rows.append((query_id, new_date, g30, g60, g90, sum_30, subject_id))
        except (ValueError, TypeError, IndexError):
            logger.error("SHIT REQUESTS OMGGGG")
    return frequency_rows, growth_rows


async def recount_request_frequency(rows, client):
    frequency_rows = []
    queries_ids = tuple(sorted([row[0] for row in rows]))
    queries_parts = []
    step = 1000
    new_date = rows[0][3].date()
    start_month = new_date - timedelta(days=29)
    end_month = new_date
    for i in range(300):
        queries_parts.append(queries_ids[i * step : (step * i) + step])
    query_1 = f"""SELECT query_id, sum(frequency) 
            FROM request_frequency
            WHERE query_id IN %(v1)s 
            AND date BETWEEN '{str(start_month)}' AND '{str(end_month)}'
            GROUP BY query_id"""
    queries_frequency = dict()
    logger.info("getting query ids")
    for queries_part in queries_parts:
        if not queries_part:
            continue
        params = {"v1": queries_part}
        query_ids_query = await client.query(query_1, parameters=params)
        query_ids_temp = {row[0]: row[1] for row in query_ids_query.result_rows}
        queries_frequency.update(query_ids_temp)
    for row in rows:
        query_id = int(row[0])
        month_frequency = int(row[2])
        try:
            prev_query_sum = queries_frequency.get(query_id, 0)
            start_date = new_date - timedelta(days=29)
            new_freq = month_frequency - prev_query_sum
            avg_new_freq = new_freq // 30
            if avg_new_freq > 0:
                for i in range(30):
                    frequency_rows.append(
                        (query_id, avg_new_freq, start_date + timedelta(days=i))
                    )
        except (ValueError, TypeError, IndexError):
            logger.error("SHIT REQUESTS OMGGGG")
    return frequency_rows


async def prepare_update_month_csv_contents(contents: list[tuple[str, int]], filename: str):
    if not "месяц" in filename:
        raise ValueError("фигня из под коня")
    file_date = date.fromisoformat(filename.lower().replace(".csv", "").replace("месяц", "").strip())
    now_date = datetime(
        year=file_date.year,
        month=file_date.month,
        day=file_date.day,
        hour=1,
        minute=0,
        second=0,
        microsecond=0,
    )
    max_query_id = await get_requests_max_id()
    queries_dict = await get_requests_id_download_data()
    requests_data = []
    new_requests = []
    error_rows = []
    new_query_scaler = 1
    async with ClientSession() as http_session:
        for row in contents:
            query = strip_invisible(str(row[0]).strip().lower())
            try:
                query_id, subject_id = queries_dict.get(query, (0, 0))
                if not query_id:
                    query_id = max_query_id + new_query_scaler
                    new_query_scaler += 1
                    new_requests.append((query_id, query, row[1] // 4, now_date))
                if not subject_id:
                    subject_id = await get_query_prio_subject(http_session=http_session, query_string=query)
                requests_data.append((query_id, query, row[1], now_date))
            except (ValueError, TypeError, IndexError):
                error_rows.append(row)
    logger.info("Data prepared")
    if len(requests_data) < 750000:
        raise ValueError
    return requests_data, error_rows, new_requests


async def get_request_frequency_by_date(date_, client):
    new_date: date = date_
    days_179 = new_date - timedelta(days=179)
    days_119 = new_date - timedelta(days=119)
    days_90 = new_date - timedelta(days=90)
    days_89 = new_date - timedelta(days=89)
    days_60 = new_date - timedelta(days=60)
    days_59 = new_date - timedelta(days=59)
    days_30 = new_date - timedelta(days=30)
    days_29 = new_date - timedelta(days=29)
    stmt = f"""SELECT 
                rf.query_id,
                max(r.subject_id),
                (sum(if(rf.date between '{str(days_179)}' and '{str(days_90)}', rf.frequency, 0)) as freq_old_90,
                sum(if(rf.date between '{str(days_89)}' and '{str(new_date)}', rf.frequency, 0)) as freq_new_90,
                sum(if(rf.date between '{str(days_119)}' and '{str(days_60)}', rf.frequency, 0)) as freq_old_60,
                sum(if(rf.date between '{str(days_59)}' and '{str(new_date)}', rf.frequency, 0)) as freq_new_60,
                sum(if(rf.date between '{str(days_59)}' and '{str(days_30)}', rf.frequency, 0)) as freq_old_30,
                sum(if(rf.date between '{str(days_29)}' and '{str(new_date)}', rf.frequency, 0)) as freq_new_30)
            FROM (select * from request_frequency where query_id in (select query_id from request_frequency where date = '{str(new_date)}') and date BETWEEN '{str(days_179)}' AND '{str(new_date)}') as rf
            JOIN request as r on r.id = rf.query_id 
            GROUP BY rf.query_id"""
    q = await client.query(stmt)
    growth_rows = []
    for row in q.result_rows:
        query_id = int(row[0])
        subject_id = int(row[1])
        try:
            (
                freq_old_90,
                freq_new_90,
                freq_old_60,
                freq_new_60,
                freq_old_30,
                freq_new_30
            ) = row[2]
            sum_30 = freq_new_30
            g30 = (int((freq_new_30 - freq_old_30) * 100 / freq_old_30) if freq_old_30 else 100) if sum_30 > 0 else 0
            g60 = (int((freq_new_60 - freq_old_60) * 100 / freq_old_60) if freq_old_60 else 100) if sum_30 > 0 else 0
            g90 = (int((freq_new_90 - freq_old_90) * 100 / freq_old_90) if freq_old_90 else 100) if sum_30 > 0 else 0
            growth_rows.append((query_id, new_date, g30, g60, g90, sum_30, subject_id))
        except (ValueError, TypeError, IndexError):
            logger.error("SHIT REQUESTS OMGGGG")
    return growth_rows




async def prepare_excel_contents(contents: list[tuple[str, int, str]], filename: str):
    file_date = date.fromisoformat(filename.strip().replace(".xlsx", ""))
    now_date = datetime(
        year=file_date.year,
        month=file_date.month,
        day=file_date.day,
        hour=1,
        minute=2,
        second=0,
        microsecond=0,
    )
    max_query_id = await get_requests_max_id()
    queries_dict = await get_requests_id_download_data_excel()
    subjects_dict = await get_today_subjects_dict()
    requests_data = []
    error_rows = []
    new_queries = []
    new_queries_need_subject = []
    new_query_scaler = 1

    async with ClientSession() as http_session:
        for i, row in enumerate(contents):
            try:
                query_raw, quantity, subject_name = row
                if i == 0:
                    print(query_raw)
                subject_name = strip_invisible(subject_name.strip().lower())
                subject_id = subjects_dict.get(subject_name, 0)
                query = strip_invisible(str(query_raw).strip().strip("!#").lower())
                if not query:
                    continue
                query_id, total_products = queries_dict.get(query, (0, 0))
                if not query_id:
                    query_id = max_query_id + new_query_scaler
                    new_query_scaler += 1
                    logger.info(f"GETTING SUBJECT FOR {query}")
                    new_queries.append((query_id, query, now_date, quantity, subject_id))
                elif not subject_id or subject_id < 0:
                    new_queries_need_subject.append((query_id, query, now_date, quantity))
                else:
                    requests_data.append((query_id, query, quantity, subject_id, total_products, now_date))
            except (ValueError, TypeError, IndexError) as e:
                error_rows.append(row)

        new_queries_meta = await get_query_list_totals(http_session=http_session, queries=new_queries)
        new_queries_subject_meta = await get_query_list_prio_subjects(http_session=http_session, queries=new_queries)
        requests_data.extend(new_queries_meta)
        requests_data.extend(new_queries_subject_meta)

    logger.info("Data prepared")
    if len(requests_data) < 95000:
        raise IndexError
    return requests_data, error_rows
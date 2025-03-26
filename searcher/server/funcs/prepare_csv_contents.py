from datetime import datetime, date, timedelta

from parser.get_init_data import (
    get_requests_id_download_data,
    get_requests_id_download_data_new,
    get_requests_max_id,
    get_request_frequency_download_data_new
)
from settings import logger


async def prepare_csv_contents(contents: list[tuple[str, int]], filename:str):
    file_date = date.fromisoformat(filename.strip().replace(".csv", ""))
    now_date = datetime(year=file_date.year, month=file_date.month, day=file_date.day, hour=1, minute=0, second=0, microsecond=0)
    max_query_id = await get_requests_max_id()
    queries_dict = await get_requests_id_download_data()
    requests_data = []
    error_rows = []
    new_query_scaler = 1
    for row in contents:
        query = str(row[0]).strip().lower()
        try:
            query_id = queries_dict.get(query)
            if not query_id:
                query_id = max_query_id + new_query_scaler
                new_query_scaler += 1
            requests_data.append((query_id, query, row[1], now_date))
        except (ValueError, TypeError, IndexError):
            error_rows.append(row)
    logger.info("Data prepared")
    return requests_data, error_rows


async def prepare_request_frequency(rows, client):
    frequency_rows = []
    queries_ids = tuple(sorted([row[0] for row in rows]))
    queries_parts = []
    step = 1000
    new_date = rows[0][3].date()
    start_week = new_date - timedelta(days=6)
    end_week = new_date - timedelta(days=1)
    for i in range(300):
        queries_parts.append(queries_ids[i * step: (step * i) + step])
    query_1 = f"""SELECT query_id, sum(frequency) 
            FROM request_frequency
            WHERE query_id IN %(v1)s 
            AND date BETWEEN '{str(start_week)}' AND '{str(end_week)}'
            GROUP BY query_id"""
    queries_frequency = dict()
    print("getting query ids")
    for queries_part in queries_parts:
        if not queries_part:
            continue
        params = {
            "v1": queries_part
        }
        query_ids_query = await client.query(query_1, parameters=params)
        query_ids_temp = {row[0]: row[1] for row in query_ids_query.result_rows}
        queries_frequency.update(query_ids_temp)
    for row in rows:
        query_id = int(row[0])
        week_frequency = int(row[2])
        try:
            prev_query_sum = queries_frequency.get(query_id)
            if not prev_query_sum:
                start_date = new_date - timedelta(days=6)
                avg_freq = week_frequency // 7
                for i in range(7):
                    frequency_rows.append((query_id, avg_freq, start_date + timedelta(days=i)))
            else:
                new_freq = week_frequency - prev_query_sum
                if new_freq < 0:
                    new_freq = 0
                frequency_rows.append((query_id, new_freq, new_date))
        except (ValueError, TypeError, IndexError):
            logger.error("SHIT REQUESTS OMGGGG")
    return frequency_rows
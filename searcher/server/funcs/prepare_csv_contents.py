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
    requests_data = []
    error_rows = []
    new_query_scaler = 1
    for row in contents:
        val = str(row[0])
        try:
            prev_row_id = await get_requests_id_download_data_new(val)
            if not prev_row_id:
                requests_data.append((max_query_id + new_query_scaler, val, row[1], now_date))
                new_query_scaler += 1
            else:
                requests_data.append((prev_row_id, val, row[1], now_date))
        except (ValueError, TypeError, IndexError):
            error_rows.append(row)
    return requests_data, error_rows


async def prepare_request_frequency(rows, client):
    frequency_rows = []
    for row in rows:
        query_id = int(row[0])
        week_frequency = int(row[2])
        new_date = row[3]
        try:
            prev_query_sum = await get_request_frequency_download_data_new(query_id, new_date, client)
            if not prev_query_sum:
                start_date = new_date - timedelta(days=6)
                avg_freq = week_frequency // 7
                for i in range(7):
                    frequency_rows.append((query_id, avg_freq, start_date + timedelta(days=i)))
            else:
                new_freq = week_frequency - prev_query_sum
                if new_freq < 0:
                    new_freq = 0
                frequency_rows.append((query_id, new_freq, row[1], new_date))
        except (ValueError, TypeError, IndexError):
            logger.error("SHIT REQUESTS OMGGGG")
    return frequency_rows
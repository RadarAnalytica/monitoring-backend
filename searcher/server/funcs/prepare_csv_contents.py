from datetime import datetime

from parser.get_init_data import get_requests_id_download_data
from settings import TIMEZONE, logger


async def prepare_csv_contents(contents: list[tuple[str, int]]):
    now_date = datetime.now(TIMEZONE)
    requests_data = []
    error_rows = []
    prev_data = await get_requests_id_download_data()
    new_query_scaler = 1
    for row in contents:
        val = str(row[0])
        try:
            if val.isdigit():
                continue
            prev_row_id = prev_data.get(val)
            if not prev_row_id:
                requests_data.append((len(prev_data) + new_query_scaler, val, row[1], now_date))
                new_query_scaler += 1
            else:
                requests_data.append((prev_row_id, val, row[1], now_date))
        except (ValueError, TypeError, IndexError):
            error_rows.append(row)
    return requests_data, error_rows
from datetime import datetime

from parser.get_init_data import get_requests_id_download_data
from settings import TIMEZONE, logger


async def prepare_csv_contents(contents: list[str]):
    now_date = datetime.now(TIMEZONE)
    contents[0] = contents[0].replace('\ufeff', '')
    requests_data = []
    error_rows = []
    prev_data = await get_requests_id_download_data()
    for i, row in enumerate(contents, 1):
        try:
            if '"' in row:
                row = row.replace('"', "", 1)
                row_values = row.strip().rsplit('",', 1)
                row_values[1] = int(row_values[1])
            elif row.strip().isdigit():
                continue
            else:
                row_values = row.strip().split(",", 1)
                row_values[1] = int(row_values[1])
            requests_data.append((prev_data.get(row_values[0], i), row_values[0], row_values[1], now_date))
        except (ValueError, TypeError, IndexError):
            error_rows.append(row)
    return requests_data, error_rows
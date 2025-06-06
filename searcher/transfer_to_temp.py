import asyncio
from datetime import date, timedelta

from server.funcs.upload_requests_data import recount_growth_by_date
from settings import logger

async def main():
    start_date = date(year=2025, month=2, day=12)
    dates_list = [start_date - timedelta(days=i) for i in range(90)]
    for d in dates_list:
        logger.info(f"DATE {d}")
        await recount_growth_by_date(d)


if __name__ == '__main__':
    asyncio.run(main())
import asyncio
from datetime import date, timedelta

from clickhouse_db.get_async_connection import get_async_connection
from service.log_alert import send_log_message
from service.common.db_worker import save_to_db_worker
from service.suppliers_history.http_worker import http_worker_supplier


async def get_today_suppliers_data():
    print("Вход")
    await send_log_message("Начался сбор по поставщикам")
    meta_queue = asyncio.Queue(10)
    history_queue = asyncio.Queue(10)
    main_queue = asyncio.Queue(30)
    try:
        async with get_async_connection() as client:
            yesterday = date.today() - timedelta(days=1)

            query = await client.query(
                "SELECT DISTINCT id FROM supplier_product WHERE id > 0 ORDER BY id"
            )
            supplier_ids = [row[0] for row in query.result_rows]
            if not supplier_ids:
                print("No ids")
                return
            print("id получены")
            supplier_meta_save_task = asyncio.create_task(
                save_to_db_worker(
                    queue=meta_queue,
                    table="supplier_meta",
                    fields=[
                        "id",
                        "name",
                        "full_name",
                        "trademark",
                        "inn",
                        "taxpayer_code",
                        "registration_date",
                        "ogrn",
                        "ogrnip",
                        "kpp",
                        "unn",
                        "unp",
                        "bin_",
                    ],
                    client=client,
                )
            )
            supplier_history_task = asyncio.create_task(
                save_to_db_worker(
                    queue=history_queue,
                    table="supplier_history",
                    fields=[
                        "id",
                        "date",
                        "rating",
                        "valuation",
                        "feedbacks",
                        "ratio",
                        "sold_items",
                    ],
                    client=client,
                )
            )
            http_tasks = [
                asyncio.create_task(
                    http_worker_supplier(
                        main_queue=main_queue,
                        meta_queue=meta_queue,
                        history_queue=history_queue,
                        date_=yesterday,
                    )
                )
                for _ in range(15)
            ]
            i = None
            for i in supplier_ids:
                await main_queue.put(i)
            await meta_queue.put(None)
            await history_queue.put(None)
            await main_queue.put(None)
            await asyncio.gather(
                *http_tasks, supplier_history_task, supplier_meta_save_task
            )
            await send_log_message("Закончился сбор по поставщикам")
            await client.command("OPTIMIZE TABLE supplier_meta FINAL")
    except Exception as e:
        await send_log_message(
            "Ошибка при сборе по поставщикам. Последний id: {i}", ex=e
        )

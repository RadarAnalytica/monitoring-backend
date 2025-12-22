"""
Celery задача для автоматического ежедневного скачивания отчёта WB.
Запускается в 07:00 UTC через Celery Beat.
"""

import asyncio
import io
from datetime import date, timedelta

import pandas as pd

from celery_main import celery_app
from service.wb_report_downloader import download_wb_report
from service.log_alert import send_log_message
from server.funcs.prepare_csv_contents import prepare_excel_contents
from server.funcs.upload_requests_data import upload_requests_excel_bg
from settings import logger


@celery_app.task(
    name="download_wb_report_task",
    max_retries=0,
    autoretry_for=(),
)
def download_wb_report_task():
    """
    Celery задача для скачивания и обработки WB отчёта.
    
    ВАЖНО: НЕТ RETRY! Одна попытка. При ошибке — уведомление в Telegram.
    
    Флоу:
    1. Скачивает отчёт с WB
    2. Парсит Excel через pandas
    3. Обрабатывает через prepare_excel_contents
    4. Загружает через upload_requests_excel_bg
    5. Отправляет уведомление о результате
    """
    asyncio.run(_download_and_process_report())


async def _download_and_process_report():
    """Асинхронная обработка отчёта."""
    yesterday = date.today() - timedelta(days=1)
    filename = f"{yesterday}.xlsx"
    
    try:
        # 1. Скачиваем отчёт
        await send_log_message(f"📥 Начинаем скачивание WB отчёта за {yesterday}")
        
        xlsx_bytes, error = await download_wb_report(wait_seconds=60)
        
        if error:
            await send_log_message(f"❌ Ошибка скачивания WB отчёта: {error}")
            logger.error(f"[WB Report Task] Ошибка скачивания: {error}")
            return  # Не retry, просто выходим
        
        if not xlsx_bytes:
            await send_log_message("❌ WB отчёт не скачан (пустой ответ)")
            return
        
        logger.info(f"[WB Report Task] Скачано {len(xlsx_bytes)} байт")
        
        # 2. Парсим Excel (sheet_name=2, skiprows=1 как в upload_excel)
        df = pd.read_excel(io.BytesIO(xlsx_bytes), sheet_name=2, skiprows=1, engine="openpyxl")
        df = df.rename(columns={
            df.columns[0]: 'query',
            df.columns[1]: 'query_count',
            df.columns[5]: 'top_ordered'
        })
        df = df[['query', 'query_count', 'top_ordered']].dropna()
        contents = list(df.itertuples(index=False, name=None))
        
        logger.info(f"[WB Report Task] Распарсено {len(contents)} строк")
        
        # 3. Обрабатываем через prepare_excel_contents
        try:
            requests_data, error_rows = await prepare_excel_contents(contents, filename=filename)
        except ValueError:
            await send_log_message(f"❌ Ошибка формата файла: {filename}")
            return
        except IndexError:
            await send_log_message(f"❌ Неполный файл: {filename} (меньше 95000 строк)")
            return
        
        logger.info(f"[WB Report Task] Подготовлено {len(requests_data)} записей, ошибок: {len(error_rows)}")
        
        # 4. Загружаем в БД
        await upload_requests_excel_bg(requests_data)
        
        # 5. Уведомление об успехе
        await send_log_message(
            f"✅ WB отчёт за {yesterday} обработан!\n"
            f"Записей: {len(requests_data)}\n"
            f"error_rows: {len(error_rows)}"
        )
        
    except Exception as e:
        error_msg = str(e)[:100]
        await send_log_message(f"❌ Ошибка обработки WB отчёта: {error_msg}")
        logger.exception(f"[WB Report Task] Исключение: {e}")
        # НЕ делаем raise — не хотим retry!

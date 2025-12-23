"""
Сервис для скачивания отчёта Wildberries "Поисковые запросы на WB. Джем".
"""

import asyncio
import io
import uuid
import zipfile
from datetime import date
from typing import Any

import aiohttp

from clickhouse_db.get_async_connection import get_async_connection
from settings import logger


BASE_URL = "https://seller-content.wildberries.ru"

# TODO: Автоматизировать получение cookies
WB_COOKIES = (
    "external-locale=ru; "
    "_wbauid=4296535381753951216; "
    "wbx-validation-key=ea0d7677-ea8e-460a-be48-5e2ab7049c88; "
    "_ga=GA1.1.268497829.1758526088; "
    "_ga_TXRZMJQDFE=GS2.1.s1759131204$o6$g1$t1759131726$j60$l0$h0; "
    "x-supplier-id-external=b2545aa7-761e-4d6c-9362-d2d76f07e0f3; "
    "__zzatw-wb=MDA0dC0cTHtmcDhhDHEWTT17CT4VHThHKHIzd2UuPG0mZ01iJzVRP0FaW1Q4NmdBEXUmCQg3LGBwVxlRExpceEdXeiwfF3tyKlgLDl9FSmllbQwtUlFRS19/Dg4/aU5ZQ11wS3E6EmBWGB5CWgtMeFtLKRZHGzJhXkZpdRVVDgsURUNwLDA8ayFjThYjS10JCCtPQn1tJ089DxhDdV9vG3siXyoIJGM1Xz9EaVhTMCpYQXt1J3Z+KmUzPGwjZUtgJkxZUH0uGQ1pN2wXPHVlLwkxLGJ5MVIvE0tsP0caRFpbQDsyVghDQE1HFF9BWncyUlFRS2EQR0lrZU5TQixmG3EVTQgNND1aciIPWzklWAgSPwsmIBd9bypWEA9eQUptbxt/Nl0cOWMRCxl+OmNdRkc3FSR7dSYKCTU3YnAvTCB7SykWRxsyYV5GaXUVCQkQX0JyJnomQmseHERdU0kQSgooHxN0JyULDhEZPUgqc18+VxlRDxZhDhYYRRcje0I3Yhk4QhgvPV8/YngiD2lIYCVJWVQILh0UfWwjS3FPLH12X30beylOIA0lVBMhP05yvJVrGg==; "
    "cfidsw-wb=LgyKJBeL1/mtzxTmCbnaeyqdU3HLfLF1IZVFu0KV5v/HC+gs8yiy/3v9OU/qUTfeJbW6nIbK4z/RjHIYg75gtSwb/RQnHisSJAlFEnUJyMfBVBfmdCZhuIwKuYZ4gtAEjc3Qn+eR5lW0hzF+0IA78o1nYC+hgugPdOKXqW4K"
)


async def get_seller_token() -> str | None:
    """
    Получает токен авторизации из ClickHouse radar.seller_tokens.
    
    Returns:
        str: токен authorizev3 или None если не найден
    """
    async with get_async_connection() as client:
        stmt = "SELECT token FROM radar.seller_tokens ORDER BY updated DESC LIMIT 1"
        result = await client.query(stmt)
        if result.result_rows:
            token = result.result_rows[0][0]
            logger.info(f"[WB Report] Токен получен: {token[:50]}...")
            return token
        logger.error("[WB Report] Токен не найден в radar.seller_tokens")
        return None


def get_common_headers(auth_token: str) -> dict[str, str]:
    """Возвращает общие заголовки для запросов к WB API."""
    return {
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "ru,en-US;q=0.9,en;q=0.8",
        "authorizev3": auth_token,
        "content-type": "application/json",
        "cookie": WB_COOKIES,
        "origin": "https://seller.wildberries.ru",
        "referer": "https://seller.wildberries.ru/",
        "root-version": "v1.74.0",
        "sec-ch-ua": '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Linux"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
    }


async def create_report_download(
    session: aiohttp.ClientSession,
    auth_token: str,
    report_type: str = "SEARCH_ANALYSIS_PREMIUM_REPORT",
    interval: str = "yesterday",
    limit: int = 300000,
) -> dict[str, Any]:
    """
    Создаёт запрос на скачивание отчёта.
    
    Returns:
        dict с request_id и статусом
    """
    url = f"{BASE_URL}/ns/analytics-api/content-analytics/api/v1/file-manager/download"
    request_id = str(uuid.uuid4())
    
    payload = {
        "id": request_id,
        "userReportName": "",
        "reportType": report_type,
        "params": {
            "items": [],
            "searchText": "",
            "cartToOrder": [],
            "openToCart": [],
            "interval": interval,
            "orderBy": {"field": "frequency", "mode": "desc"},
            "limit": limit,
        }
    }
    
    logger.info(f"[WB Report] Создаём отчёт с ID: {request_id}")
    
    async with session.post(url, json=payload, headers=get_common_headers(auth_token)) as response:
        status = response.status
        text = await response.text()
        
        logger.info(f"[WB Report] create_report: status={status}")
        
        return {
            "request_id": request_id,
            "status": status,
            "response": text,
        }


async def get_downloads_list(
    session: aiohttp.ClientSession,
    auth_token: str,
    report_types: str = "SEARCH_ANALYSIS_PREMIUM_REPORT",
) -> dict[str, Any]:
    """Получает список созданных отчётов."""
    url = f"{BASE_URL}/ns/analytics-api/content-analytics/api/v1/file-manager/downloads"
    params = {"report_types": report_types}
    
    async with session.get(url, params=params, headers=get_common_headers(auth_token)) as response:
        status = response.status
        data = await response.json()
        
        downloads_count = len(data.get("data", {}).get("downloads", []))
        logger.info(f"[WB Report] Найдено отчётов: {downloads_count}")
        
        return {"status": status, "data": data}


def find_download_by_id(downloads_response: dict, target_id: str) -> dict | None:
    """Находит файл в списке загрузок по ID."""
    downloads = downloads_response.get("data", {}).get("data", {}).get("downloads", [])
    
    for download in downloads:
        if download.get("id") == target_id:
            return download
    return None


async def generate_download_token(session: aiohttp.ClientSession, auth_token: str) -> str | None:
    """Генерирует токен для скачивания файла через JSON-RPC."""
    url = f"{BASE_URL}/ns/suppliers-auth-tokens/suppliers-portal-core/api/v1/tokensjrpc"
    
    payload = {
        "method": "generateToken",
        "params": {"team": "content-analytics"},
        "jsonrpc": "2.0",
        "id": "json-rpc_1"
    }
    
    async with session.post(url, json=payload, headers=get_common_headers(auth_token)) as response:
        if response.status == 200:
            data = await response.json()
            token = data.get("result", {}).get("token")
            if token:
                logger.info(f"[WB Report] Download token получен")
                return token
        logger.error(f"[WB Report] Ошибка получения download token: {response.status}")
        return None


async def download_and_extract_xlsx(
    session: aiohttp.ClientSession,
    download_url: str,
    download_token: str,
) -> tuple[bytes | None, str | None]:
    """
    Скачивает ZIP-архив и извлекает XLSX.
    
    Returns:
        tuple: (xlsx_bytes, error_message)
    """
    headers = {
        "accept": "*/*",
        "origin": "https://seller.wildberries.ru",
        "referer": "https://seller.wildberries.ru/",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
        "x-download-token": download_token,
    }
    
    async with session.get(download_url, headers=headers) as response:
        if response.status != 200:
            text = await response.text()
            return None, f"{response.status}: {text[:100]}"
        
        zip_content = await response.read()
        logger.info(f"[WB Report] Скачано {len(zip_content)} байт (ZIP)")
        
        try:
            with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
                xlsx_files = [f for f in zf.namelist() if f.endswith('.xlsx')]
                if not xlsx_files:
                    return None, "XLSX не найден в архиве"
                
                xlsx_content = zf.read(xlsx_files[0])
                logger.info(f"[WB Report] Извлечён {xlsx_files[0]} ({len(xlsx_content)} байт)")
                return xlsx_content, None
                
        except zipfile.BadZipFile as e:
            return None, f"Ошибка ZIP: {e}"


async def download_wb_report(wait_seconds: int = 60) -> tuple[bytes | None, str | None]:
    """
    Главная функция: создаёт отчёт, ждёт и скачивает.
    
    Args:
        wait_seconds: время ожидания формирования отчёта
        
    Returns:
        tuple: (xlsx_bytes, error_message)
    """
    # 1. Получаем токен из БД
    auth_token = await get_seller_token()
    if not auth_token:
        return None, "Токен не найден в БД"
    
    async with aiohttp.ClientSession() as session:
        # 2. Создаём отчёт
        result = await create_report_download(session, auth_token)
        if result["status"] != 200:
            return None, f"Ошибка создания отчёта: {result['status']}: {result['response'][:100]}"
        
        request_id = result["request_id"]
        
        # 3. Ждём формирования
        logger.info(f"[WB Report] Ожидаем {wait_seconds} секунд...")
        await asyncio.sleep(wait_seconds)
        
        # 4. Получаем список и ищем наш файл
        list_result = await get_downloads_list(session, auth_token)
        if list_result["status"] != 200:
            return None, f"Ошибка получения списка: {list_result['status']}"
        
        download_info = find_download_by_id(list_result, request_id)
        if not download_info:
            return None, f"Отчёт {request_id} не найден в списке"
        
        if download_info["status"] != "SUCCESS":
            return None, f"Отчёт не готов, статус: {download_info['status']}"
        
        # 5. Получаем токен для скачивания
        download_token = await generate_download_token(session, auth_token)
        if not download_token:
            return None, "Не удалось получить download token"
        
        # 6. Скачиваем и извлекаем XLSX
        xlsx_bytes, error = await download_and_extract_xlsx(
            session,
            download_info["downloadUrl"],
            download_token
        )
        
        return xlsx_bytes, error

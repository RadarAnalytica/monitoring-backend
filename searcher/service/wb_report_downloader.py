"""
Сервис для скачивания отчёта Wildberries «Поисковые запросы» (частотность).
Флоу: запрос формирования → ожидание → скачивание.
При 403 на запросе формирования: PREMIUM → SEARCH_ANALYSIS_REPORT → ошибка.
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

REPORT_TYPE_PREMIUM = "SEARCH_ANALYSIS_PREMIUM_REPORT"
REPORT_TYPE_STANDARD = "SEARCH_ANALYSIS_REPORT"
REPORT_TYPES_FALLBACK = (REPORT_TYPE_PREMIUM, REPORT_TYPE_STANDARD)

# TODO: Автоматизировать получение cookies
WB_COOKIES = (
    "external-locale=ru; "
    "_wbauid=8549818111784528314; "
    "wbx-validation-key=70055ee3-9761-4bb5-a435-3ca2de382e7e; "
    "x-supplier-id-external=b2545aa7-761e-4d6c-9362-d2d76f07e0f3; "
    "__zzatw-wb=MDA0dC0yYBwREFsKEH49WgsbSl1pCENQGC9LXz1uLWEPJ3wjYnwgGWsvC1RDMmUIPkBNOTM5NGZwVydgTmEjTFVNeSwgE3drH0FLVCNyM3dlaXceViUTFmcPRyJ1F0hAGxI6aCU6f1JpGWUzDldjGAsmVDVfP34mIRp/cSxXcS9NfXY3PmJ+MQ9pOSRjCh9+OFoLDWk3XBQ8dWU+SHV3MjtlHWZOWihMUT9FbllGaXUVF0M8HHsNKkNtLToZUXYQQlh4cBpEN0AYfxVZUnUpbn06MBtFV1BoUBNSeVdWNlofR3snV1U4EBVBcyd2KkJqH2hQYSN3R0lrZU5TQixmG3EVTQgNND1aciIPWzklWAgSPwsmIBl7dCNPfxJiPkp2bxt/Nl0cOWMRCxl+OmNdRkc3FSR7dSYKCTU3YnAvTCB7SykWRxsyYV5GaXUVCQkQX0JyJnomQmseHERdU0kQSgooHxN0JyULDhEZPUgqc18+VxlRDxZhDhYYRRcje0I3Yhk4QhgvPV8/YngiD2lIYCdHXk15JSAYem4lS3FPLH12X30beylOIA0lVBMhP05yiDD4Tw==; "
    "cfidsw-wb=2k1UIvPXQuNO81OE1cf17GGkHxBDRWf6fjB0aw1M/6e/G/zHjQmSl3q6+/VgBdiIBXS1hq9hb/LtHECrfFEGGY3lf2/kgeZU3VeNUYZwktOtT8rgtjC4KKZeK2kbssbrEgUmVETmqxGK/aEExFUpqgcdLTEHB46iFq3XX08="
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
        "accept-language": "ru,en-US;q=0.9,en;q=0.8,ka;q=0.7",
        "authorizev3": auth_token,
        "content-type": "application/json",
        "cookie": WB_COOKIES,
        "origin": "https://seller.wildberries.ru",
        "referer": "https://seller.wildberries.ru/",
        "root-version": "v1.103.1",
        "sec-ch-ua": '"Not;A=Brand";v="8", "Chromium";v="150", "Google Chrome";v="150"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Linux"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36",
    }


async def create_report_download(
    session: aiohttp.ClientSession,
    auth_token: str,
    report_type: str = REPORT_TYPE_PREMIUM,
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
    report_types: str = REPORT_TYPE_PREMIUM,
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
    Главная функция: запрос формирования отчёта → ожидание → скачивание.
    На запросе формирования: PREMIUM, при 403 — SEARCH_ANALYSIS_REPORT, снова 403 — ошибка.

    Args:
        wait_seconds: время ожидания формирования отчёта

    Returns:
        tuple: (xlsx_bytes, error_message)
    """
    auth_token = await get_seller_token()
    if not auth_token:
        return None, "Токен не найден в БД"

    async with aiohttp.ClientSession() as session:
        report_type: str | None = None
        request_id: str | None = None

        for idx, candidate_type in enumerate(REPORT_TYPES_FALLBACK):
            logger.info(f"[WB Report] Запрос формирования отчёта: {candidate_type}")
            result = await create_report_download(
                session, auth_token, report_type=candidate_type
            )
            if result["status"] == 200:
                report_type = candidate_type
                request_id = result["request_id"]
                break

            create_error = (
                f"Ошибка создания отчёта: {result['status']}: {result['response'][:100]}"
            )
            if result["status"] == 403 and idx < len(REPORT_TYPES_FALLBACK) - 1:
                next_type = REPORT_TYPES_FALLBACK[idx + 1]
                logger.warning(
                    f"[WB Report] 403 при формировании ({candidate_type}), "
                    f"пробуем {next_type}"
                )
                continue

            return None, create_error

        if not report_type or not request_id:
            return None, "Не удалось запросить формирование отчёта"

        logger.info(f"[WB Report] Ожидаем {wait_seconds} секунд (report_type={report_type})...")
        await asyncio.sleep(wait_seconds)

        list_result = await get_downloads_list(session, auth_token, report_types=report_type)
        if list_result["status"] != 200:
            return None, f"Ошибка получения списка: {list_result['status']}"

        download_info = find_download_by_id(list_result, request_id)
        if not download_info:
            return None, f"Отчёт {request_id} не найден в списке"

        if download_info["status"] != "SUCCESS":
            return None, f"Отчёт не готов, статус: {download_info['status']}"

        download_token = await generate_download_token(session, auth_token)
        if not download_token:
            return None, "Не удалось получить download token"

        return await download_and_extract_xlsx(
            session,
            download_info["downloadUrl"],
            download_token,
        )

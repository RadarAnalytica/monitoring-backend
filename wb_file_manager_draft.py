"""
Черновик скрипта для работы с Wildberries File Manager API.
Операции: создание отчёта, получение списка, скачивание файла.
"""
from datetime import date

import asyncio
import io
import uuid
import zipfile
from typing import Any

import aiohttp


# Конфигурация
BASE_URL = "https://seller-content.wildberries.ru"
AUTH_TOKEN = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3NjU3MTEzNDQsInVzZXIiOiI1MTg5ODY5OCIsInNoYXJkX2tleSI6IjE1IiwiY2xpZW50X2lkIjoic2VsbGVyLXBvcnRhbCIsInNlc3Npb25faWQiOiI2NzBhODIzYTcxNjQ0N2U0ODhkNzZmYTYyNDBmNGU2ZCIsInZhbGlkYXRpb25fa2V5IjoiNThjZjQ0MjUwODEzZDNmZmQ4ZDZiNGMyODZmZjRkMjE4OWY5ZDE2NzYwZjVmYmJiZTdmN2Q2N2I2NTAzZjY5ZCIsInVzZXJfcmVnaXN0cmF0aW9uX2R0IjoxNjc3NjAyNDE5LCJ2ZXJzaW9uIjoyfQ.YEM_ZeDlPqHhytE6Ynt5hCeRmrsWxBgE-hMK7sZcvrqQW5Ax9BOh0o72jgLpgFAP_LycHjS1GWCaJ5a5nQoQsaplQc4uPEEpyxAyq-T5QLlHxUL6vy_6C955SkFwqgbYJxqGbvJcF-TdYJb9ptjLfhWUjTvrsN43lLFhoFm_3N50q6jyPbBgKdHrYiuTV2R0crfQPj-Dtp8Rq_SZhAqwx68pdLH6PDi6lzaQhFrWm97D-E-8Uxz5ZA8fJDC-tbmShTyA7BTR7NddLlMKGym8hj0fK8AxGcqL73iJ6T6dxWi099Gek4mng1Puoc5zi-s1czVYl6XR1tfoPMuxrg5owA"

# Cookies (если нужны)
COOKIES = {
    "external-locale": "ru",
    "_wbauid": "4296535381753951216",
    "wbx-validation-key": "ea0d7677-ea8e-460a-be48-5e2ab7049c88",
    "x-supplier-id-external": "b2545aa7-761e-4d6c-9362-d2d76f07e0f3",
}


def get_common_headers() -> dict[str, str]:
    """Возвращает общие заголовки для всех запросов."""
    return {
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "ru,en-US;q=0.9,en;q=0.8,ka;q=0.7",
        "authorizev3": AUTH_TOKEN,
        "content-type": "application/json",
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
    report_type: str = "SEARCH_ANALYSIS_PREMIUM_REPORT",
    interval: str = "yesterday",
    limit: int = 300000,
    search_text: str = "",
    items: list | None = None,
) -> dict[str, Any]:
    """
    Создаёт запрос на скачивание отчёта.
    Генерирует UUID для запроса, который потом используется для получения файла.
    
    Args:
        session: aiohttp сессия
        report_type: тип отчёта
        interval: интервал данных (yesterday, week, month, etc.)
        limit: лимит строк
        search_text: поисковый запрос
        items: список элементов для фильтрации
        
    Returns:
        dict с ответом от API (включая request_id для последующего получения файла)
    """
    url = f"{BASE_URL}/ns/analytics-api/content-analytics/api/v1/file-manager/download"
    
    # Генерируем уникальный ID для запроса
    request_id = str(uuid.uuid4())
    
    payload = {
        "id": request_id,
        "userReportName": "",
        "reportType": report_type,
        "params": {
            "items": items or [],
            "searchText": search_text,
            "cartToOrder": [],
            "openToCart": [],
            "interval": interval,
            "orderBy": {
                "field": "frequency",
                "mode": "desc"
            },
            "limit": limit,
        }
    }
    
    print(f"[create_report_download] Отправляем запрос с ID: {request_id}")
    print(f"[create_report_download] Payload: {payload}")
    
    async with session.post(url, json=payload, headers=get_common_headers()) as response:
        status = response.status
        text = await response.text()
        
        print(f"[create_report_download] Status: {status}")
        print(f"[create_report_download] Response: {text[:500]}")
        
        return {
            "request_id": request_id,
            "status": status,
            "response": text,
        }


async def get_downloads_list(
    session: aiohttp.ClientSession,
    report_types: str = "SEARCH_ANALYSIS_PREMIUM_REPORT",
) -> dict[str, Any]:
    """
    Получает список созданных отчётов/файлов для скачивания.
    
    Args:
        session: aiohttp сессия
        report_types: тип отчёта для фильтрации
        
    Returns:
        dict со списком файлов и их статусами
    """
    url = f"{BASE_URL}/ns/analytics-api/content-analytics/api/v1/file-manager/downloads"
    params = {"report_types": report_types}
    
    print(f"[get_downloads_list] Запрашиваем список отчётов...")
    
    async with session.get(url, params=params, headers=get_common_headers()) as response:
        status = response.status
        data = await response.json()
        
        print(f"[get_downloads_list] Status: {status}")
        print(f"[get_downloads_list] Found {len(data.get('data', {}).get('downloads', []))} downloads")
        
        return {
            "status": status,
            "data": data,
        }


def find_download_by_id(downloads_response: dict, target_id: str) -> dict | None:
    """
    Находит файл в списке загрузок по его ID.
    
    Args:
        downloads_response: ответ от get_downloads_list
        target_id: UUID файла который ищем
        
    Returns:
        dict с информацией о файле или None если не найден
    """
    downloads = downloads_response.get("data", {}).get("data", {}).get("downloads", [])
    
    for download in downloads:
        if download.get("id") == target_id:
            print(f"[find_download_by_id] Найден файл: {download}")
            return download
    
    print(f"[find_download_by_id] Файл с ID {target_id} не найден")
    return None


async def generate_download_token(session: aiohttp.ClientSession) -> str | None:
    """
    Генерирует токен для скачивания файла через JSON-RPC.
    
    Returns:
        str: токен для заголовка x-download-token или None при ошибке
    """
    url = f"{BASE_URL}/ns/suppliers-auth-tokens/suppliers-portal-core/api/v1/tokensjrpc"
    
    payload = {
        "method": "generateToken",
        "params": {"team": "content-analytics"},
        "jsonrpc": "2.0",
        "id": "json-rpc_1"
    }
    
    print(f"[generate_download_token] Запрашиваем токен...")
    
    async with session.post(url, json=payload, headers=get_common_headers()) as response:
        status = response.status
        
        if status == 200:
            data = await response.json()
            token = data.get("result", {}).get("token")
            if token:
                print(f"[generate_download_token] Токен получен: {token[:50]}...")
                return token
            else:
                print(f"[generate_download_token] Токен не найден в ответе: {data}")
                return None
        else:
            text = await response.text()
            print(f"[generate_download_token] Ошибка: {status} - {text[:500]}")
            return None


async def download_file(
    session: aiohttp.ClientSession,
    download_url: str,
    download_token: str,
    save_path: str = "report.xlsx"
) -> dict[str, Any]:
    """
    Скачивает ZIP-архив по downloadUrl, извлекает XLSX и сохраняет на диск.
    
    Args:
        session: aiohttp сессия
        download_url: прямая ссылка на скачивание из ответа API
        download_token: токен для заголовка x-download-token
        save_path: путь для сохранения XLSX файла
        
    Returns:
        dict с информацией о скачивании
    """
    print(f"[download_file] Скачиваем: {download_url}")
    
    # Заголовки для скачивания с x-download-token
    headers = {
        "accept": "*/*",
        "origin": "https://seller.wildberries.ru",
        "referer": "https://seller.wildberries.ru/",
        "sec-ch-ua": '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Linux"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
        "x-download-token": download_token,
    }
    
    async with session.get(download_url, headers=headers) as response:
        status = response.status
        
        if status == 200:
            zip_content = await response.read()
            print(f"[download_file] Скачано {len(zip_content)} байт (ZIP)")
            
            # Распаковываем ZIP в памяти
            try:
                with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
                    # Получаем список файлов в архиве
                    file_list = zf.namelist()
                    print(f"[download_file] Файлы в архиве: {file_list}")
                    
                    # Ищем XLSX файл
                    xlsx_files = [f for f in file_list if f.endswith('.xlsx')]
                    
                    if not xlsx_files:
                        return {
                            "status": status,
                            "error": "XLSX файл не найден в архиве",
                            "files_in_archive": file_list,
                        }
                    
                    # Извлекаем первый XLSX файл
                    xlsx_filename = xlsx_files[0]
                    xlsx_content = zf.read(xlsx_filename)
                    
                    # Сохраняем на диск
                    with open(save_path, "wb") as f:
                        f.write(xlsx_content)
                    
                    print(f"[download_file] Извлечён {xlsx_filename} ({len(xlsx_content)} байт), сохранено в {save_path}")
                    
                    return {
                        "status": status,
                        "zip_size": len(zip_content),
                        "xlsx_size": len(xlsx_content),
                        "xlsx_filename": xlsx_filename,
                        "saved_to": save_path,
                    }
                    
            except zipfile.BadZipFile as e:
                return {
                    "status": status,
                    "error": f"Ошибка распаковки ZIP: {e}",
                }
        else:
            text = await response.text()
            print(f"[download_file] Ошибка: {status} - {text[:500]}")
            return {
                "status": status,
                "error": text,
            }


async def main():
    """Основная функция для тестирования."""
    
    # Создаём сессию с cookies
    async with aiohttp.ClientSession(cookies=COOKIES) as session:
        
        # 1. Создаём запрос на отчёт
        result = await create_report_download(
            session,
            report_type="SEARCH_ANALYSIS_PREMIUM_REPORT",
            interval="yesterday",
            limit=300000,
        )
        print(f"\n=== Результат создания отчёта ===")
        print(f"Request ID: {result['request_id']}")
        
        request_id = result["request_id"]
        
        # 2. Ждём 60 секунд пока отчёт сформируется
        print(f"\n=== Ожидаем 60 секунд... ===")
        await asyncio.sleep(60)
        
        # 3. Получаем список отчётов
        list_result = await get_downloads_list(session)
        print(f"\n=== Список отчётов ===")
        
        # 4. Ищем наш файл по ID
        download_info = find_download_by_id(list_result, request_id)
        
        if download_info:
            print(f"\n=== Найден файл ===")
            print(f"ID: {download_info['id']}")
            print(f"Status: {download_info['status']}")
            print(f"Size: {download_info['size']} bytes")
            print(f"URL: {download_info['downloadUrl']}")
            
            # 5. Скачиваем файл
            if download_info["status"] == "SUCCESS":
                # Сначала получаем токен для скачивания
                download_token = await generate_download_token(session)
                
                if download_token:
                    download_result = await download_file(
                        session,
                        download_info["downloadUrl"],
                        download_token,
                        save_path=f"{date.today()}.xlsx"
                    )
                    print(f"\n=== Результат скачивания ===")
                    print(download_result)
                else:
                    print("Не удалось получить токен для скачивания!")
            else:
                print(f"Файл ещё не готов, статус: {download_info['status']}")
        else:
            print(f"\n!!! Файл с ID {request_id} не найден в списке !!!")
            # Выводим все доступные ID для отладки
            downloads = list_result.get("data", {}).get("data", {}).get("downloads", [])
            print(f"Доступные ID: {[d['id'] for d in downloads]}")


if __name__ == "__main__":
    asyncio.run(main())

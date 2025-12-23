"""
Модуль для загрузки конфигурации прокси и токенов из ClickHouse.
"""
import asyncio
import json
from typing import NamedTuple

from clickhouse_db.get_async_connection import get_async_connection
from settings import logger


class ProxyConfig(NamedTuple):
    """Конфигурация прокси."""
    proxy_url: str
    proxy_user: str
    proxy_pass: str


async def load_proxies_from_db(server: str = "monitoring") -> list[ProxyConfig]:
    """
    Загружает прокси из таблицы radar.harvest_proxies.
    
    Выбирает одну строку по server, парсит JSON-массив из поля proxies,
    применяет одинаковые proxy_user/proxy_pass ко всем URL.
    
    Args:
        server: Значение поля server для фильтрации (по умолчанию 'monitoring').
    
    Returns:
        Список ProxyConfig с данными прокси.
    """
    async with get_async_connection() as client:
        query = f"""
            SELECT proxies, proxy_user, proxy_pass 
            FROM radar.harvest_proxies
            WHERE server = '{server}'
            LIMIT 1
        """
        q = await client.query(query)
        
        if not q.result_rows:
            logger.error(f"Прокси для server='{server}' не найдены в БД")
            return []
        
        row = q.result_rows[0]
        proxies_json = row[0]  # JSON-строка с массивом прокси
        proxy_user = row[1]
        proxy_pass = row[2]
        
        # Парсим JSON-массив прокси
        try:
            proxy_urls = json.loads(proxies_json)
        except json.JSONDecodeError as e:
            logger.error(f"Ошибка парсинга JSON прокси: {e}")
            return []
        
        result = [
            ProxyConfig(
                proxy_url=url,
                proxy_user=proxy_user,
                proxy_pass=proxy_pass
            )
            for url in proxy_urls
        ]
    
    logger.info(f"Загружено {len(result)} прокси из БД (server='{server}')")
    if result:
        logger.info(f"Первый прокси: {result[0].proxy_url}, user: {result[0].proxy_user}, pass_len: {len(result[0].proxy_pass)}")
    return result


async def load_tokens_from_db(limit: int = 4) -> list[str]:
    """
    Загружает токены из таблицы radar.wb_tokens.
    Токены сортируются по updated_at DESC.
    
    Args:
        limit: Количество токенов для загрузки (по умолчанию 4).
    
    Returns:
        Список токенов (Bearer tokens).
    """
    async with get_async_connection() as client:
        query = f"""
            SELECT token 
            FROM radar.wb_tokens 
            ORDER BY updated_at DESC 
            LIMIT {limit}
        """
        q = await client.query(query)
        result = []
        for row in q.result_rows:
            token = row[0]
            # Добавляем Bearer если его нет
            if not token.startswith("Bearer "):
                token = f"Bearer {token}"
            result.append(token)
    logger.info(f"Загружено {len(result)} токенов из БД")
    return result


def distribute_proxies(proxies: list[ProxyConfig], num_tasks: int) -> list[list[ProxyConfig]]:
    """
    Распределяет прокси равномерно между тасками.
    
    Args:
        proxies: Список всех прокси.
        num_tasks: Количество тасок для распределения.
    
    Returns:
        Список списков прокси для каждой таски.
    """
    if not proxies:
        return [[] for _ in range(num_tasks)]
    
    # Равномерное распределение
    proxies_per_task = len(proxies) // num_tasks
    remainder = len(proxies) % num_tasks
    
    result = []
    start = 0
    for i in range(num_tasks):
        # Добавляем по одному прокси из остатка к первым таскам
        extra = 1 if i < remainder else 0
        end = start + proxies_per_task + extra
        result.append(proxies[start:end])
        start = end
    
    return result


def split_requests(requests: list, num_tasks: int) -> list[list]:
    """
    Разделяет запросы равномерно между тасками.
    
    Args:
        requests: Список всех запросов.
        num_tasks: Количество тасок для распределения.
    
    Returns:
        Список списков запросов для каждой таски.
    """
    if not requests:
        return [[] for _ in range(num_tasks)]
    
    # Равномерное распределение
    requests_per_task = len(requests) // num_tasks
    remainder = len(requests) % num_tasks
    
    result = []
    start = 0
    for i in range(num_tasks):
        extra = 1 if i < remainder else 0
        end = start + requests_per_task + extra
        result.append(requests[start:end])
        start = end
    
    return result

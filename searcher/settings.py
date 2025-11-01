from os import getenv
from pathlib import Path
from sys import stdout
from dotenv import load_dotenv
from pytz import timezone
from loguru import logger

load_dotenv()

DEBUG = getenv("DEBUG", "1") == "1"

BASE_DIR = Path(__file__).parent

logger.remove()
# logger.add(
#     "app_data/logs/debug_logs.log" if DEBUG else "app_data/logs/bot.log",
#     rotation="00:00:00",
#     level="DEBUG" if DEBUG else "INFO",
# )
logger.add(stdout, level="DEBUG" if DEBUG else "INFO")

TIMEZONE = timezone("Europe/Moscow")

SEARCH_URL = "https://search.wb.ru/exactmatch/ru/common/v18/search"

WB_AUTH_TOKENS = {
    1: "Bearer ",
    2: "Bearer ",
    3: "Bearer ",
    4: "Bearer "
}

POPULAR_REQUESTS_URL = "https://seller.wildberries.ru/popular-search-requests"

GEOCODING_URL = "https://openweathermap.org/api/geocoding-api"

REDIS_HOST = getenv("REDIS_CONTAINER_NAME", "localhost")
REDIS_PORT = getenv("REDIS_PORT", "6379")

POSTGRES_CONFIG = {
    "driver": "postgresql+asyncpg",
    "username": getenv("PG_USER", "admin"),
    "password": getenv("PG_PASSWORD", "admin321"),
    "database": getenv("PG_DATABASE", "admin"),
    "host": getenv("PG_HOST", "localhost"),
    "port": getenv("PG_PORT", "5432"),
}

CLICKHOUSE_CONFIG = {
    "host": getenv("CLICKHOUSE_CONTAINER_NAME", "localhost"),
    "username": getenv("CLICKHOUSE_USER", "default"),
    "password": getenv("CLICKHOUSE_PASSWORD", ""),
    "database": getenv("CLICKHOUSE_DB", "__default__"),
}
SECRET_KEY = getenv(
    "SECRET_KEY", "FuzwkJ+n/R+BJIehXnX+xcUxnXVUZSa0sqrMMzWNjfp+aDPlL5j0BTAJpFQJnOIE"
)

ALGORITHM = "HS256"

BOT_TOKEN = getenv("BOT_TOKEN", None)

admins_list = (getenv("ADMINS", "")).split(",")
ADMINS = []
for admin_id in admins_list:
    try:
        ADMINS.append(int(admin_id))
    except ValueError:
        pass
PROXY_AUTH = {
    "username": "RUSGG9LQHH",
    "password": "Bqwz5KL5",
}

PROXIES = [
"https://net-146-19-78-104.mcccx.com:8443",
"https://net-146-19-44-241.mcccx.com:8443",
"https://net-176-126-104-120.mcccx.com:8443",
"https://net-185-88-103-253.mcccx.com:8443",
"https://net-185-81-144-46.mcccx.com:8443",
"https://net-5-181-168-160.mcccx.com:8443",
"https://net-185-61-216-109.mcccx.com:8443",
"https://net-185-61-218-207.mcccx.com:8443",
"https://net-185-61-216-102.mcccx.com:8443",
"https://net-185-96-37-244.mcccx.com:8443",
"https://net-146-19-91-50.mcccx.com:8443",
"https://net-45-66-209-63.mcccx.com:8443",
"https://net-185-96-37-200.mcccx.com:8443",
"https://net-185-81-145-55.mcccx.com:8443",
"https://net-185-96-37-38.mcccx.com:8443",
"https://net-5-183-252-197.mcccx.com:8443",
"https://net-185-96-37-56.mcccx.com:8443",
"https://net-146-19-78-150.mcccx.com:8443",
"https://net-147-78-183-44.mcccx.com:8443",
"https://net-185-102-113-164.mcccx.com:8443",
"https://net-193-233-88-199.mcccx.com:8443",
"https://net-5-183-252-71.mcccx.com:8443",
"https://net-147-78-182-134.mcccx.com:8443",
"https://net-147-78-182-198.mcccx.com:8443",
"https://net-193-233-88-194.mcccx.com:8443",
"https://net-45-66-209-151.mcccx.com:8443",
"https://net-5-183-252-254.mcccx.com:8443",
"https://net-147-78-183-68.mcccx.com:8443",
"https://net-176-126-104-32.mcccx.com:8443",
"https://net-147-78-182-54.mcccx.com:8443",
"https://net-5-183-252-216.mcccx.com:8443",
"https://net-185-61-218-142.mcccx.com:8443",
"https://net-185-81-145-124.mcccx.com:8443",
"https://net-213-232-121-48.mcccx.com:8443",
"https://net-185-102-113-254.mcccx.com:8443",
"https://net-146-19-44-48.mcccx.com:8443",
"https://net-146-19-78-55.mcccx.com:8443",
"https://net-185-102-113-154.mcccx.com:8443",
"https://net-185-88-103-112.mcccx.com:8443",
"https://net-185-96-37-112.mcccx.com:8443",
"https://net-185-102-113-242.mcccx.com:8443",
"https://net-45-66-209-225.mcccx.com:8443",
"https://net-185-81-144-30.mcccx.com:8443",
"https://net-185-81-145-253.mcccx.com:8443",
"https://net-185-96-37-71.mcccx.com:8443",
"https://net-147-78-182-187.mcccx.com:8443",
"https://net-147-78-183-164.mcccx.com:8443",
"https://net-185-102-112-94.mcccx.com:8443",
"https://net-45-66-209-217.mcccx.com:8443",
"https://net-185-96-37-188.mcccx.com:8443",
"https://net-176-126-104-66.mcccx.com:8443",
"https://net-147-78-182-172.mcccx.com:8443",
"https://net-45-66-209-103.mcccx.com:8443",
"https://net-146-19-44-108.mcccx.com:8443",
"https://net-5-181-169-134.mcccx.com:8443",
"https://net-185-102-113-97.mcccx.com:8443",
"https://net-45-66-209-64.mcccx.com:8443",
"https://net-45-66-209-26.mcccx.com:8443",
"https://net-193-233-88-110.mcccx.com:8443",
"https://net-185-88-103-104.mcccx.com:8443",
"https://net-185-61-218-22.mcccx.com:8443",
"https://net-5-181-169-138.mcccx.com:8443",
"https://net-185-102-112-217.mcccx.com:8443",
"https://net-146-19-44-29.mcccx.com:8443",
"https://net-176-126-104-196.mcccx.com:8443",
"https://net-5-181-169-133.mcccx.com:8443",
"https://net-185-88-103-60.mcccx.com:8443",
"https://net-185-61-216-53.mcccx.com:8443",
"https://net-185-81-145-23.mcccx.com:8443",
"https://net-176-126-104-194.mcccx.com:8443",
"https://net-185-96-37-254.mcccx.com:8443",
"https://net-5-181-168-208.mcccx.com:8443",
"https://net-146-19-39-126.mcccx.com:8443",
"https://net-176-126-104-115.mcccx.com:8443",
"https://net-213-232-121-81.mcccx.com:8443",
"https://net-185-81-144-250.mcccx.com:8443",
"https://net-146-19-91-209.mcccx.com:8443",
"https://net-5-181-169-221.mcccx.com:8443",
"https://net-185-61-216-60.mcccx.com:8443",
"https://net-185-61-216-82.mcccx.com:8443",
"https://net-193-233-88-55.mcccx.com:8443",
"https://net-185-102-112-219.mcccx.com:8443",
"https://net-146-19-78-43.mcccx.com:8443",
"https://net-5-181-169-102.mcccx.com:8443",
"https://net-5-183-252-22.mcccx.com:8443",
"https://net-185-81-144-17.mcccx.com:8443",
"https://net-185-81-144-146.mcccx.com:8443",
"https://net-185-81-144-117.mcccx.com:8443",
"https://net-185-61-216-108.mcccx.com:8443",
"https://net-185-96-37-60.mcccx.com:8443",
"https://net-147-78-183-96.mcccx.com:8443",
"https://net-213-232-121-53.mcccx.com:8443",
"https://net-185-96-37-127.mcccx.com:8443",
"https://net-146-19-44-42.mcccx.com:8443",
"https://net-193-233-88-94.mcccx.com:8443",
"https://net-185-96-37-196.mcccx.com:8443",
"https://net-185-88-103-84.mcccx.com:8443",
"https://net-185-61-216-92.mcccx.com:8443",
"https://net-185-61-216-74.mcccx.com:8443",
"https://net-146-19-44-222.mcccx.com:8443",
]
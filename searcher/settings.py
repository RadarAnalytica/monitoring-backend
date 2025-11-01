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
"http://146.19.78.104:8085",
"http://146.19.44.241:8085",
"http://176.126.104.120:8085",
"http://185.88.103.253:8085",
"http://185.81.144.46:8085",
"http://5.181.168.160:8085",
"http://185.61.216.109:8085",
"http://185.61.218.207:8085",
"http://185.61.216.102:8085",
"http://185.96.37.244:8085",
"http://146.19.91.50:8085",
"http://45.66.209.63:8085",
"http://185.96.37.200:8085",
"http://185.81.145.55:8085",
"http://185.96.37.38:8085",
"http://5.183.252.197:8085",
"http://185.96.37.56:8085",
"http://146.19.78.150:8085",
"http://147.78.183.44:8085",
"http://185.102.113.164:8085",
"http://193.233.88.199:8085",
"http://5.183.252.71:8085",
"http://147.78.182.134:8085",
"http://147.78.182.198:8085",
"http://193.233.88.194:8085",
"http://45.66.209.151:8085",
"http://5.183.252.254:8085",
"http://147.78.183.68:8085",
"http://176.126.104.32:8085",
"http://147.78.182.54:8085",
"http://5.183.252.216:8085",
"http://185.61.218.142:8085",
"http://185.81.145.124:8085",
"http://213.232.121.48:8085",
"http://185.102.113.254:8085",
"http://146.19.44.48:8085",
"http://146.19.78.55:8085",
"http://185.102.113.154:8085",
"http://185.88.103.112:8085",
"http://185.96.37.112:8085",
"http://185.102.113.242:8085",
"http://45.66.209.225:8085",
"http://185.81.144.30:8085",
"http://185.81.145.253:8085",
"http://185.96.37.71:8085",
"http://147.78.182.187:8085",
"http://147.78.183.164:8085",
"http://185.102.112.94:8085",
"http://45.66.209.217:8085",
"http://185.96.37.188:8085",
"http://176.126.104.66:8085",
"http://147.78.182.172:8085",
"http://45.66.209.103:8085",
"http://146.19.44.108:8085",
"http://5.181.169.134:8085",
"http://185.102.113.97:8085",
"http://45.66.209.64:8085",
"http://45.66.209.26:8085",
"http://193.233.88.110:8085",
"http://185.88.103.104:8085",
"http://185.61.218.22:8085",
"http://5.181.169.138:8085",
"http://185.102.112.217:8085",
"http://146.19.44.29:8085",
"http://176.126.104.196:8085",
"http://5.181.169.133:8085",
"http://185.88.103.60:8085",
"http://185.61.216.53:8085",
"http://185.81.145.23:8085",
"http://176.126.104.194:8085",
"http://185.96.37.254:8085",
"http://5.181.168.208:8085",
"http://146.19.39.126:8085",
"http://176.126.104.115:8085",
"http://213.232.121.81:8085",
"http://185.81.144.250:8085",
"http://146.19.91.209:8085",
"http://5.181.169.221:8085",
"http://185.61.216.60:8085",
"http://185.61.216.82:8085",
"http://193.233.88.55:8085",
"http://185.102.112.219:8085",
"http://146.19.78.43:8085",
"http://5.181.169.102:8085",
"http://5.183.252.22:8085",
"http://185.81.144.17:8085",
"http://185.81.144.146:8085",
"http://185.81.144.117:8085",
"http://185.61.216.108:8085",
"http://185.96.37.60:8085",
"http://147.78.183.96:8085",
"http://213.232.121.53:8085",
"http://185.96.37.127:8085",
"http://146.19.44.42:8085",
"http://193.233.88.94:8085",
"http://185.96.37.196:8085",
"http://185.88.103.84:8085",
"http://185.61.216.92:8085",
"http://185.61.216.74:8085",
"http://146.19.44.222:8085",
]
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
    1: "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3NjE5MTkxNTUsInVzZXIiOiIxMTI0NjIxNjAiLCJzaGFyZF9rZXkiOiIxNyIsImNsaWVudF9pZCI6IndiIiwic2Vzc2lvbl9pZCI6IjRjZmNhN2ExMjEwNjQ3MzRhYjNiZTU2ZmQyMmNkMWViIiwidmFsaWRhdGlvbl9rZXkiOiI1OGNmNDQyNTA4MTNkM2ZmZDhkNmI0YzI4NmZmNGQyMTg5ZjlkMTY3NjBmNWZiYmJlN2Y3ZDY3YjY1MDNmNjlkIiwicGhvbmUiOiJ1NXlqZThTN29FelpONVE0dFNiVmt3PT0iLCJ1c2VyX3JlZ2lzdHJhdGlvbl9kdCI6MTY4MzgwNTc4NCwidmVyc2lvbiI6Mn0.c4cptVXS5x_pj1t62eB3vTNBRrWUYgxxVpzDIy0eWABN95lqQ81_nCNMtgy5utjG57qcoqeJR3mgAJW8uT3crdMMvmnKfHkWSUcqeYueAR9xkuuFG80Mpex019UJSww9q533noDF0PtFPgEMYtsi7f2AAC0jf_jBKNG_6PtIeq1IcrrfNFKP0yfkCD9CW_Gws1XpFpq_hozpxRXuyA9FMrWe-osl72aM1aNw8Dl66lrDr8LQAUN1pTPxnJxQRWEirqjZl-UScPZpJ1xBWgB1VQUGnvEgqU5mPFfzJ-sVQGE_hI-TqvpRtPoUI3mR3FS234e4zzshtCGPGd28k6zdhw",
    2: "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3NjIwMTkxMzQsInVzZXIiOiIxMzkyOTgxMjMiLCJzaGFyZF9rZXkiOiIxNCIsImNsaWVudF9pZCI6IndiIiwic2Vzc2lvbl9pZCI6IjcyZTdiZDYxODNhYzQwZDViYTI5OTEyYmI0OWU4OGM5IiwidmFsaWRhdGlvbl9rZXkiOiJkNjgxNWVlYzA2Njg3OWFkZDMxOGE4Y2JiYmVkNGUxMTJkNDUwNGM5NzA5MDhkOGUxYjc5NTA2MzQ1ZTY3MjRmIiwicGhvbmUiOiJpQlZiZUp4aHNGV1AzQzdSeUtBVnVnPT0iLCJ1c2VyX3JlZ2lzdHJhdGlvbl9kdCI6MTcwMzg3MzU1NSwidmVyc2lvbiI6Mn0.fG1wIpQ0vZf5ci1-wVrVbhRtNuHKKhM9QcOgyUinRfF7m6gQ5Ux0a_ARg8VzuPd_wdq_7kn6x2pZZtit0nyTCSSp7LHLf6b1bsprnTww_zZnzzAfborhgEEQuKfQiDgaxzCTfUDphjIctZD-cY2tBaXknEFz5742fRQhZe8OGD6b4HfGVZm8J8GheCDcCrSmV9bGOJHvmA5RhelDM7BHY-SKt5QQUBvViPrrB9uQYpWwuEuhPCP6SQyvNlGQYYocjliOnRmR7OcZBUmlAJLKZ24nHXr_iiVdDkXN6B7tVV9bFPbp2fai-CNxNMeSuupFzvbzMuIBA4xZIsPG8eu-aA",
    3: "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3NjIwMTkyNjQsInVzZXIiOiIxNDg1ODgwNjUiLCJzaGFyZF9rZXkiOiIxMiIsImNsaWVudF9pZCI6IndiIiwic2Vzc2lvbl9pZCI6ImQ5NWE4ZWJjMWNhMTQwNWViMTE0ODdlNTY0MDcxNzE0IiwidmFsaWRhdGlvbl9rZXkiOiJkNjgxNWVlYzA2Njg3OWFkZDMxOGE4Y2JiYmVkNGUxMTJkNDUwNGM5NzA5MDhkOGUxYjc5NTA2MzQ1ZTY3MjRmIiwicGhvbmUiOiJpTTlLeUVBWDZJR1YxeTlOaHBzUTVBPT0iLCJ1c2VyX3JlZ2lzdHJhdGlvbl9kdCI6MTcxMTM4MTE3OCwidmVyc2lvbiI6Mn0.aRti_oxaOOeQTH0_KVN-oRzVuElUTz_1myM6HEx3ga3fS4IBsEc3jFZVXce3HqNkF_cJdF7sJe9x5zC6Jy71FJUzrjt6aJk7j5R2ZT-505K3ZHAJyMylNQSOduZgDR9fkvnjs33n5_Ts5gUtpJO_79CoZdwcEVLIgT75ufO_ebkFomRXx79Hc44_Eu1kKrIQlP3V6YxW6NAAz-QiiQyFVPk5ytshlM8AIt4EPnQdqOH2l1sHyvNZ86_61q8tPGEpL9LB3WNapNULV35nbed2LPNJba7DJPTw730l1n5HMdrcguv9pfMdPl-ekBr2_KxX_F5mr74BcmQPZ7iz-sqFew",
    4: "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3NjIwMTkzMzYsInVzZXIiOiIzMDM2NTI2MzUiLCJzaGFyZF9rZXkiOiIyOSIsImNsaWVudF9pZCI6IndiIiwic2Vzc2lvbl9pZCI6IjZiYTJjNTcwYzA4NDQ3MjhiY2Y0ZDcwYjJiNzQzNWJhIiwidmFsaWRhdGlvbl9rZXkiOiJkNjgxNWVlYzA2Njg3OWFkZDMxOGE4Y2JiYmVkNGUxMTJkNDUwNGM5NzA5MDhkOGUxYjc5NTA2MzQ1ZTY3MjRmIiwicGhvbmUiOiJWSDh5UERZejJ0REFRYmFBc1RmWHNnPT0iLCJ1c2VyX3JlZ2lzdHJhdGlvbl9kdCI6MTc2MDg1NTI3NCwidmVyc2lvbiI6Mn0.dwd6n_en0l7BFc9iwcqx4xMWpg2nUxH9UVuxyPkVB4kief5ZhnVMFDIoJ9PtKutl2zZO3MX3hrkN_fGzuVGHpdsv7EJH7tfzxaU4rNMZ2vi9pADC0IEtoRnn3enmd3qkOAvpHzmDqxcF2J9a1uB0JLR2X-13lRk2PXjdLriDQFigVZ5FGIwCqiwBfbVmPc4OHIcqlisOBEJRQTCP5-ALBVFm5BB7gdCYhtm_FN35sjOiWKf7iTjMSLqCa8sahtdXEER4lqcEy2aksoEvN7fM54mAuacEkkl7avMT5y_1_nYIX5jQXRpGCXtCJTIFzCIdDr3C-mOrfCqaMVemucKrJQ"
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
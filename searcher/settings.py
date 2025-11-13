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

WB_AUTH_TOKENS_FOR_WORKERS = {
    1: (
        "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3NjI4NDY2NTksInVzZXIiOiIzMDQ4NDU1MDEiLCJzaGFyZF9rZXkiOiIyNiIsImNsaWVudF9pZCI6IndiIiwic2Vzc2lvbl9pZCI6ImE0OWQyNjE3MjM1ODQ2NGQ5MTNlNmUzZTBkZmRhNTRkIiwidmFsaWRhdGlvbl9rZXkiOiI4ZDA1YmRiZjRhMzk1ODg2NGEwNTM2NjZkNjRhNTIxMWVhYzY4NmZlY2NjY2YwYTE5NmMxYTgyOTc3ZGYxNzVjIiwicGhvbmUiOiJpTzJLaHBYVFp1YzhCVWQvZTVhSVBnPT0iLCJ1c2VyX3JlZ2lzdHJhdGlvbl9kdCI6LTYyMTM1NTk2ODAwLCJ2ZXJzaW9uIjoyfQ.nyOaYShY5BHjKToatWTSREd_vSrm4rNg45tLMkTyPCKO1DvEQSMx_3blalXvxfFnS__alTTeaXXuVWCuc42kg7ZTT25lKFC7SB62U3AGHFV1-metzzLlhixZwYg3SsDs82JAbMPqw32tnW_rCV6h34KPELm21_9ROvOTXImmWryPPRDJCHegIp4hCAPomC_BOYE1UqAm1sKFl8tC32eF3N5UNn2uZwvWmqQvqwSUx4NBplq-fyDYyGGIEhCpZphmuLVFze3ACcWIzo0KUq294YHAg_K_O9UFcjqfFCI-90Rg20OOw8ANoy9RAmUSKFYddRnvgqzVpIUuL5sy9OkOng",
        "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3NjI4NDY3NTcsInVzZXIiOiIzMDQ4NDU1ODkiLCJzaGFyZF9rZXkiOiIyOSIsImNsaWVudF9pZCI6IndiIiwic2Vzc2lvbl9pZCI6ImNiZGViNTE4YzdlZTRmMWQ5NTc0YjdmMWQxMzlmOTg3IiwidmFsaWRhdGlvbl9rZXkiOiJjZDQ5MTlmZTg3MTBjYzNmZDUwNGFlN2ExYjg3MDg4NTYwYjM3MDgzNDcwMDUyZTIwYTlkYjY0OWM5NTBhZWRhIiwicGhvbmUiOiIyQ0Q5NzVUZ1JROTUzeW54S1NXeGlBPT0iLCJ1c2VyX3JlZ2lzdHJhdGlvbl9kdCI6LTYyMTM1NTk2ODAwLCJ2ZXJzaW9uIjoyfQ.Zh3q1Cy9AvLI83dua9XpoOR5OLmHtn2Hjj0bh0gBQUz6g4PcCu0ptm8iiZtspmOrkLbiNbI1ITl8hOoivNeorgKdKFhFCZCLD9QDLY1LjeVORoLcxE1AZJHRnexSxZLVQeTYADDtJeOMwOI9TQlBpaCTGlMqURdL8vMDXd2eM2gaOG3YAXX-QCnPwt1Gi2CqME2PjHYuVxorVGftIIdtoE656m2I64ywV0Aren30hwtfcymhfMNvzGeALs2HzqbkmiYBb2TPSvWeuI7FMcJGJoOhgDp8V9RsGmhVvoiZ6pUBV8r1swX15Dj62OggESJ4E03Qe_Vp-YExeHSYQhHfWQ",
    ),
    2:(
        "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3NjI4NDU2OTksInVzZXIiOiI1MTg5ODY5OCIsInNoYXJkX2tleSI6IjE1IiwiY2xpZW50X2lkIjoid2IiLCJzZXNzaW9uX2lkIjoiNjFhNGI4ZjVjNWM3NGIzMzhhMmJlY2MyZjJiMTZlNGMiLCJ2YWxpZGF0aW9uX2tleSI6IjM5ZTdmM2ZlMzdlNWEyMzRkMzIzMzE4MDYwYWQwZWJkN2Y0Y2UzMDg1OTAyMDQ3Yjk5ZGM5MzJhMzhiZmVlNjUiLCJwaG9uZSI6InpVTmpZQ0l2bjVyc2VTb1ZDUzU5c2c9PSIsInVzZXJfcmVnaXN0cmF0aW9uX2R0IjoxNjc3NjAyNDE5LCJ2ZXJzaW9uIjoyfQ.IVZud8b_of0oHTRaESjYPiLHl4fQTFBUONtQzCXa4UjAh2NvpXzUyJJ1NDD0Hntul5fouh4nOyslKgnz5ap0dl7i4Hr3NIe7WandgVwA2h2Yp6cbK_FLyKZ8qeV1Ay_P6bswKaLrQzGgEp6rCal5jQYc0ClPIQZEzfGNC9KjJZo4e5x0PNOZhP5llr44fV7CXvxdJP478cjHxf9YHV8QvQcBkC6xbugT_ioFkqcCdJYkelliXddEbSMtzbbxo4GPkrVmqzX-aKeplfInf9NEs2W_dTMPbqboWUdbw2kpxOvFvAMuUUFS5fDAKxPA9mf3zQ5rJUGp8Qi2LZDXnD7zQg",
        "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3NjI4NjUzMTksInVzZXIiOiI0NDQ2MjE3NSIsInNoYXJkX2tleSI6IjE4IiwiY2xpZW50X2lkIjoid2IiLCJzZXNzaW9uX2lkIjoiY2RiOWZiZWJjNDNiNDFlZGE0ODk3NTc2ZjJjOWMyODgiLCJ2YWxpZGF0aW9uX2tleSI6ImQ1ZTI0NTc5Y2E0M2ZmOWUzZDVhMDAwZjhkMmMzNzhkMDJlZDZiM2E1ZTk0MjcxNjQxNDU5MjE4ZjRiYzM3YjUiLCJwaG9uZSI6InJzTWJ2YW1KQUdFQ0JMVkJXWmJvMkE9PSIsInVzZXJfcmVnaXN0cmF0aW9uX2R0IjoxNjc2OTA2MjI1LCJ2ZXJzaW9uIjoyfQ.Et8gYKeQTxH7Mo9cpXZ9-OVuYEivCCp28kT84oWk63O_zuROFmSYZ0bLy7cyz7pa9RV-TvABZwn_FEHW31K66ut3XJVQu0iUHTYZqMXxINwEKxTFVEhek43FXxZJFMBXa2JcpWw7lYxP_Jd_sPUZPOkUBpmh-jorr5vqn-SrzUPZgpSrQpvg-pJ6y_KdCJDY3iKZFQjc1rHm118wXkRnJbk3tGavv1ZLON8v_PMOAYMoPP9jKnGCBHr1_YB1sl8PPP0p15p6Ri88l1A-vA1JTd79KrmqYyM_pS6i5arH5nUkj8Eu8nHGuaVZF8gtfhBZXjCaEiAN2YysNOiMCO3C1g",
    ),
    3:(
        "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3NjI5NjQ5NDAsInVzZXIiOiIzMDM2NTI2MzUiLCJzaGFyZF9rZXkiOiIyOSIsImNsaWVudF9pZCI6IndiIiwic2Vzc2lvbl9pZCI6IjM0ZTBhNTBkZGIwYjQyZjFiMjgxY2JlYTUxMjg3MzE0IiwidmFsaWRhdGlvbl9rZXkiOiI3MTYzOGRkZDNkZmJiY2UwYjBmYWI1YzJiZjAxNjBhOWY0YThjOGNhMjEzOGYwNDNjZWQ4NzlhZjhhYmVkMGU3IiwicGhvbmUiOiJWSDh5UERZejJ0REFRYmFBc1RmWHNnPT0iLCJ1c2VyX3JlZ2lzdHJhdGlvbl9kdCI6MTc2MDg1NTI3NCwidmVyc2lvbiI6Mn0.eHwuiVEuKY_0kzbxd_GY6OsnKiQTn0VxVT7ox3qG9Lt28KycEHd705R2fIEprQ-OJ0sd-em_7kHdXgGNU1DCtUOF7rzieegmjFdn02E3PvqR4DYG9AK9ikAraI0s7nAvIez7rgHPDSB35TOLc5BwmDRVnkvq28PrpAh2u-IR_PjFa9peSxTgc_9cV00QeZ9FrWP5ln3kYdAFvTxQUBwbF015KC069fDOKpzh7Iaeu9qgRVwcIQlgJRumQpVfWfo2KD0DozjwMSMYsZ6HWjQafyOe5bwwlfKmZuEk5gk6HvXRf1B-ZtUP6bJs_427skBfXZOiN8lI-fCqiZ4Z6VBaHA",
        "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3NjMwMTQ1NTcsInVzZXIiOiIxNDg1ODgwNjUiLCJzaGFyZF9rZXkiOiIxMiIsImNsaWVudF9pZCI6IndiIiwic2Vzc2lvbl9pZCI6ImMyMDBkYmRmMmE2YjQ2ZTdhNmEwNDdjZjEzMGVjMGQ3IiwidmFsaWRhdGlvbl9rZXkiOiI3MTYzOGRkZDNkZmJiY2UwYjBmYWI1YzJiZjAxNjBhOWY0YThjOGNhMjEzOGYwNDNjZWQ4NzlhZjhhYmVkMGU3IiwicGhvbmUiOiJpTTlLeUVBWDZJR1YxeTlOaHBzUTVBPT0iLCJ1c2VyX3JlZ2lzdHJhdGlvbl9kdCI6MTcxMTM4MTE3OCwidmVyc2lvbiI6Mn0.eA7d98EIfcRo2E1a9VfgWW_9f_wJPLP2VbQnBk5yuGjKxLCvA9EYGpAhBn2qD734rW30Z5F4FG49oQbP1lmDoi5bf7TEWh3OuI5HTQEv7JPMfOA414Ln1-Z8eudBjomxyYryRFmhRtZT32HcHumdgBOY1AiL51kABhX14j7HjxAglavfN-RKNgH8g18-G8M9TxObeLzmjALr5hlrPR0jkdOSgqYvh1IMJhQr3xEzum9NFzXMifoNEVQToD-XElEsricN7H6F2CMUdoqcm1cgsDV5zAPRgnWCspj6wCia5mJr6aszMKUiZcaEJWv9MkCr2cs51c0PL-DsnUT8XUt4aA",
    ),
    4:(
        "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3NjMwMTQ3MzAsInVzZXIiOiIxMzkyOTgxMjMiLCJzaGFyZF9rZXkiOiIxNCIsImNsaWVudF9pZCI6IndiIiwic2Vzc2lvbl9pZCI6ImZhY2FmNDZhODA5ZDQyMTA5YjBjOWVhYTI0YjBiYzM5IiwidmFsaWRhdGlvbl9rZXkiOiI0NTNiYzc4YTBhZTk1MmEyOWQwYzQzZjk0YzBkZGI1ZWVkNDQ4Y2I0YzIwMzkxZTY2YzIwYjk1YmY2MzA5ZThlIiwicGhvbmUiOiJpQlZiZUp4aHNGV1AzQzdSeUtBVnVnPT0iLCJ1c2VyX3JlZ2lzdHJhdGlvbl9kdCI6MTcwMzg3MzU1NSwidmVyc2lvbiI6Mn0.VrOWoeDgvNtShqQ2uRJhj1zhvpQ3d5B0OWPSJbRDpWBDujkhysH2FYCZ-HfMGAoV3SyxHGK6DcNyCC7DIlxrTAETjGYnsfVU8AgXsAglaGkt8Pu6OntIiZRX-BGhGVoqxOM9UGWKvsYqkjQOefjN2ltIceP6GtJ4PMwNtMdc9GU24i4_ZxHPuYzLGT-okV9IM7okFheYy4NAcIGLQ1BKOkfXu3hucj8zeUzQFKVjPCF508db_KIA49-Oz0OEqCDUq2lE3fg7SZRlSgeDEcNqniRQmgjM1wPBmXAYeA32_yTnP3dagYppS45nsk3TYdXivtKEy4ED51w6dhjUkulY1g",
        "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3NjMwMzg5NTEsInVzZXIiOiIxMTI0NjIxNjAiLCJzaGFyZF9rZXkiOiIxNyIsImNsaWVudF9pZCI6IndiIiwic2Vzc2lvbl9pZCI6IjRjZmNhN2ExMjEwNjQ3MzRhYjNiZTU2ZmQyMmNkMWViIiwidmFsaWRhdGlvbl9rZXkiOiI1OGNmNDQyNTA4MTNkM2ZmZDhkNmI0YzI4NmZmNGQyMTg5ZjlkMTY3NjBmNWZiYmJlN2Y3ZDY3YjY1MDNmNjlkIiwicGhvbmUiOiJ1NXlqZThTN29FelpONVE0dFNiVmt3PT0iLCJ1c2VyX3JlZ2lzdHJhdGlvbl9kdCI6MTY4MzgwNTc4NCwidmVyc2lvbiI6Mn0.nEFmL60g-GYQT81mFWYl0wXOIJnbwa-7dFZ_b3wFZBoX3h9ylji0YFKaAKJAIfYLnJubeKsjYDotfhhj17oIMT9YQYn696nmpPRMx4cVqMLisPFlKPgGsT46jEvbZKSwId75KzZv1IVDZy6BccK0gnzNoAo1u3wLRBAjW8cTI3AR4qDVObWWi1rx3Ww01zyDz8EUkqf77RWemKKDLtAj27UsClT2_ueVFiolzOvZGQVAaZVHkmVQnPPniIluOldeZksncQnAa1L0iV0meZhvwnr5yuJndLYtGmXRSFHGtTDFuoJOEyeOUWO-vhJR-WLcgPUAGerd_qDe1c030MkibA",
    )
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
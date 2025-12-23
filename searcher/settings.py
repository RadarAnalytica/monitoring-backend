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

# Количество Celery-тасок для параллельной обработки поисковой выдачи
NUM_SEARCH_TASKS = 4

WB_AUTH_TOKENS = {
    1: "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3NjM5Njk0NzYsInVzZXIiOiIzMDQ4NDU1MDEiLCJzaGFyZF9rZXkiOiIyNiIsImNsaWVudF9pZCI6IndiIiwic2Vzc2lvbl9pZCI6ImE0OWQyNjE3MjM1ODQ2NGQ5MTNlNmUzZTBkZmRhNTRkIiwidmFsaWRhdGlvbl9rZXkiOiI4ZDA1YmRiZjRhMzk1ODg2NGEwNTM2NjZkNjRhNTIxMWVhYzY4NmZlY2NjY2YwYTE5NmMxYTgyOTc3ZGYxNzVjIiwicGhvbmUiOiJpTzJLaHBYVFp1YzhCVWQvZTVhSVBnPT0iLCJ1c2VyX3JlZ2lzdHJhdGlvbl9kdCI6LTYyMTM1NTk2ODAwLCJ2ZXJzaW9uIjoyfQ.hBs_lteZ-BLir8bXM150xbnnn9_pJzCOzwEZk60xcWqiyqj-H1aNguPW-Hx4mjYg0HZP1p1Yt-Gg_kWJ7WmzmPm8r2XUKw2r3TPCjlQlgfT0bUhHyDPq9bntUQy-HmvTSB2nGm88fTeGNourImhlnWkiOuahv4XbSS96Z2PxqoJF3OcmpOOKz4c9pRlWZsHbknoIf7plMOZIf_AC_gBXAsRIGfXpYNTxLEFDVF7iobJFO3IMjujr1SneIhyM8wyfuObOAkviSHPoV7pgC2OAUryLL1DGA_76XNOZMUFlTI2IZyHGxh336ojdJW8IOYnAag2ZI40sB5JuPh48HzO9Gw",
    2: "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3NjM5MjI5NDAsInVzZXIiOiI0NDQ2MjE3NSIsInNoYXJkX2tleSI6IjE4IiwiY2xpZW50X2lkIjoid2IiLCJzZXNzaW9uX2lkIjoiY2RiOWZiZWJjNDNiNDFlZGE0ODk3NTc2ZjJjOWMyODgiLCJ2YWxpZGF0aW9uX2tleSI6ImQ1ZTI0NTc5Y2E0M2ZmOWUzZDVhMDAwZjhkMmMzNzhkMDJlZDZiM2E1ZTk0MjcxNjQxNDU5MjE4ZjRiYzM3YjUiLCJwaG9uZSI6InJzTWJ2YW1KQUdFQ0JMVkJXWmJvMkE9PSIsInVzZXJfcmVnaXN0cmF0aW9uX2R0IjoxNjc2OTA2MjI1LCJ2ZXJzaW9uIjoyfQ.MV8OiRJVzEzCK8Oin_rNT_gSmZVCZNYJxpzzsRq_jDEX74EbWHsy8UfVij5BvSHRnPZiM7GkQgoe9QCOM2lQ5Cet0ssgxHIcjCHOLW9dFWVyXrNL3iXaD_pXuVxilhpHPTT-hBRs1g3z2uppiTkGan3HwhGth1LtkShESdjvENQDql6BLYM3b6A58Gh9jiR20IUAKmaJo3BMlqskxQ31HCMBHijyXNO43xpPOMIyOL-Os0GhIb3JCwUV1H96_II1_PDrSpk_9jppvdo_H7v6Jb2TkLmkHhKXBNVX13QtLaOla1mw80dbDs3uqDVtdJ957TZvKhUl80bCCwnjswCAHg",
    3: "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3NjQwOTIxNDIsInVzZXIiOiIxNDg1ODgwNjUiLCJzaGFyZF9rZXkiOiIxMiIsImNsaWVudF9pZCI6IndiIiwic2Vzc2lvbl9pZCI6IjEwNTFiZDYyZTI5ODQ1NDZhMDEyNmI1YTliM2FhZjFhIiwidmFsaWRhdGlvbl9rZXkiOiIyZjhjYmY2NTViZWNiYzdhNjIwYmFlMjUwYzM1MmMzOWYxYzMyZmZiODJkZDM2YmQxODIzMjU0NGE3NzdmNjY1IiwicGhvbmUiOiJpTTlLeUVBWDZJR1YxeTlOaHBzUTVBPT0iLCJ1c2VyX3JlZ2lzdHJhdGlvbl9kdCI6MTcxMTM4MTE3OCwidmVyc2lvbiI6Mn0.CJpsKzrgQ9JIr789X6tcnJrs4cxEVSAtOZQynRgR6HRodnxLLj3xxwnbTa6e5enOPtWywbLday5AaBdpCpULB3oJ6u5Md5ahB-qaANnAReNkRcmUY3206HmyN4JMh42WFX-JJeij4chyM50AN3c-oDrX2Nb0QBPS75ga7Y7kWZ3pUQcj5eVQ0lm3VBUVdKVWiSO53CDYYJYcQxsc9odeb7XvCcyHTfE4v4gz0rs5RwwqdMJc-F7an19JlnQTYEQJ5SrFu3HrR_prCYYye6flkBYT-KJv4_m5JiDAHzz2KMwZHrHUWLws3acyvNxNygHQXe9Q_yx5hx4qddp6h3WJUw",
    4: "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3NjQwOTIyNTksInVzZXIiOiIzMDM2NTI2MzUiLCJzaGFyZF9rZXkiOiIyOSIsImNsaWVudF9pZCI6IndiIiwic2Vzc2lvbl9pZCI6IjJiZTk3YWRhOWI3NDQ0YTViYmRiODFkOTM4ZTJkNTZmIiwidmFsaWRhdGlvbl9rZXkiOiJmOGU5YjU4MjU0OTExODA0ZWRkZWU2MDhmMDMzM2FjYzc4MjNkYjZiZjBiMDI3MDQ3MWFlMDQ3YThmZjNiMjI3IiwicGhvbmUiOiJWSDh5UERZejJ0REFRYmFBc1RmWHNnPT0iLCJ1c2VyX3JlZ2lzdHJhdGlvbl9kdCI6MTc2MDg1NTI3NCwidmVyc2lvbiI6Mn0.o8SYNezWoLRmcT5GpuV6W7alIFgLizQ93UW0v2MGeOBjs01I7k4I4Z4lx83cqinQxivRZBMw2m0I1LgTu0Z3CrxeNxncNqy2s0rzrcjozi_pUHNipbWM0G1kY6gv0qlEAhz8kVCbM1En_bXyvGRIrqc6Gvb-OTZQdBd4GmXc_Aca7UVsxFFQUHqcp1cWtyXW12V_KlwiCYDfGwwWgt0E8FyQB4_Oc6buixsuFG_tNt8OPW1G7_Y_-4KGDNeqvLRfn6pZEwT-JPaarMim-lA_Q4TWF2jpua7d1Z9W8vMb-Z0cYH76jiTlA15sIO1xf11i63kud0Vq1kQGK_HU2ZX-gQ"
}

WB_AUTH_TOKENS_FOR_WORKERS = {
    1: (
        "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3NjM5Njk0NzYsInVzZXIiOiIzMDQ4NDU1MDEiLCJzaGFyZF9rZXkiOiIyNiIsImNsaWVudF9pZCI6IndiIiwic2Vzc2lvbl9pZCI6ImE0OWQyNjE3MjM1ODQ2NGQ5MTNlNmUzZTBkZmRhNTRkIiwidmFsaWRhdGlvbl9rZXkiOiI4ZDA1YmRiZjRhMzk1ODg2NGEwNTM2NjZkNjRhNTIxMWVhYzY4NmZlY2NjY2YwYTE5NmMxYTgyOTc3ZGYxNzVjIiwicGhvbmUiOiJpTzJLaHBYVFp1YzhCVWQvZTVhSVBnPT0iLCJ1c2VyX3JlZ2lzdHJhdGlvbl9kdCI6LTYyMTM1NTk2ODAwLCJ2ZXJzaW9uIjoyfQ.hBs_lteZ-BLir8bXM150xbnnn9_pJzCOzwEZk60xcWqiyqj-H1aNguPW-Hx4mjYg0HZP1p1Yt-Gg_kWJ7WmzmPm8r2XUKw2r3TPCjlQlgfT0bUhHyDPq9bntUQy-HmvTSB2nGm88fTeGNourImhlnWkiOuahv4XbSS96Z2PxqoJF3OcmpOOKz4c9pRlWZsHbknoIf7plMOZIf_AC_gBXAsRIGfXpYNTxLEFDVF7iobJFO3IMjujr1SneIhyM8wyfuObOAkviSHPoV7pgC2OAUryLL1DGA_76XNOZMUFlTI2IZyHGxh336ojdJW8IOYnAag2ZI40sB5JuPh48HzO9Gw",
        "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3NjM5MjI5NDAsInVzZXIiOiI0NDQ2MjE3NSIsInNoYXJkX2tleSI6IjE4IiwiY2xpZW50X2lkIjoid2IiLCJzZXNzaW9uX2lkIjoiY2RiOWZiZWJjNDNiNDFlZGE0ODk3NTc2ZjJjOWMyODgiLCJ2YWxpZGF0aW9uX2tleSI6ImQ1ZTI0NTc5Y2E0M2ZmOWUzZDVhMDAwZjhkMmMzNzhkMDJlZDZiM2E1ZTk0MjcxNjQxNDU5MjE4ZjRiYzM3YjUiLCJwaG9uZSI6InJzTWJ2YW1KQUdFQ0JMVkJXWmJvMkE9PSIsInVzZXJfcmVnaXN0cmF0aW9uX2R0IjoxNjc2OTA2MjI1LCJ2ZXJzaW9uIjoyfQ.MV8OiRJVzEzCK8Oin_rNT_gSmZVCZNYJxpzzsRq_jDEX74EbWHsy8UfVij5BvSHRnPZiM7GkQgoe9QCOM2lQ5Cet0ssgxHIcjCHOLW9dFWVyXrNL3iXaD_pXuVxilhpHPTT-hBRs1g3z2uppiTkGan3HwhGth1LtkShESdjvENQDql6BLYM3b6A58Gh9jiR20IUAKmaJo3BMlqskxQ31HCMBHijyXNO43xpPOMIyOL-Os0GhIb3JCwUV1H96_II1_PDrSpk_9jppvdo_H7v6Jb2TkLmkHhKXBNVX13QtLaOla1mw80dbDs3uqDVtdJ957TZvKhUl80bCCwnjswCAHg",
    ),
    2:(
        "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3NjQwOTIxNDIsInVzZXIiOiIxNDg1ODgwNjUiLCJzaGFyZF9rZXkiOiIxMiIsImNsaWVudF9pZCI6IndiIiwic2Vzc2lvbl9pZCI6IjEwNTFiZDYyZTI5ODQ1NDZhMDEyNmI1YTliM2FhZjFhIiwidmFsaWRhdGlvbl9rZXkiOiIyZjhjYmY2NTViZWNiYzdhNjIwYmFlMjUwYzM1MmMzOWYxYzMyZmZiODJkZDM2YmQxODIzMjU0NGE3NzdmNjY1IiwicGhvbmUiOiJpTTlLeUVBWDZJR1YxeTlOaHBzUTVBPT0iLCJ1c2VyX3JlZ2lzdHJhdGlvbl9kdCI6MTcxMTM4MTE3OCwidmVyc2lvbiI6Mn0.CJpsKzrgQ9JIr789X6tcnJrs4cxEVSAtOZQynRgR6HRodnxLLj3xxwnbTa6e5enOPtWywbLday5AaBdpCpULB3oJ6u5Md5ahB-qaANnAReNkRcmUY3206HmyN4JMh42WFX-JJeij4chyM50AN3c-oDrX2Nb0QBPS75ga7Y7kWZ3pUQcj5eVQ0lm3VBUVdKVWiSO53CDYYJYcQxsc9odeb7XvCcyHTfE4v4gz0rs5RwwqdMJc-F7an19JlnQTYEQJ5SrFu3HrR_prCYYye6flkBYT-KJv4_m5JiDAHzz2KMwZHrHUWLws3acyvNxNygHQXe9Q_yx5hx4qddp6h3WJUw",
        "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3NjQwOTIyNTksInVzZXIiOiIzMDM2NTI2MzUiLCJzaGFyZF9rZXkiOiIyOSIsImNsaWVudF9pZCI6IndiIiwic2Vzc2lvbl9pZCI6IjJiZTk3YWRhOWI3NDQ0YTViYmRiODFkOTM4ZTJkNTZmIiwidmFsaWRhdGlvbl9rZXkiOiJmOGU5YjU4MjU0OTExODA0ZWRkZWU2MDhmMDMzM2FjYzc4MjNkYjZiZjBiMDI3MDQ3MWFlMDQ3YThmZjNiMjI3IiwicGhvbmUiOiJWSDh5UERZejJ0REFRYmFBc1RmWHNnPT0iLCJ1c2VyX3JlZ2lzdHJhdGlvbl9kdCI6MTc2MDg1NTI3NCwidmVyc2lvbiI6Mn0.o8SYNezWoLRmcT5GpuV6W7alIFgLizQ93UW0v2MGeOBjs01I7k4I4Z4lx83cqinQxivRZBMw2m0I1LgTu0Z3CrxeNxncNqy2s0rzrcjozi_pUHNipbWM0G1kY6gv0qlEAhz8kVCbM1En_bXyvGRIrqc6Gvb-OTZQdBd4GmXc_Aca7UVsxFFQUHqcp1cWtyXW12V_KlwiCYDfGwwWgt0E8FyQB4_Oc6buixsuFG_tNt8OPW1G7_Y_-4KGDNeqvLRfn6pZEwT-JPaarMim-lA_Q4TWF2jpua7d1Z9W8vMb-Z0cYH76jiTlA15sIO1xf11i63kud0Vq1kQGK_HU2ZX-gQ",
    ),
    3:(
        "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3NjQwOTI0MjksInVzZXIiOiIxMzkyOTgxMjMiLCJzaGFyZF9rZXkiOiIxNCIsImNsaWVudF9pZCI6IndiIiwic2Vzc2lvbl9pZCI6Ijc4MGZjMGU5NTNkZTQ1Y2E5MzBhMTAxYzU5YTZhZGIxIiwidmFsaWRhdGlvbl9rZXkiOiJhMmRmYmUwNzE1MTQ4NTk3Y2Q0MTk0NTg5YmYxZTdmOTJiOWE0ZjU5YmNlODY2ODg3OWIxNmIxNTc3NzE4YTQwIiwicGhvbmUiOiJpQlZiZUp4aHNGV1AzQzdSeUtBVnVnPT0iLCJ1c2VyX3JlZ2lzdHJhdGlvbl9kdCI6MTcwMzg3MzU1NSwidmVyc2lvbiI6Mn0.OO6GxWgXgG460vi2LxAuIHFE87KcG80PmHUhan5Auh4T8ArZ35Rfp2rG-fPHdEIV1jqd6GLp9t07MeYs68BSNTp1UsNfVQqSylMeqio8oyyTxPRG-hWmyCnkIeLlwVWb2L4fLnhgTBWMN6Hl4IS317qjoUZiwkEtLCP3OSLaTExTBgYM4wqbj9LnHsLrOUaIOH8n5N6AgK0VbwVUFwBMoZ3Dxd1RxvDfnrKgkgWoT8jxZp5Yhtx7NH03jCwwC7ILLVsh0qRLbk179W9zarAA08fTEeXtUQbc_zfSQMBUEl8VP5Vc3EH2F_chjdOZls3eaMS_PuJiOzno6e7T-0Q9SA",
        "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3NjQwOTI1MzksInVzZXIiOiI1MTcyOTUyMyIsInNoYXJkX2tleSI6IjUiLCJjbGllbnRfaWQiOiJ3YiIsInNlc3Npb25faWQiOiIyZjA3OTNjMjU4NGU0NGY1OWU5ZGFkNWM2NWQ4NmFkZiIsInZhbGlkYXRpb25fa2V5IjoiNGZmYWI4MzI5YWI5ZmFlY2NmNjk1MjA1ZDcxNTlkN2IzMzE3MDFkZWZkNGJkYzYxYmRlYmIyYjcxYTllYmYzNSIsInBob25lIjoiNDQ0N2xjWFdvSTRCcDhRM1ZyZ0NYUT09IiwidXNlcl9yZWdpc3RyYXRpb25fZHQiOjE2OTUwNDY3NzAsInZlcnNpb24iOjJ9.kNDq1_tE3f3MsJJ0S4Mf7NEXHtf_IgaOYjU5t7WBEQKy_upBnM2ACIpogkMkyapjYJsAen6JkgknaLYX8Q9fDQ8RpUicqlzVeVqDz7tDv60-nlmxJkL2FJ0JPCjWxIcYzNM43bculIFK39H1_gMxnF6eK4dlFEA4kgDOBJwmAaA3tWIav0sMR3nOrlN4zYbSIvNuON2pmqe594qJYKP3_9CKmnhPWzCBqwnSOYXNZTLmILq9GsiPWyrO2cUPltxhCbNqF-9DypFVvYIX9tF5Ek6ccNSbi7sQkbT2jwFyC3sskv7cYQUpIbX3COoMvbX6pjnhYKMsc2w3zfsKqWZ6-w",
    ),
    4:(
        "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3NjM5NzY1NDIsInVzZXIiOiIxMTI0NjIxNjAiLCJzaGFyZF9rZXkiOiIxNyIsImNsaWVudF9pZCI6IndiIiwic2Vzc2lvbl9pZCI6IjRjZmNhN2ExMjEwNjQ3MzRhYjNiZTU2ZmQyMmNkMWViIiwidmFsaWRhdGlvbl9rZXkiOiI1OGNmNDQyNTA4MTNkM2ZmZDhkNmI0YzI4NmZmNGQyMTg5ZjlkMTY3NjBmNWZiYmJlN2Y3ZDY3YjY1MDNmNjlkIiwicGhvbmUiOiJ1NXlqZThTN29FelpONVE0dFNiVmt3PT0iLCJ1c2VyX3JlZ2lzdHJhdGlvbl9kdCI6MTY4MzgwNTc4NCwidmVyc2lvbiI6Mn0.o59a3og2bLLfRjm4ZA-q1Bu8JnDkWDlqEsRhM85an1DMX7luzzkhyPqF4CAiLp0MmAGcihfhMeldIvnvl_1DfBjE6CYoSxy1_qfHvt9KNHxRIqq5NVAmbuJLSUU-qND-MY1vjP5B8kFxtu177J-DzvjwRl3Xf0jRA_L0NrUJtGcySiFEsCctdTduLp2OvH1VJIqiboxQ82e8_CEIT6UzJZ7-Ua3yjz7wsW2S55aQbaHG783eRMPW2go5DFujUh7ysw0-BvVl-4q8nCFOvjsU-sGKb1RwMY1YgVb-VJYUosft0C8BjVhwyLN_JGRienLRDHtub7-_VRNdJ55RVTiskA",
        "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3NjQxMDY2NzMsInVzZXIiOiIzODgwMTQ0MCIsInNoYXJkX2tleSI6IjIxIiwiY2xpZW50X2lkIjoid2IiLCJzZXNzaW9uX2lkIjoiMDg5YTJlMTI3MGRlNDE0ZDlmYWE4MjNkNTU5ZjM4YTQiLCJ2YWxpZGF0aW9uX2tleSI6IjExMzIyZGM4Mzk4YTQ2ZDRkNjFmMDQ2OGNlOTY4ZThjODVhNWJhNjNhOTI4MGE1ZDRiMGMwODcwOWI2MmQ2YTciLCJwaG9uZSI6IkpBTlhXT3hOSDlHdzFtZVZFa3gwR0E9PSIsInVzZXJfcmVnaXN0cmF0aW9uX2R0IjoxNjcwNzExNDA2LCJ2ZXJzaW9uIjoyfQ.a-kq_UQ4wikeUTeq0nlzxMx3lfpWEpiugJfGCGZ-x82zutXKnbuszk4bQ-q2YPVoAakm_OTwViOsyLoabZP9jnc4iEC51nak87pf5oJTTDngzMs5f3HkzXJXyHBRsytRQcy4TeyfaGrU8F75BsavaxfxR9DO4jnpihan-17kYqXQg_J89jR9kr1cRPROGHyyBWYWzQBf2UQaGA4cqpv4thfg4i294Zlt4AZSET3G5P1rJ9uW3aML5k_PNv3AB5oU6MUiK16CLLeAXm11W0vCPs2352-mcjqhpEueSODpTPxpEruFIp9x1J2MygpdiQwUdGLf6dbzAeAnAP3iMU_DzA",
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
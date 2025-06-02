from fastapi import APIRouter
from fastapi.responses import JSONResponse, StreamingResponse
from datetime import datetime, date as date_type
from urllib.parse import quote
from server.funcs.get_trends_report import get_report_download_bytes, MONTH_NAMES
from server.funcs.get_wb_id_external_report import get_external_report_download_bytes

router = APIRouter()


@router.get("/trends")
async def get_advert_download(date_: date_type):
    """
    Выгрузка трендовых запросов
    """
    try:
        excel_stream = await get_report_download_bytes(date_=date_)
        today = datetime.now()
        return StreamingResponse(
            excel_stream,
            media_type="application/vnd.ms-excel",
            headers={
                "Content-Disposition": f"attachment; filename="
                f"{quote(f'Топ 500 трендовых запросов в {MONTH_NAMES[today.month]} {today.year}г', encoding='utf-8')}.xlsx"
            },
        )
    except Exception as e:
        print(f"Ошибка: {e}")
        return JSONResponse(status_code=500, content="Ошибка")


@router.get("/wb_id_trends")
async def get_advert_download(date_: date_type):
    """
    Выгрузка топ 50 товаров
    """
    try:
        excel_stream = await get_external_report_download_bytes()
        today = datetime.now()
        return StreamingResponse(
            excel_stream,
            media_type="application/vnd.ms-excel",
            headers={
                "Content-Disposition": f"attachment; filename="
                f"{quote(f'Топ 50 товаров со внешней рекламой в {MONTH_NAMES[today.month]} {today.year}г', encoding='utf-8')}.xlsx"
            },
        )
    except Exception as e:
        print(f"Ошибка: {e}")
        return JSONResponse(status_code=500, content="Ошибка")

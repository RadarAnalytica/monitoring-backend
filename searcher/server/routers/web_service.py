from datetime import datetime, date

from fastapi import APIRouter
from fastapi.params import Query, Depends
from starlette.responses import JSONResponse

from settings import logger
from server.auth_token.check_token import check_jwt_token
from server.auth_token.token_scheme import oauth2_scheme
from server.funcs.web_service import get_product_request_data

web_service_router = APIRouter()


@web_service_router.get("/wb_id_analysis")
async def get_product_queries_v2(
    product_id: int = Query(),
    date_from: date = Query(),
    date_to: date = Query(),
    token: str = Depends(oauth2_scheme),
):
    if not check_jwt_token(token):
        return JSONResponse(status_code=403, content="Unauthorized")
    start = datetime.now()
    result = await get_product_request_data(
        product_id=product_id, date_from=date_from, date_to=date_to
    )
    logger.info(
        f"Время выполнения wb_id_analysis {(datetime.now() - start).total_seconds()}s"
    )
    return result

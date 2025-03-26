from datetime import datetime
from typing import Optional

from fastapi import APIRouter
from fastapi.params import Body, Query, Depends
from starlette.responses import JSONResponse

from server.funcs.get_best_similar import get_best_similar_products
from server.funcs.get_preset_data import get_preset_db_data, get_preset_by_id_db_data, get_query_frequency_db
from settings import logger
from server.auth_token.check_token import check_jwt_token
from server.auth_token.token_scheme import oauth2_scheme
from server.funcs.get_keywords_data import get_keywords_payload
from server.funcs.get_product_query_data import get_product_db_data, get_product_db_data_latest

query_router = APIRouter()



@query_router.get("/product_queries_v2")
async def get_product_queries_v2(
    product_id: int = Query(),
    city: int = Query(),
    interval: int = Query(),
    token: str = Depends(oauth2_scheme),
):
    if not check_jwt_token(token):
        return JSONResponse(status_code=403, content="Unauthorized")
    start = datetime.now()
    result = await get_product_db_data(product_id, city, interval)
    logger.info(f"Время выполнения v2 {(datetime.now() - start).total_seconds()}s")
    return result


@query_router.get("/product_bot")
async def get_product_simple(
    product_id: int = Query(),
    city: int = Query(),
    interval: int = Query(),
    token: str = Depends(oauth2_scheme),
):
    if not check_jwt_token(token):
        return JSONResponse(status_code=403, content="Unauthorized")
    start = datetime.now()
    result = await get_product_db_data(product_id, city, interval)
    logger.info(f"Время выполнения product_bot {(datetime.now() - start).total_seconds()}s")
    return result


@query_router.get("/latest")
async def get_product_queries_latest(
    product_id: int = Query(),
    city: Optional[int] = Query(default=None),
    token: str = Depends(oauth2_scheme),
):
    if not check_jwt_token(token):
        return JSONResponse(status_code=403, content="Unauthorized")
    if not city:
        city = -1257786
    start = datetime.now()
    result = await get_product_db_data_latest(product_id, city)
    logger.info(f"Время выполнения latest {(datetime.now() - start).total_seconds()}s")
    return result



@query_router.post("/get_keywords")
async def get_products_keywords(
    products_ids: list[int] = Body(), token: str = Depends(oauth2_scheme)
):
    if not check_jwt_token(token):
        return JSONResponse(status_code=403, content="Unauthorized")
    result = await get_keywords_payload(products=products_ids)
    return result


@query_router.get("/get_similar")
async def get_similar(
    product_id: int = Query(), token: str = Depends(oauth2_scheme)
):
    if not check_jwt_token(token):
        return JSONResponse(status_code=403, content="Unauthorized")
    result = await get_best_similar_products(product_id=product_id)
    return result


@query_router.get("/get_presets")
async def get_presets(
    token: str = Depends(oauth2_scheme)
):
    if not check_jwt_token(token):
        return JSONResponse(status_code=403, content="Unauthorized")
    result = await get_preset_db_data()
    return result


@query_router.get("/get_preset_data/{preset_id}")
async def get_preset_queries(
    preset_id: int,
    token: str = Depends(oauth2_scheme)
):
    if not check_jwt_token(token):
        return JSONResponse(status_code=403, content="Unauthorized")
    result = await get_preset_by_id_db_data(preset_id)
    return result


@query_router.get("/get_query_frequency/{query}")
async def get_preset_queries(
    query: str | int,
    token: str = Depends(oauth2_scheme)
):
    if not check_jwt_token(token):
        return JSONResponse(status_code=403, content="Unauthorized")
    query = str(query)
    result = await get_query_frequency_db(query)
    return result
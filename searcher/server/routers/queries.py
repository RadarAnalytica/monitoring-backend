from datetime import datetime
from typing import Optional, Literal

from fastapi import APIRouter, Path, Body, Query
from fastapi.params import Body, Query, Depends
from starlette.responses import JSONResponse

from server.funcs.get_best_similar import get_best_similar_products
from server.funcs.get_preset_data import get_preset_db_data, get_preset_by_id_db_data, get_query_frequency_db, \
    get_query_frequency_all_time_db, get_preset_by_query_all_time_db_data, get_single_preset_db_data
from settings import logger
from server.auth_token.check_token import check_jwt_token
from server.auth_token.token_scheme import oauth2_scheme
from server.funcs.get_keywords_data import get_keywords_payload
from server.funcs.get_product_query_data import get_product_db_data, get_product_db_data_latest, get_ex_ad, \
    get_ex_ad_query, get_ex_ad_page, get_product_db_data_competitors, get_product_db_data_web_service

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


@query_router.get("/request_monitor_web_service")
async def request_monitor_web_service(
    product_id: int = Query(),
    city: int = Query(),
    interval: int = Query(),
    page: Optional[int] = Query(default=None, ge=1, le=1000),
    limit: Optional[int] = Query(default=None, ge=1, le=100),
    sorting: Optional[Literal["asc", "desc"]] = Query(default="desc"),
    token: str = Depends(oauth2_scheme),
):
    if not check_jwt_token(token):
        return JSONResponse(status_code=403, content="Unauthorized")
    if not limit:
        limit = 25
    if not page:
        page = 1
    if limit not in [1, 5, 25, 50, 100]:
        return JSONResponse(status_code=422, content="Limit must be in [1, 5, 25, 50, 100]")
    asc = sorting == "asc"
    start = datetime.now()
    result = await get_product_db_data_web_service(product_id, city, interval, limit=limit, page=page, asc=asc)
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


@query_router.get("/competitors")
async def get_product_competitors(
    product_id: int = Query(),
    token: str = Depends(oauth2_scheme),
):
    if not check_jwt_token(token):
        return JSONResponse(status_code=403, content="Unauthorized")
    start = datetime.now()
    result = await get_product_db_data_competitors(product_id)
    logger.info(f"Время выполнения latest {(datetime.now() - start).total_seconds()}s")
    return result


@query_router.get("/external")
async def get_product_queries_external(
    product_id: int = Query(),
    token: str = Depends(oauth2_scheme),
):
    if not check_jwt_token(token):
        return JSONResponse(status_code=403, content="Unauthorized")
    start = datetime.now()
    result = await get_ex_ad(product_id)
    logger.info(f"Время выполнения external {(datetime.now() - start).total_seconds()}s")
    return result

@query_router.get("/external/page")
async def get_page_external(
    products: str = Query(),
    token: str = Depends(oauth2_scheme),
):
    if not check_jwt_token(token):
        return JSONResponse(status_code=403, content="Unauthorized")
    start = datetime.now()
    p_ids_str = products.split(",")
    p_ids = []
    for p_id_str in p_ids_str:
        try:
            int(p_id_str)
            p_ids.append(p_id_str)
        except (ValueError, TypeError):
            continue
    result = dict()
    if p_ids:
        result = await get_ex_ad_page(p_ids)
    logger.info(f"Время выполнения external page {(datetime.now() - start).total_seconds()}s")
    return result


@query_router.get("/external/query")
async def get_product_queries_external_query(
    products: str = Query(),
    token: str = Depends(oauth2_scheme),
):
    if not check_jwt_token(token):
        return JSONResponse(status_code=403, content="Unauthorized")
    start = datetime.now()
    p_ids_str = products.split(",")
    p_ids = []
    for p_id_str in p_ids_str:
        try:
            int(p_id_str)
            p_ids.append(p_id_str)
        except (ValueError, TypeError):
            continue
    result = 0
    if p_ids:
        result = await get_ex_ad_query(p_ids)
    logger.info(f"Время выполнения /external/query {(datetime.now() - start).total_seconds()}s")
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


@query_router.get("/get_preset")
async def get_preset(
    query: str,
    token: str = Depends(oauth2_scheme)
):
    if not check_jwt_token(token):
        return JSONResponse(status_code=403, content="Unauthorized")
    result = await get_single_preset_db_data(query=query)
    return result


@query_router.get("/get_preset_data/day")
async def get_preset_queries(
    query: Optional[str] = Query(default=None),
    preset: Optional[str] = Query(default=None),
    page: Optional[int] = Query(default=1),
    token: str = Depends(oauth2_scheme)
):
    if not check_jwt_token(token):
        return JSONResponse(status_code=403, content="Unauthorized")
    if preset:
        try:
            preset = int(preset)
        except ValueError:
            preset = None
    if query:
        query = query.strip().lower()
    start = datetime.now()
    result = await get_preset_by_id_db_data(query=query, preset_id=preset, page=page)
    logger.info(f"Время выполнения /get_preset_data/day {(datetime.now() - start).total_seconds()}s")
    return result


@query_router.get("/get_preset_data/month")
async def get_preset_queries(
    query: Optional[str] = Query(default=None),
    preset: Optional[str] = Query(default=None),
    page: Optional[int] = Query(default=1),
    token: str = Depends(oauth2_scheme)
):
    if not check_jwt_token(token):
        return JSONResponse(status_code=403, content="Unauthorized")
    if preset:
        try:
            preset = int(preset)
        except ValueError:
            preset = None
    if query:
        query = query.strip().lower()
    start = datetime.now()

    result = await get_preset_by_query_all_time_db_data(query=query, preset_id=preset, page=page)
    logger.info(f"Время выполнения /get_preset_data/month {(datetime.now() - start).total_seconds()}s")

    return result


@query_router.get("/get_query_frequency/day/{query}")
async def get_preset_queries(
    query: str | int,
    token: str = Depends(oauth2_scheme)
):
    if not check_jwt_token(token):
        return JSONResponse(status_code=403, content="Unauthorized")
    query = str(query)
    query = query.strip().lower()
    result = await get_query_frequency_db(query)
    return result


@query_router.get("/get_query_frequency/month/{query}")
async def get_preset_queries(
    query: str | int,
    token: str = Depends(oauth2_scheme)
):
    if not check_jwt_token(token):
        return JSONResponse(status_code=403, content="Unauthorized")
    query = str(query)
    query = query.strip().lower()
    result = await get_query_frequency_all_time_db(query)
    return result
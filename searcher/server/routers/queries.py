from fastapi import APIRouter
from fastapi.params import Body, Query

from server.funcs.get_keywords_data import get_keywords_payload
from server.funcs.get_product_query_data import get_product_query_payload


query_router = APIRouter()


@query_router.get("/product_queries")
async def get_product_queries(product_id: int = Query(), city: int = Query(), interval: int = Query()):
    result = await get_product_query_payload(product_id=product_id, interval=interval, city=city)
    return result


@query_router.post("/get_keywords")
async def get_products_keywords(products_ids: list[int] = Body()):
    result = await get_keywords_payload(products=products_ids)
    return result

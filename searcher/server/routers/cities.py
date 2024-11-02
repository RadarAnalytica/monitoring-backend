from fastapi import APIRouter
from fastapi.params import Body, Depends
from fastapi.responses import JSONResponse

from clickhouse_db.get_async_connection import get_async_connection
from server.auth_token.check_token import check_jwt_token
from settings import logger

from server.auth_token.token_scheme import oauth2_scheme


city_router = APIRouter()


@city_router.post("/add_cities")
async def add_cities(
    cities: dict[str, int] = Body(), token: str = Depends(oauth2_scheme)
):
    if not check_jwt_token(token):
        return JSONResponse(status_code=403, content="Unauthorized")
    try:
        cities_data = [(key, val) for key, val in cities.items()]
        async with get_async_connection() as client:
            await client.insert("city", cities_data, column_names=["name", "dest"])
    except Exception as e:
        logger.error(f"{e}")
        return {"message": "Error with cities"}
    return JSONResponse(content="ok", status_code=201)

from fastapi import APIRouter
from fastapi.params import Body
from fastapi.responses import JSONResponse

from clickhouse_db.get_async_connection import get_async_connection
from settings import logger


city_router = APIRouter()


@city_router.post("/add_cities")
async def add_cities(cities: dict[str, int] = Body()):
    try:
        cities_data = [(key, val) for key, val in cities.items()]
        async with get_async_connection() as client:
            await client.insert("city", cities_data, column_names=["name", "dest"])
    except Exception as e:
        logger.error(f"{e}")
        return {"message": "Error with cities"}
    return JSONResponse(
        content="ok",
        status_code=201
    )
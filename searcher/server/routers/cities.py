from datetime import datetime

from fastapi import APIRouter
from fastapi.params import Body
from fastapi.responses import JSONResponse

from clickhouse_db.get_async_connection import get_async_connection
from settings import logger


city_router = APIRouter()


@city_router.post("/add_cities")
async def add_cities(cities: dict[str, int] = Body()):
    try:
        now = datetime.now()
        cities_data = [(i, keyval[0], keyval[1], now) for i, keyval in enumerate(cities.items(), 1)]
        async with get_async_connection() as client:
            await client.insert("city", cities_data, column_names=["id", "name", "dest", "updated"])
    except Exception as e:
        logger.error(f"{e}")
        return {"message": "Error with cities"}
    return JSONResponse(
        content="ok",
        status_code=201
    )
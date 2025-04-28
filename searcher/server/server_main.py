from fastapi import FastAPI
from server.routers.load_csv import csv_router
from server.routers.cities import city_router
from server.routers.queries import query_router
from server.routers.web_service import web_service_router
from server.routers.admin import router as admin_router

app = FastAPI()


app.include_router(query_router, prefix="/api/queries", tags=["queries"])
app.include_router(web_service_router, prefix="/api/web_service", tags=["web_service"])
app.include_router(csv_router, prefix="/api/csv", tags=["csv"])
app.include_router(city_router, prefix="/api/cities", tags=["cities"])
app.include_router(admin_router, prefix="/api/admin", tags=["admin"])


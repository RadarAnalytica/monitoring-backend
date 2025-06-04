from fastapi import APIRouter, File, UploadFile, BackgroundTasks
from fastapi.params import Depends
from fastapi.responses import JSONResponse

from server.auth_token.check_token import check_jwt_token
from server.auth_token.token_scheme import oauth2_scheme
from server.funcs.prepare_csv_contents import prepare_csv_contents, prepare_update_month_csv_contents
from server.funcs.upload_requests_data import upload_requests_csv_bg, recount_requests_csv_bg
import pandas as pd
from settings import logger

csv_router = APIRouter()


@csv_router.post("/upload_csv")
async def upload_csv(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(media_type="text/csv"),
    token: str = Depends(oauth2_scheme),
):
    logger.info("Got CSV load request")
    if not check_jwt_token(token):
        return JSONResponse(status_code=403, content="Unauthorized")
    try:
        contents = [
            tuple(row) for row in pd.read_csv(file.file, encoding="utf-8-sig", header=None).itertuples(index=False)
        ]
        contents.reverse()
        try:
            requests_data, error_rows = await prepare_csv_contents(
                contents, filename=file.filename
            )
        except:
            return {"message": "Error with file name, must be {YYYY-MM-DD}.csv"}
        logger.info("Loading to background")
        background_tasks.add_task(upload_requests_csv_bg, requests_data)
    except Exception as e:
        logger.error(f"{e}")
        return {"message": "There was an error uploading the file"}
    return JSONResponse(
        content={"message": "CSV uploaded to background.", "error_rows": error_rows},
        status_code=201,
    )


@csv_router.post("/upload_csv_correction")
async def upload_csv_correction(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(media_type="text/csv"),
    token: str = Depends(oauth2_scheme),
):
    logger.info("Got update freq CSV load request")
    if not check_jwt_token(token):
        return JSONResponse(status_code=403, content="Unauthorized")
    try:
        contents = [
            tuple(row) for row in pd.read_csv(file.file, encoding="utf-8-sig", header=None).itertuples(index=False)
        ]
        try:
            requests_data, error_rows, new_requests = await prepare_update_month_csv_contents(
                contents, filename=file.filename
            )
        except:
            return {"message": "Error with file name, must be {YYYY-MM-DD}.csv"}
        logger.info("Loading to background")
        background_tasks.add_task(recount_requests_csv_bg, requests_data, new_requests)
    except Exception as e:
        logger.error(f"{e}")
        return {"message": "There was an error uploading the file"}
    return JSONResponse(
        content={"message": "CSV uploaded to background.", "error_rows": error_rows},
        status_code=201,
    )

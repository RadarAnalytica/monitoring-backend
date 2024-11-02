from fastapi import APIRouter, File, UploadFile, BackgroundTasks
from fastapi.params import Depends
from fastapi.responses import JSONResponse

from server.auth_token.check_token import check_jwt_token
from server.auth_token.token_scheme import oauth2_scheme
from server.funcs.prepare_csv_contents import prepare_csv_contents
from server.funcs.upload_requests_data import upload_requests_csv_bg
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
        contents = [row.decode() for row in file.file.readlines()]
        requests_data, error_rows = await prepare_csv_contents(contents)
        background_tasks.add_task(upload_requests_csv_bg, requests_data)
    except Exception as e:
        logger.error(f"{e}")
        return {"message": "There was an error uploading the file"}
    return JSONResponse(
        content={"message": "CSV uploaded to background.", "error_rows": error_rows},
        status_code=201,
    )

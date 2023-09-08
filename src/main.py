from fastapi import FastAPI, UploadFile, HTTPException, status
from dotenv import load_dotenv
from fastapi.responses import FileResponse

from .utils.utils import save_uploaded_file, generate_filepath, validate_token
from .utils import csv_parser_utils
from .models import RequestData


load_dotenv()

app = FastAPI()


@app.post("/upload-raw-csv/")
async def create_upload_file(file: UploadFile, token: str):
    if not file.filename.endswith(".csv"):
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            detail="Invalid file format. Only .csv files are allowed.",
        )
    if not validate_token(token):
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid token, token is not Unix Path safe, avoid adding special characters.",
        )
    await save_uploaded_file(file, generate_filepath(token))
    return {
        "column_metadata": csv_parser_utils.guess_metrics_and_columns(
            token, file.filename
        )
    }


@app.post("/generate-ingest-files/")
async def generate_ingest_files(token: str, data: RequestData):
    return csv_parser_utils.generate_ingest_files(
        token, data.column_metadata.model_dump()
    )


@app.get("/dimensions/")
async def get_dim_files(token: str):
    return csv_parser_utils.get_dimensions(token)

@app.post('/dimensions/')
async def update_dim_file():
    return "update success"

@app.get("/events/")
async def get_eve_files(token: str):
    return csv_parser_utils.get_events(token)

@app.post("/events/")
async def update_eve_file(token: str):
    return "update success"

@app.get('/downlod-ingest/', response_class=FileResponse)
async def download_ingest_folder(token: str):
    zip_folder_path = csv_parser_utils.download_ingest_folder(token)
    return FileResponse(zip_folder_path, media_type='application/zip', filename='ingest.zip')


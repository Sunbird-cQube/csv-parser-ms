from fastapi import FastAPI, UploadFile, HTTPException
from utils import save_uploaded_file
from utils import generate_filepath
from dotenv import load_dotenv
from utils import csv_parser_utils
from models import RequestData
from fastapi import status


load_dotenv()

app = FastAPI()


@app.post("/upload-raw-csv/")
async def create_upload_file(file: UploadFile, token: str):
    if not file.filename.endswith(".csv"):
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            detail="Invalid file format. Only .csv files are allowed.",
        )

    await save_uploaded_file(file, generate_filepath(token))
    return {"column_metadata": csv_parser_utils.guess_metrics_and_columns(token, file.filename)}


@app.post('/generate-ingest-files')
async def generate_ingest_files(token: str, data: RequestData):
    return csv_parser_utils.generate_ingest_files(token, data.column_metadata.model_dump())

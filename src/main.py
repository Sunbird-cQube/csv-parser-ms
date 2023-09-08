from fastapi import FastAPI, UploadFile, HTTPException
from utils import save_uploaded_file
from utils import generate_filepath
from dotenv import load_dotenv
from utils import csv_parser_utils
from fastapi import status

load_dotenv()

app = FastAPI()


@app.post("/uploadfile/")
async def create_upload_file(file: UploadFile, token: str):
    if not file.filename.endswith(".csv"):
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            detail="Invalid file format. Only .csv files are allowed.",
        )

    await save_uploaded_file(file, generate_filepath(token))
    return csv_parser_utils.format_df_columns(file.filename, token)

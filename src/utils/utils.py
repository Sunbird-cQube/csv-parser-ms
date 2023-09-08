import os
from fastapi import UploadFile
import shutil

async def save_uploaded_file(file: UploadFile, full_filepath: str):
    # Define a directory where you want to save uploaded files
    os.makedirs(full_filepath, exist_ok=True)

    # Create a unique filename for the uploaded file
    file_path = os.path.join(full_filepath, file.filename)
    content = await file.read()

    # Open the file in binary write mode and write the uploaded data
    with open(file_path, "wb") as f:
        f.write(content)


def generate_filepath(token: str):
    base_path = os.getenv("TMP_BASE_PATH")
    filepath = os.path.join(base_path, token)
    return filepath


def create_folder_if_not(path: str):
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path)
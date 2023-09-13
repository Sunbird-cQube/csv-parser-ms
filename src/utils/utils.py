import os, re
from fastapi import UploadFile
import shutil
import tempfile


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


def validate_token(token):
    unsafe_pattern = r'[\/\0:*\?"\'$!]'
    return not re.search(unsafe_pattern, token)


def get_directory_structure(path):
    result = {"name": os.path.basename(path), "type": "folder", "children": []}

    try:
        with os.scandir(path) as entries:
            for entry in entries:
                if entry.is_dir():
                    result["children"].append(get_directory_structure(entry.path))
                else:
                    result["children"].append({"name": entry.name, "type": "file"})
    except OSError as e:
        print(f"Error scanning directory {path}: {str(e)}")

    return result

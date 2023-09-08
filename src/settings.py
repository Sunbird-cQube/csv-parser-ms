import os
from dotenv import load_dotenv

load_dotenv()

DEFAULT_PROGRAM_NAME = os.getenv("DEFAULT_PROGRAM_NAME")
TMP_BASE_PATH = os.getenv("TMP_BASE_PATH")

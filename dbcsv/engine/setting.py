import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

STORAGE_PATH: Path = Path("dbcsv/engine/storage/")
SECRET_KEY: str = os.getenv("SECRET_KEY")
ALGORITHM: str = os.getenv("ALGORITHM")
ACCESS_TOKEN_EXPIRE_MINUTES: int = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES"))

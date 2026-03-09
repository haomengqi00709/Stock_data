import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).parent
DATA_RAW = BASE_DIR / "data" / "raw"
DATA_PROCESSED = BASE_DIR / "data" / "processed"
DATA_DB = BASE_DIR / "data" / "db"
DB_PATH = DATA_DB / "stocks.duckdb"

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
FRED_API_KEY   = os.getenv("FRED_API_KEY")

HISTORY_YEARS = 10
SLEEP_MIN = 2
SLEEP_MAX = 5
BATCH_SIZE = 10
MAX_RETRIES = 3

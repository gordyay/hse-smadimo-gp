from pathlib import Path
from dotenv import load_dotenv
import os

BASE_DIR = Path(__file__).resolve().parent.parent
LOGS_DIR = BASE_DIR / "logs"
DATASETS_DIR = BASE_DIR / "raw_datasets"

dotenv_path = os.path.join(BASE_DIR, '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

BINANCE_API_KEY = os.environ.get("BINANCE_API_KEY")
DUNE_API_KEY = os.environ.get("DUNE_API_KEY")
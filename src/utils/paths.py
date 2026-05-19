from pathlib import Path
from src.utils.paths import DEFAULT_DATA_FILE, PARQUET_DIR, RAW_DATA_DIR, CONFIG_DIR

PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
PARQUET_DIR = PROJECT_ROOT / "data" / "parquet_out"
CONFIG_DIR = PROJECT_ROOT / "config"
DOCS_DIR = PROJECT_ROOT / "docs"

DEFAULT_DATA_FILE = PARQUET_DIR / str(DEFAULT_DATA_FILE)


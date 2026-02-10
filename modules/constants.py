from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "logtt.duckdb"
CONFIG_PATH = PROJECT_ROOT / "config.json"

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "logtt.duckdb"
CONFIG_PATH = PROJECT_ROOT / "config.json"
LEVEL_COLOR_MAP = {
    "FATAL": "#DC143C",
    "EMERG": "#DC143C",
    "ALERT": "#FF4500",
    "CRIT": "#FF4500",
    "CRITICAL": "#FF4500",
    "ERROR": "#FF6347",
    "ERR": "#FF6347",
    "WARN": "#FFA500",
    "WARNING": "#FFA500",
    "NOTICE": "#4682B4",
    "INFO": "#4CAF50",
    "DEBUG": "#9E9E9E",
    "TRACE": "#BDBDBD",
}

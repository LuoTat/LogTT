import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "logtt.db"


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn
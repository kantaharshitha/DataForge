"""Database connection and helpers for DataForge."""

from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[2]
DB_DIR = ROOT / "db"
DB_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = Path(os.getenv("DATAFORGE_DB", DB_DIR / "dataforge.duckdb"))


@contextmanager
def get_conn():
    conn = duckdb.connect(str(DB_PATH))
    try:
        yield conn
    finally:
        conn.close()

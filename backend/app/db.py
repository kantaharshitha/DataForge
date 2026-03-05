"""Database connection and helpers for DataForge."""

from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[2]
MIGRATIONS_DIR = ROOT / "backend" / "migrations"

IS_VERCEL = os.getenv("VERCEL") == "1"
if IS_VERCEL:
    default_db_path = Path("/tmp/dataforge.duckdb")
else:
    db_dir = ROOT / "db"
    db_dir.mkdir(parents=True, exist_ok=True)
    default_db_path = db_dir / "dataforge.duckdb"

DB_PATH = Path(os.getenv("DATAFORGE_DB", str(default_db_path)))
_SCHEMA_READY = False


def _ensure_schema(conn: duckdb.DuckDBPyConnection) -> None:
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return

    for sql_file in sorted(MIGRATIONS_DIR.glob("*.sql")):
        sql = sql_file.read_text(encoding="utf-8")
        conn.execute(sql)

    _SCHEMA_READY = True


@contextmanager
def get_conn():
    conn = duckdb.connect(str(DB_PATH))
    try:
        _ensure_schema(conn)
        yield conn
    finally:
        conn.close()

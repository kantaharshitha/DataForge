"""Database connection and helpers for DataForge."""

from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[2]
MIGRATIONS_DIR = ROOT / "backend" / "migrations"

IS_VERCEL = os.getenv("VERCEL") == "1"
RUNTIME_MODE = os.getenv("DATAFORGE_RUNTIME_MODE", "").strip().lower()
if not RUNTIME_MODE:
    RUNTIME_MODE = "vercel-ephemeral" if IS_VERCEL else "local"

if RUNTIME_MODE == "persistent":
    default_db_path = ROOT / "db" / "dataforge.duckdb"
elif RUNTIME_MODE == "vercel-ephemeral":
    default_db_path = Path("/tmp/dataforge.duckdb")
else:
    default_db_path = ROOT / "db" / "dataforge.duckdb"

DB_PATH = Path(os.getenv("DATAFORGE_DB", str(default_db_path)))
DB_PATH.parent.mkdir(parents=True, exist_ok=True)
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


def get_runtime_info() -> dict:
    return {
        "runtime_mode": RUNTIME_MODE,
        "is_vercel": IS_VERCEL,
        "db_path": str(DB_PATH),
        "db_exists": DB_PATH.exists(),
    }

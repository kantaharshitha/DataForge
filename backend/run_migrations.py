"""Simple migration runner for DuckDB SQL files."""

from __future__ import annotations

from pathlib import Path

from app.db import get_conn


MIGRATIONS_DIR = Path(__file__).resolve().parent / "migrations"


def run_migrations() -> None:
    with get_conn() as conn:
        for sql_file in sorted(MIGRATIONS_DIR.glob("*.sql")):
            sql = sql_file.read_text(encoding="utf-8")
            conn.execute(sql)
            print(f"Applied migration: {sql_file.name}")


if __name__ == "__main__":
    run_migrations()

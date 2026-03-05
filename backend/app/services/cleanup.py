"""Operational cleanup utilities for retention and disk hygiene."""

from __future__ import annotations

from pathlib import Path

from app.db import get_conn
from app.services.ingestion import RAW_DIR


def _ids_to_delete(conn, table: str, id_col: str, order_col: str, keep_last: int) -> list[str]:
    if keep_last < 0:
        keep_last = 0

    rows = conn.execute(
        f"SELECT {id_col} FROM {table} ORDER BY {order_col} DESC"
    ).fetchall()
    ids = [row[0] for row in rows]
    return ids[keep_last:]


def run_cleanup(keep_last_runs: int = 20, keep_raw_files: int = 200) -> dict:
    deleted = {
        "validation_runs": 0,
        "validation_results": 0,
        "validation_exceptions": 0,
        "kpi_runs": 0,
        "drift_runs": 0,
        "drift_events": 0,
        "lineage_runs": 0,
        "lineage_edges": 0,
        "raw_files": 0,
    }

    with get_conn() as conn:
        # Validation retention.
        val_ids = _ids_to_delete(conn, "validation_runs", "validation_run_id", "started_at", keep_last_runs)
        for run_id in val_ids:
            deleted["validation_results"] += conn.execute(
                "DELETE FROM validation_results WHERE validation_run_id = ?",
                [run_id],
            ).rowcount
            deleted["validation_exceptions"] += conn.execute(
                "DELETE FROM validation_exceptions WHERE validation_run_id = ?",
                [run_id],
            ).rowcount
            deleted["validation_runs"] += conn.execute(
                "DELETE FROM validation_runs WHERE validation_run_id = ?",
                [run_id],
            ).rowcount

        # KPI retention.
        kpi_ids = _ids_to_delete(conn, "kpi_run_log", "kpi_run_id", "generated_at", keep_last_runs)
        for run_id in kpi_ids:
            deleted["kpi_runs"] += conn.execute(
                "DELETE FROM kpi_run_log WHERE kpi_run_id = ?",
                [run_id],
            ).rowcount

        # Drift retention.
        drift_ids = _ids_to_delete(conn, "schema_drift_runs", "drift_run_id", "run_at", keep_last_runs)
        for run_id in drift_ids:
            deleted["drift_events"] += conn.execute(
                "DELETE FROM schema_drift_events WHERE drift_run_id = ?",
                [run_id],
            ).rowcount
            deleted["drift_runs"] += conn.execute(
                "DELETE FROM schema_drift_runs WHERE drift_run_id = ?",
                [run_id],
            ).rowcount

        # Lineage retention.
        lineage_ids = _ids_to_delete(conn, "lineage_runs", "lineage_run_id", "run_at", keep_last_runs)
        for run_id in lineage_ids:
            deleted["lineage_edges"] += conn.execute(
                "DELETE FROM lineage_edges WHERE lineage_run_id = ?",
                [run_id],
            ).rowcount
            deleted["lineage_runs"] += conn.execute(
                "DELETE FROM lineage_runs WHERE lineage_run_id = ?",
                [run_id],
            ).rowcount

    # Raw file retention.
    raw_dir = Path(RAW_DIR)
    if raw_dir.exists() and raw_dir.is_dir():
        files = sorted(
            [p for p in raw_dir.iterdir() if p.is_file()],
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        stale = files[max(0, keep_raw_files) :]
        for path in stale:
            try:
                path.unlink(missing_ok=True)
                deleted["raw_files"] += 1
            except Exception:
                continue

    return {
        "keep_last_runs": keep_last_runs,
        "keep_raw_files": keep_raw_files,
        "deleted": deleted,
    }

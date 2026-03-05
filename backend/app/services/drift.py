"""Schema drift detection service for Phase 5."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

from app.db import get_conn


SEVERITY_BY_CHANGE = {
    "column_added": "LOW",
    "column_removed": "HIGH",
    "type_changed": "MEDIUM",
    "key_candidates_changed": "HIGH",
}


def _normalize_type(type_name: str) -> str:
    raw = str(type_name).strip().lower()
    if any(t in raw for t in ["int", "bigint", "smallint"]):
        return "integer"
    if any(t in raw for t in ["float", "double", "decimal", "numeric", "real"]):
        return "number"
    if any(t in raw for t in ["date", "time"]):
        return "datetime"
    if "bool" in raw:
        return "boolean"
    return "text"


def diff_schema_versions(
    previous_schema: dict[str, str],
    current_schema: dict[str, str],
    previous_key_candidates: list[str],
    current_key_candidates: list[str],
) -> list[dict]:
    events: list[dict] = []

    prev_cols = set(previous_schema.keys())
    curr_cols = set(current_schema.keys())

    for col in sorted(curr_cols - prev_cols):
        events.append(
            {
                "change_type": "column_added",
                "column_name": col,
                "old_value": None,
                "new_value": str(current_schema[col]),
                "severity": SEVERITY_BY_CHANGE["column_added"],
                "details": {"reason": "column present in current schema only"},
            }
        )

    for col in sorted(prev_cols - curr_cols):
        events.append(
            {
                "change_type": "column_removed",
                "column_name": col,
                "old_value": str(previous_schema[col]),
                "new_value": None,
                "severity": SEVERITY_BY_CHANGE["column_removed"],
                "details": {"reason": "column missing from current schema"},
            }
        )

    for col in sorted(prev_cols & curr_cols):
        old_type = _normalize_type(previous_schema[col])
        new_type = _normalize_type(current_schema[col])
        if old_type != new_type:
            events.append(
                {
                    "change_type": "type_changed",
                    "column_name": col,
                    "old_value": old_type,
                    "new_value": new_type,
                    "severity": SEVERITY_BY_CHANGE["type_changed"],
                    "details": {
                        "previous_raw_type": str(previous_schema[col]),
                        "current_raw_type": str(current_schema[col]),
                    },
                }
            )

    previous_keys = sorted(set(previous_key_candidates))
    current_keys = sorted(set(current_key_candidates))
    if previous_keys != current_keys:
        events.append(
            {
                "change_type": "key_candidates_changed",
                "column_name": None,
                "old_value": ",".join(previous_keys) if previous_keys else None,
                "new_value": ",".join(current_keys) if current_keys else None,
                "severity": SEVERITY_BY_CHANGE["key_candidates_changed"],
                "details": {
                    "previous_key_candidates": previous_keys,
                    "current_key_candidates": current_keys,
                },
            }
        )

    return events


def _latest_two_versions(conn, dataset_name: str):
    rows = conn.execute(
        """
        SELECT version_no, schema_json, key_candidates_json
        FROM schema_versions
        WHERE dataset_name = ?
        ORDER BY version_no DESC
        LIMIT 2
        """,
        [dataset_name],
    ).fetchall()
    return rows


def _persist_drift(conn, dataset_name: str, from_version: int, to_version: int, events: list[dict]) -> dict:
    drift_run_id = str(uuid.uuid4())
    run_at = datetime.now(timezone.utc).isoformat()

    high_count = sum(1 for e in events if e["severity"] == "HIGH")
    medium_count = sum(1 for e in events if e["severity"] == "MEDIUM")
    low_count = sum(1 for e in events if e["severity"] == "LOW")

    status = "CHANGED" if events else "NO_CHANGE"

    conn.execute(
        """
        INSERT INTO schema_drift_runs (
            drift_run_id, dataset_name, from_version, to_version, run_at,
            event_count, high_count, medium_count, low_count, status, summary_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            drift_run_id,
            dataset_name,
            from_version,
            to_version,
            run_at,
            len(events),
            high_count,
            medium_count,
            low_count,
            status,
            json.dumps({"dataset_name": dataset_name, "status": status}),
        ],
    )

    for event in events:
        conn.execute(
            """
            INSERT INTO schema_drift_events (
                event_id, drift_run_id, dataset_name, change_type, column_name,
                old_value, new_value, severity, details_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                str(uuid.uuid4()),
                drift_run_id,
                dataset_name,
                event["change_type"],
                event["column_name"],
                event["old_value"],
                event["new_value"],
                event["severity"],
                json.dumps(event.get("details", {})),
                run_at,
            ],
        )

    return {
        "drift_run_id": drift_run_id,
        "dataset_name": dataset_name,
        "from_version": from_version,
        "to_version": to_version,
        "run_at": run_at,
        "event_count": len(events),
        "high_count": high_count,
        "medium_count": medium_count,
        "low_count": low_count,
        "status": status,
    }


def run_schema_drift_scan(dataset_name: str | None = None) -> dict:
    with get_conn() as conn:
        if dataset_name:
            datasets = [dataset_name]
        else:
            datasets = [
                row[0]
                for row in conn.execute(
                    "SELECT DISTINCT dataset_name FROM schema_versions ORDER BY dataset_name"
                ).fetchall()
            ]

        run_summaries: list[dict] = []

        for ds in datasets:
            versions = _latest_two_versions(conn, ds)
            if len(versions) < 2:
                continue

            current_version, current_schema_json, current_keys_json = versions[0]
            previous_version, previous_schema_json, previous_keys_json = versions[1]

            current_schema = json.loads(current_schema_json)
            previous_schema = json.loads(previous_schema_json)
            current_keys = json.loads(current_keys_json) if current_keys_json else []
            previous_keys = json.loads(previous_keys_json) if previous_keys_json else []

            events = diff_schema_versions(
                previous_schema=previous_schema,
                current_schema=current_schema,
                previous_key_candidates=previous_keys,
                current_key_candidates=current_keys,
            )

            summary = _persist_drift(
                conn,
                dataset_name=ds,
                from_version=int(previous_version),
                to_version=int(current_version),
                events=events,
            )
            run_summaries.append(summary)

    return {
        "run_count": len(run_summaries),
        "total_events": sum(r["event_count"] for r in run_summaries),
        "runs": run_summaries,
    }


def list_drift_runs() -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT drift_run_id, dataset_name, from_version, to_version, run_at,
                   event_count, high_count, medium_count, low_count, status
            FROM schema_drift_runs
            ORDER BY run_at DESC
            """
        ).fetchall()

    return [
        {
            "drift_run_id": row[0],
            "dataset_name": row[1],
            "from_version": row[2],
            "to_version": row[3],
            "run_at": row[4],
            "event_count": row[5],
            "high_count": row[6],
            "medium_count": row[7],
            "low_count": row[8],
            "status": row[9],
        }
        for row in rows
    ]


def get_latest_drift_run() -> dict:
    runs = list_drift_runs()
    if not runs:
        raise ValueError("No drift runs found.")
    return runs[0]


def list_drift_events(dataset_name: str) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT event_id, drift_run_id, dataset_name, change_type, column_name,
                   old_value, new_value, severity, details_json, created_at
            FROM schema_drift_events
            WHERE dataset_name = ?
            ORDER BY created_at DESC
            """,
            [dataset_name],
        ).fetchall()

    return [
        {
            "event_id": row[0],
            "drift_run_id": row[1],
            "dataset_name": row[2],
            "change_type": row[3],
            "column_name": row[4],
            "old_value": row[5],
            "new_value": row[6],
            "severity": row[7],
            "details": json.loads(row[8]) if row[8] else {},
            "created_at": row[9],
        }
        for row in rows
    ]

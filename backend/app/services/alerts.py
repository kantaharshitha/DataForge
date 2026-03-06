"""Alerting hooks for trust drops and high-severity drift events."""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone

from app.db import get_conn


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_ts(value) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value).replace("Z", "+00:00")
    parsed = datetime.fromisoformat(text)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _dataset_key_from_context(context: dict) -> str:
    for key in ("dataset_name", "dataset"):
        value = context.get(key)
        if value:
            return str(value)
    return "__global__"


def _is_duplicate_alert(alert_type: str, context: dict, dedup_minutes: int) -> bool:
    if dedup_minutes <= 0:
        return False

    now = datetime.now(timezone.utc)
    window_seconds = dedup_minutes * 60
    dataset_key = _dataset_key_from_context(context)

    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT context_json, created_at
            FROM alert_events
            WHERE alert_type = ?
            ORDER BY created_at DESC
            LIMIT 200
            """,
            [alert_type],
        ).fetchall()

    for row in rows:
        ctx = json.loads(row[0]) if row[0] else {}
        created_at = _parse_ts(row[1])
        age_seconds = (now - created_at).total_seconds()
        if age_seconds > window_seconds:
            continue
        if _dataset_key_from_context(ctx) == dataset_key:
            return True
    return False


def _deliver_webhook(payload: dict) -> tuple[str, str | None]:
    webhook_url = os.getenv("DATAFORGE_ALERT_WEBHOOK_URL", "").strip()
    if not webhook_url:
        return "SKIPPED", None

    req = urllib.request.Request(
        webhook_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=5):  # nosec B310
            return "DELIVERED", None
    except urllib.error.URLError as exc:
        return "FAILED", str(exc)


def emit_alert(
    *,
    alert_type: str,
    severity: str,
    title: str,
    message: str,
    context: dict | None = None,
) -> dict:
    alert_id = str(uuid.uuid4())
    created_at = _utc_now_iso()
    context_payload = context or {}
    dedup_minutes = int(os.getenv("DATAFORGE_ALERT_DEDUP_MINUTES", "30"))
    outbound_payload = {
        "alert_id": alert_id,
        "alert_type": alert_type,
        "severity": severity,
        "title": title,
        "message": message,
        "context": context_payload,
        "created_at": created_at,
    }
    if _is_duplicate_alert(alert_type, context_payload, dedup_minutes):
        delivery_status, delivery_error = "DEDUPED", "Suppressed duplicate alert within dedup window."
    else:
        delivery_status, delivery_error = _deliver_webhook(outbound_payload)

    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO alert_events (
                alert_id, alert_type, severity, title, message,
                context_json, delivery_status, delivery_error, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                alert_id,
                alert_type,
                severity,
                title,
                message,
                json.dumps(context_payload),
                delivery_status,
                delivery_error,
                created_at,
            ],
        )

    return {
        "alert_id": alert_id,
        "alert_type": alert_type,
        "severity": severity,
        "title": title,
        "message": message,
        "context": context_payload,
        "delivery_status": delivery_status,
        "delivery_error": delivery_error,
        "created_at": created_at,
    }


def list_recent_alerts(limit: int = 50) -> list[dict]:
    safe_limit = max(1, min(500, int(limit)))
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT alert_id, alert_type, severity, title, message, context_json,
                   delivery_status, delivery_error, created_at
            FROM alert_events
            ORDER BY created_at DESC
            LIMIT ?
            """,
            [safe_limit],
        ).fetchall()

    return [
        {
            "alert_id": row[0],
            "alert_type": row[1],
            "severity": row[2],
            "title": row[3],
            "message": row[4],
            "context": json.loads(row[5]) if row[5] else {},
            "delivery_status": row[6],
            "delivery_error": row[7],
            "created_at": row[8],
        }
        for row in rows
    ]

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
    source_alert_id = context.get("source_alert_id")
    if source_alert_id:
        return f"alert:{source_alert_id}"
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
            SELECT e.alert_id, e.alert_type, e.severity, e.title, e.message, e.context_json,
                   e.delivery_status, e.delivery_error, e.created_at,
                   a.ack_id, a.acknowledged_by, a.note, a.acknowledged_at,
                   s.assignment_id, s.assigned_to, s.assigned_by, s.priority, s.due_by, s.assigned_at
            FROM alert_events e
            LEFT JOIN alert_acknowledgements a ON e.alert_id = a.alert_id
            LEFT JOIN alert_assignments s ON e.alert_id = s.alert_id
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
            "is_acknowledged": bool(row[9]),
            "acknowledged_by": row[10],
            "ack_note": row[11],
            "acknowledged_at": row[12],
            "is_assigned": bool(row[13]),
            "assigned_to": row[14],
            "assigned_by": row[15],
            "assignment_priority": row[16],
            "assignment_due_by": row[17],
            "assigned_at": row[18],
        }
        for row in rows
    ]


def acknowledge_alert(alert_id: str, acknowledged_by: str, note: str | None = None) -> dict:
    who = acknowledged_by.strip()
    if not alert_id.strip():
        raise ValueError("alert_id is required.")
    if not who:
        raise ValueError("acknowledged_by is required.")

    ack_id = str(uuid.uuid4())
    acknowledged_at = _utc_now_iso()
    with get_conn() as conn:
        exists = conn.execute(
            "SELECT alert_id FROM alert_events WHERE alert_id = ?",
            [alert_id],
        ).fetchone()
        if not exists:
            raise ValueError("Alert not found.")

        conn.execute("DELETE FROM alert_acknowledgements WHERE alert_id = ?", [alert_id])
        conn.execute(
            """
            INSERT INTO alert_acknowledgements (
                ack_id, alert_id, acknowledged_by, note, acknowledged_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            [ack_id, alert_id, who, (note or None), acknowledged_at],
        )

    return {
        "ack_id": ack_id,
        "alert_id": alert_id,
        "acknowledged_by": who,
        "note": note or None,
        "acknowledged_at": acknowledged_at,
    }


def summarize_alerts(window_hours: int = 24) -> dict:
    safe_hours = max(1, min(168, int(window_hours)))

    with get_conn() as conn:
        total = conn.execute("SELECT COUNT(*) FROM alert_events").fetchone()[0]
        recent = conn.execute(
            """
            SELECT COUNT(*)
            FROM alert_events
            WHERE created_at >= (CURRENT_TIMESTAMP - (? * INTERVAL '1 hour'))
            """,
            [safe_hours],
        ).fetchone()[0]

        severity_rows = conn.execute(
            """
            SELECT severity, COUNT(*) AS c
            FROM alert_events
            GROUP BY severity
            ORDER BY c DESC, severity ASC
            """
        ).fetchall()

        delivery_rows = conn.execute(
            """
            SELECT delivery_status, COUNT(*) AS c
            FROM alert_events
            GROUP BY delivery_status
            ORDER BY c DESC, delivery_status ASC
            """
        ).fetchall()

        type_rows = conn.execute(
            """
            SELECT alert_type, COUNT(*) AS c
            FROM alert_events
            GROUP BY alert_type
            ORDER BY c DESC, alert_type ASC
            """
        ).fetchall()

    return {
        "total_alerts": int(total),
        "alerts_in_window": int(recent),
        "window_hours": safe_hours,
        "by_severity": {row[0]: int(row[1]) for row in severity_rows},
        "by_delivery_status": {row[0]: int(row[1]) for row in delivery_rows},
        "by_alert_type": {row[0]: int(row[1]) for row in type_rows},
    }


def summarize_alert_sla(window_hours: int = 24) -> dict:
    safe_hours = max(1, min(720, int(window_hours)))
    window_days = max(1.0, safe_hours / 24.0)

    with get_conn() as conn:
        open_high = conn.execute(
            """
            SELECT COUNT(*)
            FROM alert_events e
            LEFT JOIN alert_acknowledgements a ON e.alert_id = a.alert_id
            WHERE e.severity = 'HIGH'
              AND a.alert_id IS NULL
            """
        ).fetchone()[0]

        mtta_minutes = conn.execute(
            """
            SELECT AVG(
                EXTRACT(EPOCH FROM (a.acknowledged_at - e.created_at)) / 60.0
            )
            FROM alert_events e
            JOIN alert_acknowledgements a ON e.alert_id = a.alert_id
            WHERE e.created_at >= (CURRENT_TIMESTAMP - (? * INTERVAL '1 hour'))
            """,
            [safe_hours],
        ).fetchone()[0]

        escalations = conn.execute(
            """
            SELECT COUNT(*)
            FROM alert_escalations
            WHERE escalated_at >= (CURRENT_TIMESTAMP - (? * INTERVAL '1 hour'))
            """,
            [safe_hours],
        ).fetchone()[0]

    escalations_per_day = round(float(escalations) / window_days, 2)
    return {
        "window_hours": safe_hours,
        "open_high_alerts": int(open_high),
        "mtta_minutes": round(float(mtta_minutes), 2) if mtta_minutes is not None else None,
        "escalations_in_window": int(escalations),
        "escalations_per_day": escalations_per_day,
    }


def run_alert_sla_breach_check(window_hours: int = 24) -> dict:
    sla = summarize_alert_sla(window_hours=window_hours)
    breaches: list[dict] = []

    max_open_high = int(os.getenv("DATAFORGE_SLA_MAX_OPEN_HIGH", "5"))
    max_mtta = float(os.getenv("DATAFORGE_SLA_MAX_MTTA_MINUTES", "60"))
    max_escalations_per_day = float(os.getenv("DATAFORGE_SLA_MAX_ESCALATIONS_PER_DAY", "3"))

    suppressed: list[str] = []

    def _breach_already_emitted_today(metric: str) -> bool:
        with get_conn() as conn:
            rows = conn.execute(
                """
                SELECT context_json
                FROM alert_events
                WHERE alert_type LIKE 'SLA_BREACH_%'
                  AND created_at >= DATE_TRUNC('day', CURRENT_TIMESTAMP)
                """
            ).fetchall()
        for row in rows:
            ctx = json.loads(row[0]) if row[0] else {}
            if ctx.get("metric") == metric:
                return True
        return False

    if int(sla["open_high_alerts"]) > max_open_high and not _breach_already_emitted_today("open_high_alerts"):
        emitted = emit_alert(
            alert_type="SLA_BREACH_OPEN_HIGH",
            severity="HIGH",
            title="SLA breach: open high alerts",
            message=(
                f"Open high alerts {sla['open_high_alerts']} exceeded threshold {max_open_high} "
                f"in last {sla['window_hours']}h."
            ),
            context={
                "metric": "open_high_alerts",
                "actual": sla["open_high_alerts"],
                "threshold": max_open_high,
                "window_hours": sla["window_hours"],
            },
        )
        breaches.append({"metric": "open_high_alerts", "alert_id": emitted["alert_id"]})
    elif int(sla["open_high_alerts"]) > max_open_high:
        suppressed.append("open_high_alerts")

    if (
        sla["mtta_minutes"] is not None
        and float(sla["mtta_minutes"]) > max_mtta
        and not _breach_already_emitted_today("mtta_minutes")
    ):
        emitted = emit_alert(
            alert_type="SLA_BREACH_MTTA",
            severity="HIGH",
            title="SLA breach: MTTA",
            message=(
                f"MTTA {sla['mtta_minutes']}m exceeded threshold {max_mtta}m "
                f"in last {sla['window_hours']}h."
            ),
            context={
                "metric": "mtta_minutes",
                "actual": sla["mtta_minutes"],
                "threshold": max_mtta,
                "window_hours": sla["window_hours"],
            },
        )
        breaches.append({"metric": "mtta_minutes", "alert_id": emitted["alert_id"]})
    elif sla["mtta_minutes"] is not None and float(sla["mtta_minutes"]) > max_mtta:
        suppressed.append("mtta_minutes")

    if (
        float(sla["escalations_per_day"]) > max_escalations_per_day
        and not _breach_already_emitted_today("escalations_per_day")
    ):
        emitted = emit_alert(
            alert_type="SLA_BREACH_ESCALATIONS_PER_DAY",
            severity="MEDIUM",
            title="SLA breach: escalations per day",
            message=(
                f"Escalations/day {sla['escalations_per_day']} exceeded threshold {max_escalations_per_day} "
                f"in last {sla['window_hours']}h."
            ),
            context={
                "metric": "escalations_per_day",
                "actual": sla["escalations_per_day"],
                "threshold": max_escalations_per_day,
                "window_hours": sla["window_hours"],
            },
        )
        breaches.append({"metric": "escalations_per_day", "alert_id": emitted["alert_id"]})
    elif float(sla["escalations_per_day"]) > max_escalations_per_day:
        suppressed.append("escalations_per_day")

    payload = {
        "window_hours": sla["window_hours"],
        "thresholds": {
            "max_open_high_alerts": max_open_high,
            "max_mtta_minutes": max_mtta,
            "max_escalations_per_day": max_escalations_per_day,
        },
        "sla": sla,
        "breach_count": len(breaches),
        "breaches": breaches,
        "suppressed_count": len(suppressed),
        "suppressed_metrics": suppressed,
    }
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO sla_breach_runs (
                run_id, window_hours, thresholds_json, sla_json,
                breach_count, suppressed_count, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                str(uuid.uuid4()),
                int(payload["window_hours"]),
                json.dumps(payload["thresholds"]),
                json.dumps(payload["sla"]),
                int(payload["breach_count"]),
                int(payload["suppressed_count"]),
                _utc_now_iso(),
            ],
        )
    return payload


def get_alert_sla_history(days: int = 14) -> list[dict]:
    safe_days = max(1, min(90, int(days)))
    history: dict[str, dict] = {}

    def _upsert(day_key: str) -> dict:
        if day_key not in history:
            history[day_key] = {
                "date": day_key,
                "acknowledged_alerts": 0,
                "avg_mtta_minutes": None,
                "escalations": 0,
                "sla_breaches": 0,
            }
        return history[day_key]

    with get_conn() as conn:
        ack_rows = conn.execute(
            """
            SELECT
              CAST(DATE_TRUNC('day', a.acknowledged_at) AS DATE) AS d,
              COUNT(*) AS ack_count,
              AVG(EXTRACT(EPOCH FROM (a.acknowledged_at - e.created_at)) / 60.0) AS avg_mtta
            FROM alert_events e
            JOIN alert_acknowledgements a ON e.alert_id = a.alert_id
            WHERE a.acknowledged_at >= (CURRENT_TIMESTAMP - (? * INTERVAL '1 day'))
            GROUP BY 1
            ORDER BY 1
            """,
            [safe_days],
        ).fetchall()

        esc_rows = conn.execute(
            """
            SELECT
              CAST(DATE_TRUNC('day', escalated_at) AS DATE) AS d,
              COUNT(*) AS esc_count
            FROM alert_escalations
            WHERE escalated_at >= (CURRENT_TIMESTAMP - (? * INTERVAL '1 day'))
            GROUP BY 1
            ORDER BY 1
            """,
            [safe_days],
        ).fetchall()

        breach_rows = conn.execute(
            """
            SELECT
              CAST(DATE_TRUNC('day', created_at) AS DATE) AS d,
              COUNT(*) AS breach_count
            FROM alert_events
            WHERE alert_type LIKE 'SLA_BREACH_%'
              AND created_at >= (CURRENT_TIMESTAMP - (? * INTERVAL '1 day'))
            GROUP BY 1
            ORDER BY 1
            """,
            [safe_days],
        ).fetchall()

    for row in ack_rows:
        rec = _upsert(str(row[0]))
        rec["acknowledged_alerts"] = int(row[1])
        rec["avg_mtta_minutes"] = round(float(row[2]), 2) if row[2] is not None else None

    for row in esc_rows:
        rec = _upsert(str(row[0]))
        rec["escalations"] = int(row[1])

    for row in breach_rows:
        rec = _upsert(str(row[0]))
        rec["sla_breaches"] = int(row[1])

    return [history[key] for key in sorted(history.keys())]


def list_alert_sla_breaches(
    days: int = 14,
    limit: int = 100,
    *,
    metric: str | None = None,
    severity: str | None = None,
) -> dict:
    safe_days = max(1, min(90, int(days)))
    safe_limit = max(1, min(500, int(limit)))
    metric_filter = (metric or "").strip().lower() or None
    severity_filter = (severity or "").strip().upper() or None
    allowed_severity = {"LOW", "MEDIUM", "HIGH", "CRITICAL"}
    if severity_filter and severity_filter not in allowed_severity:
        raise ValueError("severity must be one of LOW, MEDIUM, HIGH, CRITICAL")

    with get_conn() as conn:
        if severity_filter:
            rows = conn.execute(
                """
                SELECT alert_id, alert_type, severity, title, message, context_json, delivery_status, created_at
                FROM alert_events
                WHERE alert_type LIKE 'SLA_BREACH_%'
                  AND created_at >= (CURRENT_TIMESTAMP - (? * INTERVAL '1 day'))
                  AND severity = ?
                ORDER BY created_at DESC
                LIMIT 5000
                """,
                [safe_days, severity_filter],
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT alert_id, alert_type, severity, title, message, context_json, delivery_status, created_at
                FROM alert_events
                WHERE alert_type LIKE 'SLA_BREACH_%'
                  AND created_at >= (CURRENT_TIMESTAMP - (? * INTERVAL '1 day'))
                ORDER BY created_at DESC
                LIMIT 5000
                """,
                [safe_days],
            ).fetchall()

        recent_suppressed = conn.execute(
            """
            SELECT COALESCE(SUM(suppressed_count), 0)
            FROM sla_breach_runs
            WHERE created_at >= (CURRENT_TIMESTAMP - INTERVAL '24 hour')
            """
        ).fetchone()[0]

        last_run = conn.execute(
            """
            SELECT run_id, window_hours, breach_count, suppressed_count, created_at
            FROM sla_breach_runs
            ORDER BY created_at DESC
            LIMIT 1
            """
        ).fetchone()

    events: list[dict] = []
    for row in rows:
        context = json.loads(row[5]) if row[5] else {}
        row_metric = str(context.get("metric", "")).strip().lower()
        if metric_filter and row_metric != metric_filter:
            continue
        events.append(
            {
                "alert_id": row[0],
                "alert_type": row[1],
                "severity": row[2],
                "title": row[3],
                "message": row[4],
                "context": context,
                "delivery_status": row[6],
                "created_at": row[7],
            }
        )
        if len(events) >= safe_limit:
            break

    return {
        "days": safe_days,
        "limit": safe_limit,
        "metric": metric_filter,
        "severity": severity_filter,
        "events": events,
        "suppressed_last_24h": int(recent_suppressed or 0),
        "last_run": {
            "run_id": last_run[0],
            "window_hours": last_run[1],
            "breach_count": last_run[2],
            "suppressed_count": last_run[3],
            "created_at": last_run[4],
        }
        if last_run
        else None,
    }


def assign_alert(
    *,
    alert_id: str,
    assigned_to: str,
    assigned_by: str,
    priority: str = "MEDIUM",
    due_by: str | None = None,
) -> dict:
    if not alert_id.strip():
        raise ValueError("alert_id is required.")
    if not assigned_to.strip():
        raise ValueError("assigned_to is required.")
    if not assigned_by.strip():
        raise ValueError("assigned_by is required.")

    normalized_priority = priority.strip().upper()
    if normalized_priority not in {"LOW", "MEDIUM", "HIGH", "CRITICAL"}:
        raise ValueError("priority must be LOW, MEDIUM, HIGH, or CRITICAL.")

    assignment_id = str(uuid.uuid4())
    assigned_at = _utc_now_iso()
    with get_conn() as conn:
        exists = conn.execute(
            "SELECT alert_id FROM alert_events WHERE alert_id = ?",
            [alert_id],
        ).fetchone()
        if not exists:
            raise ValueError("Alert not found.")

        conn.execute("DELETE FROM alert_assignments WHERE alert_id = ?", [alert_id])
        conn.execute(
            """
            INSERT INTO alert_assignments (
                assignment_id, alert_id, assigned_to, assigned_by, priority, due_by, assigned_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                assignment_id,
                alert_id,
                assigned_to.strip(),
                assigned_by.strip(),
                normalized_priority,
                due_by,
                assigned_at,
            ],
        )

    return {
        "assignment_id": assignment_id,
        "alert_id": alert_id,
        "assigned_to": assigned_to.strip(),
        "assigned_by": assigned_by.strip(),
        "priority": normalized_priority,
        "due_by": due_by,
        "assigned_at": assigned_at,
    }


def run_alert_escalation_scan(older_than_minutes: int = 60, limit: int = 50) -> dict:
    effective_default = int(os.getenv("DATAFORGE_ESCALATION_MINUTES", "60"))
    candidate = effective_default if int(older_than_minutes) == 60 else int(older_than_minutes)
    min_age = max(1, candidate)
    scan_limit = max(1, min(500, int(limit)))
    now = datetime.now(timezone.utc)

    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT e.alert_id, e.alert_type, e.severity, e.message, e.context_json, e.created_at
            FROM alert_events e
            LEFT JOIN alert_acknowledgements a ON e.alert_id = a.alert_id
            LEFT JOIN alert_escalations esc ON e.alert_id = esc.alert_id
            WHERE e.severity = 'HIGH'
              AND a.alert_id IS NULL
              AND esc.alert_id IS NULL
            ORDER BY e.created_at ASC
            LIMIT ?
            """,
            [scan_limit],
        ).fetchall()

    escalated: list[dict] = []
    for row in rows:
        source_alert_id = row[0]
        source_context = json.loads(row[4]) if row[4] else {}
        created_at = _parse_ts(row[5])
        age_minutes = int((now - created_at).total_seconds() // 60)
        if age_minutes < min_age:
            continue

        emitted = emit_alert(
            alert_type="ALERT_ESCALATED",
            severity="HIGH",
            title="Unacknowledged high-severity alert escalated",
            message=f"Alert {source_alert_id} remained unacknowledged for {age_minutes} minutes.",
            context={
                "source_alert_id": source_alert_id,
                "source_alert_type": row[1],
                "source_message": row[3],
                "source_created_at": str(row[5]),
                "source_context": source_context,
                "age_minutes": age_minutes,
            },
        )

        with get_conn() as conn:
            conn.execute(
                """
                INSERT INTO alert_escalations (
                    escalation_id, alert_id, emitted_alert_id, reason, source_age_minutes, escalated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    str(uuid.uuid4()),
                    source_alert_id,
                    emitted["alert_id"],
                    "unacknowledged_high_alert",
                    age_minutes,
                    _utc_now_iso(),
                ],
            )

        escalated.append(
            {
                "source_alert_id": source_alert_id,
                "emitted_alert_id": emitted["alert_id"],
                "age_minutes": age_minutes,
            }
        )

    return {
        "scanned": len(rows),
        "escalated_count": len(escalated),
        "older_than_minutes": min_age,
        "escalated": escalated,
    }

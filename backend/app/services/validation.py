"""Validation engine and trust score service for Phase 3."""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone

from app.db import get_conn
from app.services.alerts import emit_alert

DIMENSION_WEIGHTS = {
    "completeness": 0.30,
    "integrity": 0.30,
    "conformance": 0.20,
    "temporal": 0.10,
    "drift": 0.10,
}

SEVERITY_MULTIPLIER = {
    "CRITICAL": 1.0,
    "HIGH": 0.7,
    "MEDIUM": 0.4,
    "LOW": 0.2,
}

NEGATIVE_CHECK_COLUMNS = {
    "quantity",
    "unit_price",
    "discount_amount",
    "tax_amount",
    "line_total",
    "unit_cost",
    "list_price",
    "on_hand_qty",
    "reserved_qty",
    "reorder_point",
}


class RuleResult(dict):
    pass


def _quote_ident(identifier: str) -> str:
    return f'"{identifier.replace(chr(34), chr(34) * 2)}"'


def _latest_datasets(conn) -> list[tuple[str, str]]:
    rows = conn.execute(
        """
        WITH ranked AS (
            SELECT dataset_id, dataset_name, ingested_at,
                   ROW_NUMBER() OVER (PARTITION BY dataset_name ORDER BY ingested_at DESC) AS rn
            FROM dataset_registry
        )
        SELECT dataset_id, dataset_name
        FROM ranked
        WHERE rn = 1
        ORDER BY dataset_name
        """
    ).fetchall()
    return [(row[0], row[1]) for row in rows]


def _latest_profile(conn, dataset_id: str) -> list[dict]:
    rows = conn.execute(
        """
        SELECT pr.column_name, pr.is_candidate_key
        FROM profiling_results pr
        JOIN profiling_runs run ON run.run_id = pr.run_id
        WHERE run.dataset_id = ?
          AND run.run_at = (
            SELECT MAX(run_at) FROM profiling_runs WHERE dataset_id = ?
          )
        """,
        [dataset_id, dataset_id],
    ).fetchall()
    return [{"column_name": row[0], "is_candidate_key": bool(row[1])} for row in rows]


def _build_rule(
    *,
    validation_run_id: str,
    dimension: str,
    rule_code: str,
    dataset_name: str,
    severity: str,
    base_weight: float,
    evaluated_records: int,
    failed_records: int,
    message: str,
    sample_rows: list[str] | None = None,
) -> RuleResult:
    evaluated = max(0, int(evaluated_records))
    failed = max(0, int(failed_records))
    rate = (failed / evaluated) if evaluated else 0.0
    penalty = round(base_weight * SEVERITY_MULTIPLIER[severity] * rate, 6)

    return RuleResult(
        result_id=str(uuid.uuid4()),
        validation_run_id=validation_run_id,
        dimension=dimension,
        rule_code=rule_code,
        dataset_name=dataset_name,
        severity=severity,
        base_weight=base_weight,
        evaluated_records=evaluated,
        failed_records=failed,
        failure_rate=round(rate, 6),
        penalty_points=penalty,
        message=message,
        sample_rows_json=json.dumps(sample_rows or []),
    )


def _completeness_rules(conn, validation_run_id: str, dataset_id: str, dataset_name: str) -> list[RuleResult]:
    rows = conn.execute(
        "SELECT row_count FROM dataset_registry WHERE dataset_id = ?",
        [dataset_id],
    ).fetchone()
    row_count = int(rows[0]) if rows else 0
    table_name = _quote_ident(f"stg_{dataset_name}")

    results: list[RuleResult] = []
    profile_cols = _latest_profile(conn, dataset_id)
    key_columns = [c["column_name"] for c in profile_cols if c["is_candidate_key"]]

    for col in key_columns:
        column = _quote_ident(col)
        failed = conn.execute(
            f"SELECT COUNT(*) FROM {table_name} WHERE {column} IS NULL"
        ).fetchone()[0]

        sample_rows = [
            str(r[0])
            for r in conn.execute(
                f"SELECT ROW_NUMBER() OVER () as rn FROM {table_name} WHERE {column} IS NULL LIMIT 5"
            ).fetchall()
        ]

        results.append(
            _build_rule(
                validation_run_id=validation_run_id,
                dimension="completeness",
                rule_code=f"required_non_null:{col}",
                dataset_name=dataset_name,
                severity="CRITICAL",
                base_weight=1.0,
                evaluated_records=row_count,
                failed_records=int(failed),
                message=f"Column '{col}' should not contain nulls.",
                sample_rows=sample_rows,
            )
        )

    return results


def _integrity_rules(conn, validation_run_id: str) -> list[RuleResult]:
    rows = conn.execute(
        """
        SELECT child_dataset_name, child_column, parent_dataset_name, parent_column
        FROM relationship_candidates
        WHERE status = 'ACCEPTED'
        """
    ).fetchall()

    results: list[RuleResult] = []

    for child_dataset, child_column, parent_dataset, parent_column in rows:
        child_table = _quote_ident(f"stg_{child_dataset}")
        parent_table = _quote_ident(f"stg_{parent_dataset}")
        child_col = _quote_ident(child_column)
        parent_col = _quote_ident(parent_column)

        evaluated = conn.execute(
            f"SELECT COUNT(*) FROM {child_table} WHERE {child_col} IS NOT NULL"
        ).fetchone()[0]

        failed = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM {child_table} c
            LEFT JOIN {parent_table} p
              ON c.{child_col} = p.{parent_col}
            WHERE c.{child_col} IS NOT NULL
              AND p.{parent_col} IS NULL
            """
        ).fetchone()[0]

        sample_rows = [
            str(r[0])
            for r in conn.execute(
                f"""
                SELECT c.{child_col}::VARCHAR
                FROM {child_table} c
                LEFT JOIN {parent_table} p
                  ON c.{child_col} = p.{parent_col}
                WHERE c.{child_col} IS NOT NULL
                  AND p.{parent_col} IS NULL
                LIMIT 5
                """
            ).fetchall()
        ]

        results.append(
            _build_rule(
                validation_run_id=validation_run_id,
                dimension="integrity",
                rule_code=f"fk_exists:{child_dataset}.{child_column}->{parent_dataset}.{parent_column}",
                dataset_name=child_dataset,
                severity="CRITICAL",
                base_weight=1.0,
                evaluated_records=int(evaluated),
                failed_records=int(failed),
                message=f"FK check {child_dataset}.{child_column} references {parent_dataset}.{parent_column}.",
                sample_rows=sample_rows,
            )
        )

    return results


def _conformance_rules(conn, validation_run_id: str, dataset_name: str) -> list[RuleResult]:
    table_name = _quote_ident(f"stg_{dataset_name}")
    columns = conn.execute(f"DESCRIBE {table_name}").fetchall()

    col_names = {row[0] for row in columns}
    results: list[RuleResult] = []

    for col in sorted(col_names & NEGATIVE_CHECK_COLUMNS):
        qcol = _quote_ident(col)
        evaluated = conn.execute(
            f"SELECT COUNT(*) FROM {table_name} WHERE {qcol} IS NOT NULL"
        ).fetchone()[0]

        failed = conn.execute(
            f"SELECT COUNT(*) FROM {table_name} WHERE TRY_CAST({qcol} AS DOUBLE) < 0"
        ).fetchone()[0]

        sample_rows = [
            str(r[0])
            for r in conn.execute(
                f"SELECT {qcol}::VARCHAR FROM {table_name} WHERE TRY_CAST({qcol} AS DOUBLE) < 0 LIMIT 5"
            ).fetchall()
        ]

        results.append(
            _build_rule(
                validation_run_id=validation_run_id,
                dimension="conformance",
                rule_code=f"non_negative:{col}",
                dataset_name=dataset_name,
                severity="HIGH",
                base_weight=0.8,
                evaluated_records=int(evaluated),
                failed_records=int(failed),
                message=f"Column '{col}' should be non-negative.",
                sample_rows=sample_rows,
            )
        )

    return results


def _temporal_rules(conn, validation_run_id: str, dataset_name: str) -> list[RuleResult]:
    table_name = _quote_ident(f"stg_{dataset_name}")
    columns = {row[0] for row in conn.execute(f"DESCRIBE {table_name}").fetchall()}

    if "order_date" not in columns or "ship_date" not in columns:
        return []

    evaluated = conn.execute(
        f"SELECT COUNT(*) FROM {table_name} WHERE order_date IS NOT NULL AND ship_date IS NOT NULL"
    ).fetchone()[0]

    failed = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM {table_name}
        WHERE order_date IS NOT NULL
          AND ship_date IS NOT NULL
          AND TRY_CAST(ship_date AS DATE) < TRY_CAST(order_date AS DATE)
        """
    ).fetchone()[0]

    sample_rows = [
        f"order_date={r[0]}, ship_date={r[1]}"
        for r in conn.execute(
            f"""
            SELECT order_date::VARCHAR, ship_date::VARCHAR
            FROM {table_name}
            WHERE order_date IS NOT NULL
              AND ship_date IS NOT NULL
              AND TRY_CAST(ship_date AS DATE) < TRY_CAST(order_date AS DATE)
            LIMIT 5
            """
        ).fetchall()
    ]

    return [
        _build_rule(
            validation_run_id=validation_run_id,
            dimension="temporal",
            rule_code="ship_date_after_order_date",
            dataset_name=dataset_name,
            severity="MEDIUM",
            base_weight=0.6,
            evaluated_records=int(evaluated),
            failed_records=int(failed),
            message="ship_date should be on or after order_date.",
            sample_rows=sample_rows,
        )
    ]


def _drift_rules(conn, validation_run_id: str, dataset_name: str) -> list[RuleResult]:
    versions = conn.execute(
        """
        SELECT schema_json
        FROM schema_versions
        WHERE dataset_name = ?
        ORDER BY version_no DESC
        LIMIT 2
        """,
        [dataset_name],
    ).fetchall()

    if len(versions) < 2:
        return [
            _build_rule(
                validation_run_id=validation_run_id,
                dimension="drift",
                rule_code="schema_drift_changes",
                dataset_name=dataset_name,
                severity="LOW",
                base_weight=0.5,
                evaluated_records=0,
                failed_records=0,
                message="Not enough schema history to evaluate drift.",
                sample_rows=[],
            )
        ]

    current = json.loads(versions[0][0])
    previous = json.loads(versions[1][0])

    current_cols = set(current.keys())
    previous_cols = set(previous.keys())

    added = sorted(current_cols - previous_cols)
    removed = sorted(previous_cols - current_cols)
    type_changes = sorted(
        [
            col
            for col in (current_cols & previous_cols)
            if str(current[col]).lower() != str(previous[col]).lower()
        ]
    )

    drift_count = len(added) + len(removed) + len(type_changes)

    return [
        _build_rule(
            validation_run_id=validation_run_id,
            dimension="drift",
            rule_code="schema_drift_changes",
            dataset_name=dataset_name,
            severity="LOW",
            base_weight=0.5,
            evaluated_records=max(1, len(previous_cols)),
            failed_records=drift_count,
            message="Schema drift check between latest two versions.",
            sample_rows=[
                f"added={added}",
                f"removed={removed}",
                f"type_changes={type_changes}",
            ],
        )
    ]


def compute_trust_score_from_rules(rule_results: list[dict]) -> tuple[dict[str, float], int]:
    penalties_by_dimension = {key: 0.0 for key in DIMENSION_WEIGHTS.keys()}

    for row in rule_results:
        penalties_by_dimension[row["dimension"]] += float(row["penalty_points"])

    dimension_scores = {
        dim: round(max(0.0, 100.0 - (penalty * 100.0)), 2)
        for dim, penalty in penalties_by_dimension.items()
    }

    trust_score = int(
        round(
            sum(dimension_scores[dim] * weight for dim, weight in DIMENSION_WEIGHTS.items())
        )
    )

    return dimension_scores, trust_score


def run_validation() -> dict:
    validation_run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc).isoformat()

    with get_conn() as conn:
        datasets = _latest_datasets(conn)
        all_rules: list[RuleResult] = []

        for dataset_id, dataset_name in datasets:
            all_rules.extend(_completeness_rules(conn, validation_run_id, dataset_id, dataset_name))
            all_rules.extend(_conformance_rules(conn, validation_run_id, dataset_name))
            all_rules.extend(_temporal_rules(conn, validation_run_id, dataset_name))
            all_rules.extend(_drift_rules(conn, validation_run_id, dataset_name))

        all_rules.extend(_integrity_rules(conn, validation_run_id))

        previous_row = conn.execute(
            """
            SELECT validation_run_id, trust_score
            FROM validation_runs
            ORDER BY started_at DESC
            LIMIT 1
            """
        ).fetchone()

        dimension_scores, trust_score = compute_trust_score_from_rules(all_rules)
        status = "FAILED" if any(r["failed_records"] > 0 and r["severity"] in {"CRITICAL", "HIGH"} for r in all_rules) else "PASSED"

        ended_at = datetime.now(timezone.utc).isoformat()

        conn.execute(
            """
            INSERT INTO validation_runs (
                validation_run_id, started_at, ended_at, status,
                trust_score, dimension_scores_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                validation_run_id,
                started_at,
                ended_at,
                status,
                trust_score,
                json.dumps(dimension_scores),
            ],
        )

        for row in all_rules:
            conn.execute(
                """
                INSERT INTO validation_results (
                    result_id, validation_run_id, dimension, rule_code,
                    dataset_name, severity, base_weight,
                    evaluated_records, failed_records, failure_rate,
                    penalty_points, message, sample_rows_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    row["result_id"],
                    row["validation_run_id"],
                    row["dimension"],
                    row["rule_code"],
                    row["dataset_name"],
                    row["severity"],
                    row["base_weight"],
                    row["evaluated_records"],
                    row["failed_records"],
                    row["failure_rate"],
                    row["penalty_points"],
                    row["message"],
                    row["sample_rows_json"],
                ],
            )

            if row["failed_records"] > 0:
                conn.execute(
                    """
                    INSERT INTO validation_exceptions (
                        exception_id, validation_run_id, result_id,
                        dataset_name, rule_code, sample_rows_json, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        str(uuid.uuid4()),
                        validation_run_id,
                        row["result_id"],
                        row["dataset_name"],
                        row["rule_code"],
                        row["sample_rows_json"],
                        ended_at,
                    ],
                )

        conn.execute(
            """
            INSERT INTO audit_event_log (event_id, event_type, payload_json, created_at)
            VALUES (?, ?, ?, ?)
            """,
            [
                str(uuid.uuid4()),
                "VALIDATION_RUN_COMPLETED",
                json.dumps(
                    {
                        "validation_run_id": validation_run_id,
                        "status": status,
                        "trust_score": trust_score,
                        "rule_count": len(all_rules),
                    }
                ),
                ended_at,
            ],
        )

    # Phase 6 alert hook: trust-score regression and low absolute score.
    drop_threshold = int(os.getenv("DATAFORGE_ALERT_TRUST_DROP", "10"))
    score_floor = int(os.getenv("DATAFORGE_ALERT_TRUST_FLOOR", "80"))
    if previous_row:
        previous_run_id, previous_score = previous_row[0], int(previous_row[1])
        score_drop = previous_score - trust_score
        if score_drop >= drop_threshold:
            emit_alert(
                alert_type="TRUST_SCORE_DROP",
                severity="HIGH",
                title="Trust score dropped significantly",
                message=f"Trust score dropped by {score_drop} points (from {previous_score} to {trust_score}).",
                context={
                    "previous_validation_run_id": previous_run_id,
                    "current_validation_run_id": validation_run_id,
                    "previous_score": previous_score,
                    "current_score": trust_score,
                    "drop_points": score_drop,
                },
            )

    if trust_score < score_floor:
        emit_alert(
            alert_type="TRUST_SCORE_LOW",
            severity="MEDIUM",
            title="Trust score below configured floor",
            message=f"Trust score is {trust_score}, below floor {score_floor}.",
            context={
                "validation_run_id": validation_run_id,
                "trust_score": trust_score,
                "score_floor": score_floor,
            },
        )

    return {
        "validation_run_id": validation_run_id,
        "started_at": started_at,
        "ended_at": ended_at,
        "status": status,
        "trust_score": trust_score,
        "dimension_scores": dimension_scores,
        "rule_count": len(all_rules),
    }


def list_validation_runs() -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT validation_run_id, started_at, ended_at, status, trust_score, dimension_scores_json
            FROM validation_runs
            ORDER BY started_at DESC
            """
        ).fetchall()

    return [
        {
            "validation_run_id": row[0],
            "started_at": row[1],
            "ended_at": row[2],
            "status": row[3],
            "trust_score": row[4],
            "dimension_scores": json.loads(row[5]) if row[5] else {},
        }
        for row in rows
    ]


def get_validation_results(validation_run_id: str) -> dict:
    with get_conn() as conn:
        run = conn.execute(
            """
            SELECT validation_run_id, started_at, ended_at, status, trust_score, dimension_scores_json
            FROM validation_runs
            WHERE validation_run_id = ?
            """,
            [validation_run_id],
        ).fetchone()

        if not run:
            raise ValueError("Validation run not found.")

        results = conn.execute(
            """
            SELECT result_id, dimension, rule_code, dataset_name, severity, base_weight,
                   evaluated_records, failed_records, failure_rate, penalty_points,
                   message, sample_rows_json
            FROM validation_results
            WHERE validation_run_id = ?
            ORDER BY dimension, rule_code
            """,
            [validation_run_id],
        ).fetchall()

        exceptions = conn.execute(
            """
            SELECT exception_id, dataset_name, rule_code, sample_rows_json, created_at
            FROM validation_exceptions
            WHERE validation_run_id = ?
            ORDER BY created_at DESC
            """,
            [validation_run_id],
        ).fetchall()

    return {
        "validation_run_id": run[0],
        "started_at": run[1],
        "ended_at": run[2],
        "status": run[3],
        "trust_score": run[4],
        "dimension_scores": json.loads(run[5]) if run[5] else {},
        "results": [
            {
                "result_id": row[0],
                "dimension": row[1],
                "rule_code": row[2],
                "dataset_name": row[3],
                "severity": row[4],
                "base_weight": row[5],
                "evaluated_records": row[6],
                "failed_records": row[7],
                "failure_rate": row[8],
                "penalty_points": row[9],
                "message": row[10],
                "sample_rows": json.loads(row[11]) if row[11] else [],
            }
            for row in results
        ],
        "exceptions": [
            {
                "exception_id": row[0],
                "dataset_name": row[1],
                "rule_code": row[2],
                "sample_rows": json.loads(row[3]) if row[3] else [],
                "created_at": row[4],
            }
            for row in exceptions
        ],
    }


def get_latest_trust_score() -> dict:
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT validation_run_id, started_at, ended_at, status, trust_score, dimension_scores_json
            FROM validation_runs
            ORDER BY started_at DESC
            LIMIT 1
            """
        ).fetchone()

    if not row:
        raise ValueError("No validation runs found.")

    return {
        "validation_run_id": row[0],
        "started_at": row[1],
        "ended_at": row[2],
        "status": row[3],
        "trust_score": row[4],
        "dimension_scores": json.loads(row[5]) if row[5] else {},
    }

"""Deterministic relationship inference service for Phase 2."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

from app.db import get_conn


def _quote_ident(identifier: str) -> str:
    return f'"{identifier.replace(chr(34), chr(34) * 2)}"'


def _latest_dataset_records(conn) -> list[tuple[str, str]]:
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
    return [(r[0], r[1]) for r in rows]


def _latest_profile_columns(conn, dataset_id: str) -> list[dict]:
    rows = conn.execute(
        """
        SELECT pr.column_name, pr.inferred_type, pr.is_candidate_key, pr.non_null_count, pr.distinct_count
        FROM profiling_results pr
        JOIN profiling_runs run ON run.run_id = pr.run_id
        WHERE run.dataset_id = ?
          AND run.run_at = (
              SELECT MAX(run_at)
              FROM profiling_runs
              WHERE dataset_id = ?
          )
        ORDER BY pr.column_name
        """,
        [dataset_id, dataset_id],
    ).fetchall()

    return [
        {
            "column_name": row[0],
            "inferred_type": row[1],
            "is_candidate_key": bool(row[2]),
            "non_null_count": int(row[3]),
            "distinct_count": int(row[4]),
        }
        for row in rows
    ]


def _load_distinct_values(conn, table_name: str, column_name: str) -> set[str]:
    col = _quote_ident(column_name)
    table = _quote_ident(table_name)
    rows = conn.execute(
        f"SELECT DISTINCT {col}::VARCHAR FROM {table} WHERE {col} IS NOT NULL LIMIT 100000"
    ).fetchall()
    return {row[0] for row in rows if row[0] is not None}


def _type_bucket(dtype: str) -> str:
    normalized = dtype.lower()
    if any(token in normalized for token in ["int", "float", "double", "decimal", "numeric"]):
        return "numeric"
    if any(token in normalized for token in ["date", "time"]):
        return "datetime"
    if "bool" in normalized:
        return "boolean"
    return "text"


def compute_confidence_score(
    overlap_ratio: float,
    parent_coverage_ratio: float,
    name_score: float,
    type_score: float,
) -> float:
    score = (
        0.6 * overlap_ratio
        + 0.15 * parent_coverage_ratio
        + 0.15 * name_score
        + 0.10 * type_score
    )
    return round(min(max(score, 0.0), 1.0), 4)


def infer_cardinality_hint(parent_unique: bool, child_unique: bool) -> str:
    if parent_unique and child_unique:
        return "one_to_one"
    if parent_unique and not child_unique:
        return "one_to_many"
    if not parent_unique and child_unique:
        return "many_to_one"
    return "many_to_many"


def _name_score(child_col: str, parent_col: str, parent_table: str) -> float:
    if child_col == parent_col:
        return 1.0
    if child_col.endswith("_id") and parent_col.endswith("_id"):
        left = child_col[:-3]
        right = parent_table.rstrip("s")
        if left == right:
            return 0.9
    if child_col.endswith("_id") and parent_col == "id":
        return 0.6
    return 0.2


def run_relationship_inference(min_confidence: float = 0.6) -> dict:
    inference_run_id = str(uuid.uuid4())
    created_at = datetime.now(timezone.utc).isoformat()

    with get_conn() as conn:
        datasets = _latest_dataset_records(conn)

        if len(datasets) < 2:
            return {
                "inference_run_id": inference_run_id,
                "created_at": created_at,
                "candidate_count": 0,
            }

        profiles: dict[str, list[dict]] = {
            dataset_id: _latest_profile_columns(conn, dataset_id)
            for dataset_id, _ in datasets
        }

        candidates_to_insert: list[dict] = []

        for child_dataset_id, child_dataset_name in datasets:
            child_table = f"stg_{child_dataset_name}"
            child_columns = profiles.get(child_dataset_id, [])
            for parent_dataset_id, parent_dataset_name in datasets:
                if child_dataset_id == parent_dataset_id:
                    continue

                parent_table = f"stg_{parent_dataset_name}"
                parent_columns = profiles.get(parent_dataset_id, [])
                parent_key_columns = [c for c in parent_columns if c["is_candidate_key"]]

                if not parent_key_columns:
                    continue

                for child_col in child_columns:
                    child_name = child_col["column_name"]
                    if child_name.lower() in {"id", "_id"}:
                        continue

                    child_values = _load_distinct_values(conn, child_table, child_name)
                    if not child_values:
                        continue

                    child_unique = child_col["distinct_count"] == child_col["non_null_count"]

                    for parent_col in parent_key_columns:
                        parent_name = parent_col["column_name"]
                        parent_values = _load_distinct_values(conn, parent_table, parent_name)
                        if not parent_values:
                            continue

                        overlap_count = len(child_values & parent_values)
                        if overlap_count == 0:
                            continue

                        overlap_ratio = overlap_count / max(len(child_values), 1)
                        parent_coverage_ratio = overlap_count / max(len(parent_values), 1)
                        name_score = _name_score(child_name, parent_name, parent_dataset_name)
                        type_score = 1.0 if _type_bucket(child_col["inferred_type"]) == _type_bucket(parent_col["inferred_type"]) else 0.0
                        confidence = compute_confidence_score(
                            overlap_ratio=overlap_ratio,
                            parent_coverage_ratio=parent_coverage_ratio,
                            name_score=name_score,
                            type_score=type_score,
                        )

                        if confidence < min_confidence:
                            continue

                        cardinality = infer_cardinality_hint(
                            parent_unique=True,
                            child_unique=child_unique,
                        )

                        candidate = {
                            "candidate_id": str(uuid.uuid4()),
                            "inference_run_id": inference_run_id,
                            "child_dataset_id": child_dataset_id,
                            "child_dataset_name": child_dataset_name,
                            "child_column": child_name,
                            "parent_dataset_id": parent_dataset_id,
                            "parent_dataset_name": parent_dataset_name,
                            "parent_column": parent_name,
                            "overlap_ratio": round(overlap_ratio, 4),
                            "parent_coverage_ratio": round(parent_coverage_ratio, 4),
                            "name_score": round(name_score, 4),
                            "type_score": round(type_score, 4),
                            "confidence_score": confidence,
                            "cardinality_hint": cardinality,
                            "status": "PENDING",
                            "rationale": (
                                f"Overlap={overlap_ratio:.2%}, ParentCoverage={parent_coverage_ratio:.2%}, "
                                f"NameScore={name_score:.2f}, TypeScore={type_score:.2f}"
                            ),
                            "evidence_json": json.dumps(
                                {
                                    "overlap_count": overlap_count,
                                    "child_distinct_count": len(child_values),
                                    "parent_distinct_count": len(parent_values),
                                }
                            ),
                            "created_at": created_at,
                        }
                        candidates_to_insert.append(candidate)

        # De-duplicate by same child/parent pair, keep highest confidence.
        deduped: dict[tuple[str, str, str, str], dict] = {}
        for c in candidates_to_insert:
            key = (c["child_dataset_name"], c["child_column"], c["parent_dataset_name"], c["parent_column"])
            if key not in deduped or c["confidence_score"] > deduped[key]["confidence_score"]:
                deduped[key] = c

        conn.execute(
            "DELETE FROM relationship_candidates WHERE inference_run_id = ?",
            [inference_run_id],
        )

        for c in deduped.values():
            conn.execute(
                """
                INSERT INTO relationship_candidates (
                    candidate_id, inference_run_id,
                    child_dataset_id, child_dataset_name, child_column,
                    parent_dataset_id, parent_dataset_name, parent_column,
                    overlap_ratio, parent_coverage_ratio, name_score, type_score,
                    confidence_score, cardinality_hint, status, rationale, evidence_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    c["candidate_id"],
                    c["inference_run_id"],
                    c["child_dataset_id"],
                    c["child_dataset_name"],
                    c["child_column"],
                    c["parent_dataset_id"],
                    c["parent_dataset_name"],
                    c["parent_column"],
                    c["overlap_ratio"],
                    c["parent_coverage_ratio"],
                    c["name_score"],
                    c["type_score"],
                    c["confidence_score"],
                    c["cardinality_hint"],
                    c["status"],
                    c["rationale"],
                    c["evidence_json"],
                    c["created_at"],
                ],
            )

    return {
        "inference_run_id": inference_run_id,
        "created_at": created_at,
        "candidate_count": len(deduped),
    }


def list_relationship_candidates(inference_run_id: str | None = None) -> list[dict]:
    with get_conn() as conn:
        if inference_run_id is None:
            latest = conn.execute(
                "SELECT inference_run_id FROM relationship_candidates ORDER BY created_at DESC LIMIT 1"
            ).fetchone()
            if not latest:
                return []
            inference_run_id = latest[0]

        rows = conn.execute(
            """
            SELECT candidate_id, inference_run_id,
                   child_dataset_name, child_column,
                   parent_dataset_name, parent_column,
                   overlap_ratio, parent_coverage_ratio,
                   name_score, type_score, confidence_score,
                   cardinality_hint, status, rationale, evidence_json, created_at
            FROM relationship_candidates
            WHERE inference_run_id = ?
            ORDER BY confidence_score DESC, child_dataset_name, child_column
            """,
            [inference_run_id],
        ).fetchall()

    return [
        {
            "candidate_id": row[0],
            "inference_run_id": row[1],
            "child_dataset_name": row[2],
            "child_column": row[3],
            "parent_dataset_name": row[4],
            "parent_column": row[5],
            "overlap_ratio": row[6],
            "parent_coverage_ratio": row[7],
            "name_score": row[8],
            "type_score": row[9],
            "confidence_score": row[10],
            "cardinality_hint": row[11],
            "status": row[12],
            "rationale": row[13],
            "evidence": json.loads(row[14]) if row[14] else {},
            "created_at": row[15],
        }
        for row in rows
    ]


def decide_relationship_candidate(candidate_id: str, decision: str, reviewer_notes: str | None = None) -> dict:
    normalized = decision.strip().upper()
    if normalized not in {"ACCEPTED", "REJECTED"}:
        raise ValueError("Decision must be ACCEPTED or REJECTED.")

    decided_at = datetime.now(timezone.utc).isoformat()
    decision_id = str(uuid.uuid4())

    with get_conn() as conn:
        candidate = conn.execute(
            "SELECT candidate_id FROM relationship_candidates WHERE candidate_id = ?",
            [candidate_id],
        ).fetchone()
        if not candidate:
            raise ValueError("Candidate not found.")

        conn.execute(
            "UPDATE relationship_candidates SET status = ? WHERE candidate_id = ?",
            [normalized, candidate_id],
        )

        conn.execute(
            """
            INSERT INTO relationship_decisions (decision_id, candidate_id, decision, reviewer_notes, decided_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            [decision_id, candidate_id, normalized, reviewer_notes, decided_at],
        )

    return {
        "decision_id": decision_id,
        "candidate_id": candidate_id,
        "decision": normalized,
        "reviewer_notes": reviewer_notes,
        "decided_at": decided_at,
    }

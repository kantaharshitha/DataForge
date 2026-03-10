"""ER model graph service built from existing metadata tables."""

from __future__ import annotations

import json

from app.db import get_conn


def _latest_dataset_names(conn) -> list[str]:
    rows = conn.execute(
        """
        WITH ranked AS (
            SELECT dataset_name, ingested_at,
                   ROW_NUMBER() OVER (PARTITION BY dataset_name ORDER BY ingested_at DESC) AS rn
            FROM dataset_registry
        )
        SELECT dataset_name
        FROM ranked
        WHERE rn = 1
        ORDER BY dataset_name
        """
    ).fetchall()
    return [str(r[0]) for r in rows]


def _latest_schema_map(conn, dataset_name: str) -> dict[str, str]:
    row = conn.execute(
        """
        SELECT schema_json
        FROM schema_versions
        WHERE dataset_name = ?
        ORDER BY version_no DESC, created_at DESC
        LIMIT 1
        """,
        [dataset_name],
    ).fetchone()
    if not row or not row[0]:
        return {}
    return json.loads(row[0])


def _latest_key_candidates(conn, dataset_name: str) -> set[str]:
    row = conn.execute(
        """
        SELECT key_candidates_json
        FROM schema_versions
        WHERE dataset_name = ?
        ORDER BY version_no DESC, created_at DESC
        LIMIT 1
        """,
        [dataset_name],
    ).fetchone()
    if not row or not row[0]:
        return set()
    parsed = json.loads(row[0])
    return {str(c) for c in parsed if c}


def _latest_profile_candidate_keys(conn, dataset_name: str) -> set[str]:
    rows = conn.execute(
        """
        SELECT pr.column_name
        FROM profiling_results pr
        JOIN profiling_runs run ON run.run_id = pr.run_id
        WHERE run.dataset_name = ?
          AND run.run_at = (
              SELECT MAX(run_at)
              FROM profiling_runs
              WHERE dataset_name = ?
          )
          AND pr.is_candidate_key = TRUE
        ORDER BY pr.column_name
        """,
        [dataset_name, dataset_name],
    ).fetchall()
    return {str(r[0]) for r in rows}


def _accepted_relationships(conn) -> list[dict]:
    rows = conn.execute(
        """
        WITH latest_accepted AS (
            SELECT
                child_dataset_name, child_column, parent_dataset_name, parent_column,
                cardinality_hint, created_at, confidence_score,
                ROW_NUMBER() OVER (
                    PARTITION BY child_dataset_name, child_column, parent_dataset_name, parent_column
                    ORDER BY created_at DESC, confidence_score DESC
                ) AS rn
            FROM relationship_candidates
            WHERE status = 'ACCEPTED'
        )
        SELECT child_dataset_name, child_column, parent_dataset_name, parent_column, cardinality_hint
        FROM latest_accepted
        WHERE rn = 1
        ORDER BY child_dataset_name, child_column, parent_dataset_name, parent_column
        """
    ).fetchall()
    return [
        {
            "from_table": str(r[0]),
            "from_column": str(r[1]),
            "to_table": str(r[2]),
            "to_column": str(r[3]),
            "cardinality": str(r[4]) if r[4] is not None else None,
        }
        for r in rows
    ]


def get_er_model_graph() -> dict:
    with get_conn() as conn:
        dataset_names = _latest_dataset_names(conn)
        relationships = _accepted_relationships(conn)

        fk_pairs = {
            (rel["from_table"], rel["from_column"])
            for rel in relationships
        }

        tables: list[dict] = []
        for dataset_name in dataset_names:
            schema_map = _latest_schema_map(conn, dataset_name)
            if not schema_map:
                continue

            # PK inference uses both schema version key candidates and latest profiling key flags.
            key_candidates = _latest_key_candidates(conn, dataset_name)
            key_candidates |= _latest_profile_candidate_keys(conn, dataset_name)

            columns = []
            for col_name in sorted(schema_map.keys()):
                columns.append(
                    {
                        "name": col_name,
                        "type": str(schema_map[col_name]),
                        "pk": col_name in key_candidates,
                        "fk": (dataset_name, col_name) in fk_pairs,
                    }
                )

            tables.append({"name": dataset_name, "columns": columns})

    return {
        "tables": tables,
        "relationships": relationships,
    }


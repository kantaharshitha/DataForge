"""Ingestion service for file upload, staging load, and profiling persistence."""

from __future__ import annotations

import hashlib
import io
import json
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from app.services.profiling import profile_dataframe

PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

ALLOWED_EXTENSIONS = {".csv", ".xlsx", ".xls"}
MAX_UPLOAD_BYTES = 25 * 1024 * 1024


def _sanitize_name(name: str) -> str:
    sanitized = re.sub(r"[^a-zA-Z0-9]+", "_", name.strip().lower()).strip("_")
    return sanitized or "dataset"


def validate_upload_inputs(file_name: str | None, data_bytes: bytes) -> str:
    if not file_name:
        raise ValueError("File name is required.")

    suffix = Path(file_name).suffix.lower()
    if suffix not in ALLOWED_EXTENSIONS:
        raise ValueError("Unsupported file type. Use CSV or XLSX.")

    if not data_bytes:
        raise ValueError("Uploaded file is empty.")

    if len(data_bytes) > MAX_UPLOAD_BYTES:
        raise ValueError("File too large. Maximum supported size is 25 MB.")

    return suffix


def _load_dataframe_from_bytes(file_name: str, data_bytes: bytes, suffix: str) -> pd.DataFrame:
    buffer = io.BytesIO(data_bytes)

    try:
        if suffix == ".csv":
            df = pd.read_csv(buffer)
        else:
            df = pd.read_excel(buffer)
    except Exception as exc:  # pragma: no cover - parser-specific exceptions
        raise ValueError(f"Failed to parse file '{file_name}': {exc}") from exc

    if df.shape[1] == 0:
        raise ValueError("Dataset has no columns.")

    if len(df) == 0:
        raise ValueError("Dataset has no rows.")

    if df.columns.duplicated().any():
        raise ValueError("Dataset contains duplicate column names.")

    return df


def ingest_file(file_name: str, data_bytes: bytes) -> dict:
    from app.db import get_conn

    suffix = validate_upload_inputs(file_name, data_bytes)

    dataset_name = _sanitize_name(Path(file_name).stem)
    dataset_id = str(uuid.uuid4())
    version_id = str(uuid.uuid4())
    run_id = str(uuid.uuid4())
    file_hash = hashlib.sha256(data_bytes).hexdigest()

    ingest_ts = datetime.now(timezone.utc).isoformat()

    df = _load_dataframe_from_bytes(file_name, data_bytes, suffix)
    row_count = len(df)
    duplicate_rows, column_profiles, key_candidates = profile_dataframe(df)

    schema_map = {col: str(dtype) for col, dtype in df.dtypes.items()}
    table_name = f"stg_{dataset_name}"
    target_path = RAW_DIR / f"{dataset_id}_{Path(file_name).name}"

    with get_conn() as conn:
        conn.execute("BEGIN")
        try:
            conn.execute(
                """
                INSERT INTO dataset_registry (dataset_id, dataset_name, source_file, ingested_at, row_count, file_hash, status)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [dataset_id, dataset_name, file_name, ingest_ts, row_count, file_hash, "INGESTED"],
            )

            current_version = conn.execute(
                "SELECT COALESCE(MAX(version_no), 0) + 1 FROM schema_versions WHERE dataset_name = ?",
                [dataset_name],
            ).fetchone()[0]

            conn.execute(
                """
                INSERT INTO schema_versions (version_id, dataset_name, version_no, created_at, schema_json, key_candidates_json)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    version_id,
                    dataset_name,
                    int(current_version),
                    ingest_ts,
                    json.dumps(schema_map),
                    json.dumps(key_candidates),
                ],
            )

            conn.register("incoming_df", df)
            conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM incoming_df")

            conn.execute(
                """
                INSERT INTO profiling_runs (run_id, dataset_id, dataset_name, run_at, row_count, column_count, duplicate_rows)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [run_id, dataset_id, dataset_name, ingest_ts, row_count, df.shape[1], duplicate_rows],
            )

            for p in column_profiles:
                conn.execute(
                    """
                    INSERT INTO profiling_results
                    (result_id, run_id, column_name, inferred_type, null_count, non_null_count, null_pct, unique_pct,
                     distinct_count, duplicate_value_count, is_candidate_key, min_value, max_value, mean_value, sample_values_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        str(uuid.uuid4()),
                        run_id,
                        p.column_name,
                        p.inferred_type,
                        p.null_count,
                        p.non_null_count,
                        p.null_pct,
                        p.unique_pct,
                        p.distinct_count,
                        p.duplicate_value_count,
                        p.is_candidate_key,
                        p.min_value,
                        p.max_value,
                        p.mean_value,
                        json.dumps(p.sample_values),
                    ],
                )

            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    target_path.write_bytes(data_bytes)

    return {
        "dataset_id": dataset_id,
        "dataset_name": dataset_name,
        "row_count": row_count,
        "schema_version": int(current_version),
        "profiling_run_id": run_id,
        "key_candidates": key_candidates,
    }

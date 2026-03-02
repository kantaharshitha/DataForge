"""Typed API response models for DataForge."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    status: str


class UploadResponse(BaseModel):
    dataset_id: str
    dataset_name: str
    row_count: int
    schema_version: int
    profiling_run_id: str
    key_candidates: list[str]


class DatasetSummary(BaseModel):
    dataset_id: str
    dataset_name: str
    source_file: str
    ingested_at: datetime
    row_count: int
    status: str


class ColumnProfileResponse(BaseModel):
    column_name: str
    inferred_type: str
    null_count: int
    non_null_count: int
    null_pct: float
    unique_pct: float
    distinct_count: int
    duplicate_value_count: int
    is_candidate_key: bool
    min_value: str | None = None
    max_value: str | None = None
    mean_value: float | None = None
    sample_values: list[str] = Field(default_factory=list)


class ProfileRunResponse(BaseModel):
    run_id: str
    dataset_name: str
    run_at: datetime
    row_count: int
    column_count: int
    duplicate_rows: int
    columns: list[ColumnProfileResponse]

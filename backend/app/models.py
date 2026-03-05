"""Typed API request/response models for DataForge."""

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


class InferenceRunResponse(BaseModel):
    inference_run_id: str
    created_at: datetime
    candidate_count: int


class RelationshipCandidateResponse(BaseModel):
    candidate_id: str
    inference_run_id: str
    child_dataset_name: str
    child_column: str
    parent_dataset_name: str
    parent_column: str
    overlap_ratio: float
    parent_coverage_ratio: float
    name_score: float
    type_score: float
    confidence_score: float
    cardinality_hint: str
    status: str
    rationale: str
    evidence: dict = Field(default_factory=dict)
    created_at: datetime


class RelationshipDecisionRequest(BaseModel):
    candidate_id: str
    decision: str
    reviewer_notes: str | None = None


class RelationshipDecisionResponse(BaseModel):
    decision_id: str
    candidate_id: str
    decision: str
    reviewer_notes: str | None = None
    decided_at: datetime


class ValidationRunResponse(BaseModel):
    validation_run_id: str
    started_at: datetime
    ended_at: datetime
    status: str
    trust_score: int
    dimension_scores: dict[str, float] = Field(default_factory=dict)
    rule_count: int


class ValidationRunSummaryResponse(BaseModel):
    validation_run_id: str
    started_at: datetime
    ended_at: datetime
    status: str
    trust_score: int
    dimension_scores: dict[str, float] = Field(default_factory=dict)


class ValidationResultItemResponse(BaseModel):
    result_id: str
    dimension: str
    rule_code: str
    dataset_name: str
    severity: str
    base_weight: float
    evaluated_records: int
    failed_records: int
    failure_rate: float
    penalty_points: float
    message: str
    sample_rows: list[str] = Field(default_factory=list)


class ValidationExceptionItemResponse(BaseModel):
    exception_id: str
    dataset_name: str
    rule_code: str
    sample_rows: list[str] = Field(default_factory=list)
    created_at: datetime


class ValidationRunDetailResponse(BaseModel):
    validation_run_id: str
    started_at: datetime
    ended_at: datetime
    status: str
    trust_score: int
    dimension_scores: dict[str, float] = Field(default_factory=dict)
    results: list[ValidationResultItemResponse]
    exceptions: list[ValidationExceptionItemResponse]


class LatestTrustScoreResponse(BaseModel):
    validation_run_id: str
    started_at: datetime
    ended_at: datetime
    status: str
    trust_score: int
    dimension_scores: dict[str, float] = Field(default_factory=dict)


class KpiRegistryItemResponse(BaseModel):
    kpi_id: str
    kpi_code: str
    kpi_name: str
    definition: str
    formula: str
    required_fields: list[str] = Field(default_factory=list)
    status: str
    created_at: datetime
    updated_at: datetime


class KpiSeedResponse(BaseModel):
    inserted: int


class KpiRunResponse(BaseModel):
    kpi_run_id: str
    validation_run_id: str | None = None
    generated_at: datetime
    status: str
    kpi_values: dict[str, float] = Field(default_factory=dict)


class KpiLatestResponse(BaseModel):
    kpi_run_id: str
    validation_run_id: str | None = None
    generated_at: datetime
    status: str
    kpi_values: dict[str, float] = Field(default_factory=dict)


class DashboardCardResponse(BaseModel):
    kpi_code: str
    value: float


class DashboardTrustContextResponse(BaseModel):
    validation_run_id: str
    trust_score: int
    dimension_scores: dict[str, float] = Field(default_factory=dict)
    status: str


class ExecutiveDashboardResponse(BaseModel):
    kpi_run_id: str
    generated_at: datetime
    cards: list[DashboardCardResponse]
    trust_context: DashboardTrustContextResponse | None = None


class DriftRunResponse(BaseModel):
    drift_run_id: str
    dataset_name: str
    from_version: int | None = None
    to_version: int | None = None
    run_at: datetime
    event_count: int
    high_count: int
    medium_count: int
    low_count: int
    status: str


class DriftRunExecuteResponse(BaseModel):
    run_count: int
    total_events: int
    runs: list[DriftRunResponse] = Field(default_factory=list)


class DriftEventResponse(BaseModel):
    event_id: str
    drift_run_id: str
    dataset_name: str
    change_type: str
    column_name: str | None = None
    old_value: str | None = None
    new_value: str | None = None
    severity: str
    details: dict = Field(default_factory=dict)
    created_at: datetime


class LineageBuildResponse(BaseModel):
    lineage_run_id: str
    run_at: datetime
    status: str
    node_count: int
    edge_count: int


class LineageRunResponse(BaseModel):
    lineage_run_id: str
    run_at: datetime
    status: str
    source_context: dict = Field(default_factory=dict)


class LineageNodeResponse(BaseModel):
    node_id: str
    node_type: str
    node_key: str
    display_name: str
    metadata: dict = Field(default_factory=dict)
    created_at: datetime
    updated_at: datetime


class LineageEdgeResponse(BaseModel):
    edge_id: str
    lineage_run_id: str
    from_node_id: str
    to_node_id: str
    edge_type: str
    metadata: dict = Field(default_factory=dict)
    created_at: datetime


class LineageGraphResponse(BaseModel):
    lineage_run_id: str
    nodes: list[LineageNodeResponse] = Field(default_factory=list)
    edges: list[LineageEdgeResponse] = Field(default_factory=list)


class OpsCleanupResponse(BaseModel):
    keep_last_runs: int
    keep_raw_files: int
    deleted: dict[str, int] = Field(default_factory=dict)


class PipelineStageMetric(BaseModel):
    stage: str
    duration_ms: float
    details: dict = Field(default_factory=dict)


class PipelineRunResponse(BaseModel):
    correlation_id: str
    started_at: datetime
    ended_at: datetime
    total_duration_ms: float
    stage_metrics: list[PipelineStageMetric] = Field(default_factory=list)
    summary: dict = Field(default_factory=dict)

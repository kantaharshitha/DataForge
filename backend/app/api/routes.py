"""API routes for DataForge operations."""

from __future__ import annotations

import json

from fastapi import APIRouter, File, HTTPException, UploadFile

from app.db import get_conn
from app.models import (
    DatasetSummary,
    DriftEventResponse,
    DriftRunExecuteResponse,
    DriftRunResponse,
    ExecutiveDashboardResponse,
    HealthResponse,
    InferenceRunResponse,
    KpiLatestResponse,
    KpiRegistryItemResponse,
    KpiRunResponse,
    KpiSeedResponse,
    LatestTrustScoreResponse,
    LineageBuildResponse,
    LineageGraphResponse,
    LineageRunResponse,
    ProfileRunResponse,
    RelationshipCandidateResponse,
    RelationshipDecisionRequest,
    RelationshipDecisionResponse,
    UploadResponse,
    ValidationRunDetailResponse,
    ValidationRunResponse,
    ValidationRunSummaryResponse,
)
from app.services.drift import (
    get_latest_drift_run,
    list_drift_events,
    list_drift_runs,
    run_schema_drift_scan,
)
from app.services.ingestion import ingest_file
from app.services.inference import (
    decide_relationship_candidate,
    list_relationship_candidates,
    run_relationship_inference,
)
from app.services.kpi import (
    get_executive_dashboard,
    get_latest_kpi_run,
    list_kpi_registry,
    run_kpis,
    seed_kpi_registry,
)
from app.services.lineage import (
    build_lineage_graph,
    get_lineage_for_dataset,
    get_lineage_for_kpi,
    get_lineage_graph,
    list_lineage_runs,
)
from app.services.validation import (
    get_latest_trust_score,
    get_validation_results,
    list_validation_runs,
    run_validation,
)

router = APIRouter()


@router.get("/health", response_model=HealthResponse)
def health_check() -> HealthResponse:
    return HealthResponse(status="ok")


@router.post("/upload", response_model=UploadResponse)
async def upload_dataset(file: UploadFile = File(...)) -> UploadResponse:
    try:
        payload = await file.read()
        result = ingest_file(file.filename, payload)
        return UploadResponse(**result)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/datasets", response_model=list[DatasetSummary])
def list_datasets() -> list[DatasetSummary]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT dataset_id, dataset_name, source_file, ingested_at, row_count, status
            FROM dataset_registry
            ORDER BY ingested_at DESC
            """
        ).fetchall()

    return [
        DatasetSummary(
            dataset_id=r[0],
            dataset_name=r[1],
            source_file=r[2],
            ingested_at=r[3],
            row_count=r[4],
            status=r[5],
        )
        for r in rows
    ]


@router.get("/profiles/{dataset_id}", response_model=ProfileRunResponse)
def get_latest_profile(dataset_id: str) -> ProfileRunResponse:
    with get_conn() as conn:
        run = conn.execute(
            """
            SELECT run_id, dataset_name, run_at, row_count, column_count, duplicate_rows
            FROM profiling_runs
            WHERE dataset_id = ?
            ORDER BY run_at DESC
            LIMIT 1
            """,
            [dataset_id],
        ).fetchone()

        if not run:
            raise HTTPException(status_code=404, detail="Profile run not found for dataset_id")

        results = conn.execute(
            """
            SELECT column_name, inferred_type, null_count, non_null_count, null_pct, unique_pct,
                   distinct_count, duplicate_value_count, is_candidate_key, min_value, max_value,
                   mean_value, sample_values_json
            FROM profiling_results
            WHERE run_id = ?
            ORDER BY column_name
            """,
            [run[0]],
        ).fetchall()

    return ProfileRunResponse(
        run_id=run[0],
        dataset_name=run[1],
        run_at=run[2],
        row_count=run[3],
        column_count=run[4],
        duplicate_rows=run[5],
        columns=[
            {
                "column_name": row[0],
                "inferred_type": row[1],
                "null_count": row[2],
                "non_null_count": row[3],
                "null_pct": row[4],
                "unique_pct": row[5],
                "distinct_count": row[6],
                "duplicate_value_count": row[7],
                "is_candidate_key": row[8],
                "min_value": row[9],
                "max_value": row[10],
                "mean_value": row[11],
                "sample_values": json.loads(row[12]) if row[12] else [],
            }
            for row in results
        ],
    )


@router.post("/inference/run", response_model=InferenceRunResponse)
def run_inference() -> InferenceRunResponse:
    result = run_relationship_inference()
    return InferenceRunResponse(**result)


@router.get("/inference/candidates", response_model=list[RelationshipCandidateResponse])
def get_inference_candidates(inference_run_id: str | None = None) -> list[RelationshipCandidateResponse]:
    rows = list_relationship_candidates(inference_run_id=inference_run_id)
    return [RelationshipCandidateResponse(**row) for row in rows]


@router.post("/inference/decide", response_model=RelationshipDecisionResponse)
def decide_inference_candidate(request: RelationshipDecisionRequest) -> RelationshipDecisionResponse:
    try:
        result = decide_relationship_candidate(
            candidate_id=request.candidate_id,
            decision=request.decision,
            reviewer_notes=request.reviewer_notes,
        )
        return RelationshipDecisionResponse(**result)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/validation/run", response_model=ValidationRunResponse)
def execute_validation() -> ValidationRunResponse:
    result = run_validation()
    return ValidationRunResponse(**result)


@router.get("/validation/runs", response_model=list[ValidationRunSummaryResponse])
def get_validation_runs() -> list[ValidationRunSummaryResponse]:
    return [ValidationRunSummaryResponse(**row) for row in list_validation_runs()]


@router.get("/validation/results/{validation_run_id}", response_model=ValidationRunDetailResponse)
def get_validation_run_results(validation_run_id: str) -> ValidationRunDetailResponse:
    try:
        return ValidationRunDetailResponse(**get_validation_results(validation_run_id))
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/trust/latest", response_model=LatestTrustScoreResponse)
def get_latest_trust() -> LatestTrustScoreResponse:
    try:
        return LatestTrustScoreResponse(**get_latest_trust_score())
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/kpi/seed", response_model=KpiSeedResponse)
def seed_kpis() -> KpiSeedResponse:
    return KpiSeedResponse(inserted=seed_kpi_registry())


@router.get("/kpi/registry", response_model=list[KpiRegistryItemResponse])
def get_kpi_registry() -> list[KpiRegistryItemResponse]:
    return [KpiRegistryItemResponse(**row) for row in list_kpi_registry()]


@router.post("/kpi/run", response_model=KpiRunResponse)
def execute_kpi_run() -> KpiRunResponse:
    return KpiRunResponse(**run_kpis())


@router.get("/kpi/latest", response_model=KpiLatestResponse)
def get_kpi_latest() -> KpiLatestResponse:
    try:
        return KpiLatestResponse(**get_latest_kpi_run())
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/dashboard/executive", response_model=ExecutiveDashboardResponse)
def get_dashboard_executive() -> ExecutiveDashboardResponse:
    try:
        return ExecutiveDashboardResponse(**get_executive_dashboard())
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/drift/run", response_model=DriftRunExecuteResponse)
def execute_schema_drift(dataset_name: str | None = None) -> DriftRunExecuteResponse:
    return DriftRunExecuteResponse(**run_schema_drift_scan(dataset_name=dataset_name))


@router.get("/drift/runs", response_model=list[DriftRunResponse])
def get_drift_runs() -> list[DriftRunResponse]:
    return [DriftRunResponse(**row) for row in list_drift_runs()]


@router.get("/drift/latest", response_model=DriftRunResponse)
def get_drift_latest() -> DriftRunResponse:
    try:
        return DriftRunResponse(**get_latest_drift_run())
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/drift/events/{dataset_name}", response_model=list[DriftEventResponse])
def get_drift_dataset_events(dataset_name: str) -> list[DriftEventResponse]:
    return [DriftEventResponse(**row) for row in list_drift_events(dataset_name)]


@router.post("/lineage/build", response_model=LineageBuildResponse)
def execute_lineage_build() -> LineageBuildResponse:
    return LineageBuildResponse(**build_lineage_graph())


@router.get("/lineage/runs", response_model=list[LineageRunResponse])
def get_lineage_runs() -> list[LineageRunResponse]:
    return [LineageRunResponse(**row) for row in list_lineage_runs()]


@router.get("/lineage/graph", response_model=LineageGraphResponse)
def get_lineage_graph_view(lineage_run_id: str | None = None) -> LineageGraphResponse:
    try:
        return LineageGraphResponse(**get_lineage_graph(lineage_run_id=lineage_run_id))
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/lineage/kpi/{kpi_code}", response_model=LineageGraphResponse)
def get_lineage_kpi_view(kpi_code: str, lineage_run_id: str | None = None) -> LineageGraphResponse:
    try:
        return LineageGraphResponse(**get_lineage_for_kpi(kpi_code=kpi_code, lineage_run_id=lineage_run_id))
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/lineage/dataset/{dataset_name}", response_model=LineageGraphResponse)
def get_lineage_dataset_view(dataset_name: str, lineage_run_id: str | None = None) -> LineageGraphResponse:
    try:
        return LineageGraphResponse(
            **get_lineage_for_dataset(dataset_name=dataset_name, lineage_run_id=lineage_run_id)
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

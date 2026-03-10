"""API routes for DataForge operations."""

from __future__ import annotations

import csv
import json
import os
import zipfile
from io import BytesIO
from io import StringIO

from fastapi import APIRouter, Depends, File, Header, HTTPException, UploadFile
from fastapi.responses import PlainTextResponse, Response

from app.db import get_conn, get_runtime_info
from app.models import (
    AlertAssignRequest,
    AlertAssignResponse,
    AlertAcknowledgeRequest,
    AlertAcknowledgeResponse,
    AlertEventResponse,
    AlertEscalationRunResponse,
    AlertSLABreachEventResponse,
    AlertSLABreachInboxResponse,
    AlertSLAHistoryPointResponse,
    AlertSLABreachRunResponse,
    AlertSLAResponse,
    AlertSummaryResponse,
    DatasetSummary,
    DriftEventResponse,
    DriftRunExecuteResponse,
    DriftRunResponse,
    ERModelResponse,
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
    OpsCleanupResponse,
    PipelineRunResponse,
    ProfileRunResponse,
    RelationshipCandidateResponse,
    RelationshipDecisionRequest,
    RelationshipDecisionResponse,
    RuntimeInfoResponse,
    UploadResponse,
    ValidationRunDetailResponse,
    ValidationRunResponse,
    ValidationRunSummaryResponse,
)
from app.services.alerts import (
    acknowledge_alert,
    assign_alert,
    list_alert_sla_breaches,
    list_recent_alerts,
    run_alert_escalation_scan,
    run_alert_sla_breach_check,
    get_alert_sla_history,
    summarize_alert_sla,
    summarize_alerts,
)
from app.services.cleanup import run_cleanup
from app.services.pipeline import run_pipeline_with_observability
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
from app.services.model_graph import get_er_model_graph
from app.services.validation import (
    get_latest_trust_score,
    get_validation_results,
    list_validation_runs,
    run_validation,
)

router = APIRouter()


def require_ops_api_key(x_api_key: str | None = Header(default=None)) -> None:
    expected = os.getenv("DATAFORGE_OPS_API_KEY")
    if not expected:
        return
    if x_api_key != expected:
        raise HTTPException(status_code=401, detail="Unauthorized: invalid x-api-key")


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


@router.get("/model/er", response_model=ERModelResponse)
def get_er_model_view() -> ERModelResponse:
    return ERModelResponse(**get_er_model_graph())


@router.get("/exports/drift/{dataset_name}.csv", response_class=PlainTextResponse)
def export_drift_events_csv(dataset_name: str) -> PlainTextResponse:
    events = list_drift_events(dataset_name)
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "event_id",
            "drift_run_id",
            "dataset_name",
            "change_type",
            "column_name",
            "old_value",
            "new_value",
            "severity",
            "created_at",
        ]
    )
    for event in events:
        writer.writerow(
            [
                event.get("event_id", ""),
                event.get("drift_run_id", ""),
                event.get("dataset_name", ""),
                event.get("change_type", ""),
                event.get("column_name", ""),
                event.get("old_value", ""),
                event.get("new_value", ""),
                event.get("severity", ""),
                event.get("created_at", ""),
            ]
        )
    return PlainTextResponse(
        content=output.getvalue(),
        headers={"Content-Disposition": f'attachment; filename="drift_{dataset_name}.csv"'},
    )


@router.get("/exports/validation/{validation_run_id}.csv", response_class=PlainTextResponse)
def export_validation_results_csv(validation_run_id: str) -> PlainTextResponse:
    try:
        payload = get_validation_results(validation_run_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "result_id",
            "dimension",
            "rule_code",
            "dataset_name",
            "severity",
            "evaluated_records",
            "failed_records",
            "failure_rate",
            "penalty_points",
            "message",
        ]
    )
    for row in payload.get("results", []):
        writer.writerow(
            [
                row.get("result_id", ""),
                row.get("dimension", ""),
                row.get("rule_code", ""),
                row.get("dataset_name", ""),
                row.get("severity", ""),
                row.get("evaluated_records", ""),
                row.get("failed_records", ""),
                row.get("failure_rate", ""),
                row.get("penalty_points", ""),
                row.get("message", ""),
            ]
        )
    return PlainTextResponse(
        content=output.getvalue(),
        headers={"Content-Disposition": f'attachment; filename="validation_{validation_run_id}.csv"'},
    )


@router.get("/exports/lineage/{lineage_run_id}.json")
def export_lineage_json(lineage_run_id: str) -> dict:
    try:
        return get_lineage_graph(lineage_run_id=lineage_run_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/exports/run/{correlation_id}.zip")
def export_pipeline_artifact_bundle(correlation_id: str) -> Response:
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT correlation_id, started_at, ended_at, total_duration_ms, summary_json
            FROM pipeline_run_log
            WHERE correlation_id = ?
            """,
            [correlation_id],
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Pipeline run not found for correlation_id")

        stage_rows = conn.execute(
            """
            SELECT stage_order, stage_name, duration_ms, details_json, created_at
            FROM pipeline_stage_metrics
            WHERE correlation_id = ?
            ORDER BY stage_order ASC
            """,
            [correlation_id],
        ).fetchall()

    summary = json.loads(row[4]) if row[4] else {}
    validation_run_id = summary.get("validation_run_id")
    lineage_run_id = summary.get("lineage_run_id")
    drift_run_ids = summary.get("drift_run_ids", [])

    drift_events: list[dict] = []
    if drift_run_ids:
        with get_conn() as conn:
            placeholders = ",".join(["?"] * len(drift_run_ids))
            events = conn.execute(
                f"""
                SELECT event_id, drift_run_id, dataset_name, change_type, column_name, old_value, new_value, severity, created_at
                FROM schema_drift_events
                WHERE drift_run_id IN ({placeholders})
                ORDER BY created_at DESC
                """,
                drift_run_ids,
            ).fetchall()
            drift_events = [
                {
                    "event_id": e[0],
                    "drift_run_id": e[1],
                    "dataset_name": e[2],
                    "change_type": e[3],
                    "column_name": e[4],
                    "old_value": e[5],
                    "new_value": e[6],
                    "severity": e[7],
                    "created_at": str(e[8]),
                }
                for e in events
            ]

    validation_results = None
    if validation_run_id:
        try:
            validation_results = get_validation_results(validation_run_id)
        except ValueError:
            validation_results = None

    lineage_graph = None
    if lineage_run_id:
        try:
            lineage_graph = get_lineage_graph(lineage_run_id=lineage_run_id)
        except ValueError:
            lineage_graph = None

    manifest = {
        "correlation_id": row[0],
        "started_at": str(row[1]),
        "ended_at": str(row[2]),
        "total_duration_ms": row[3],
        "summary": summary,
        "stage_metrics_count": len(stage_rows),
    }

    stage_metrics = [
        {
            "stage_order": r[0],
            "stage_name": r[1],
            "duration_ms": r[2],
            "details": json.loads(r[3]) if r[3] else {},
            "created_at": str(r[4]),
        }
        for r in stage_rows
    ]

    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("manifest.json", json.dumps(manifest, indent=2))
        zf.writestr("pipeline_stage_metrics.json", json.dumps(stage_metrics, indent=2))
        zf.writestr("drift_events.json", json.dumps(drift_events, indent=2))
        if validation_results is not None:
            zf.writestr("validation_results.json", json.dumps(validation_results, indent=2, default=str))
        if lineage_graph is not None:
            zf.writestr("lineage_graph.json", json.dumps(lineage_graph, indent=2, default=str))

    bundle_name = f"dataforge_bundle_{correlation_id}.zip"
    return Response(
        content=zip_buffer.getvalue(),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{bundle_name}"'},
    )


@router.get("/alerts/recent", response_model=list[AlertEventResponse])
def get_recent_alerts(limit: int = 50) -> list[AlertEventResponse]:
    return [AlertEventResponse(**row) for row in list_recent_alerts(limit=limit)]


@router.get("/alerts/summary", response_model=AlertSummaryResponse)
def get_alerts_summary(window_hours: int = 24) -> AlertSummaryResponse:
    return AlertSummaryResponse(**summarize_alerts(window_hours=window_hours))


@router.get("/alerts/sla", response_model=AlertSLAResponse)
def get_alerts_sla(window_hours: int = 24) -> AlertSLAResponse:
    return AlertSLAResponse(**summarize_alert_sla(window_hours=window_hours))


@router.get("/alerts/sla/history", response_model=list[AlertSLAHistoryPointResponse])
def get_alerts_sla_history(days: int = 14) -> list[AlertSLAHistoryPointResponse]:
    return [AlertSLAHistoryPointResponse(**row) for row in get_alert_sla_history(days=days)]


@router.get("/alerts/sla/breaches", response_model=AlertSLABreachInboxResponse)
def get_alerts_sla_breaches(
    days: int = 14,
    limit: int = 100,
    metric: str | None = None,
    severity: str | None = None,
) -> AlertSLABreachInboxResponse:
    try:
        payload = list_alert_sla_breaches(days=days, limit=limit, metric=metric, severity=severity)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    payload["events"] = [AlertSLABreachEventResponse(**row) for row in payload["events"]]
    return AlertSLABreachInboxResponse(**payload)


@router.post("/alerts/acknowledge", response_model=AlertAcknowledgeResponse)
def post_acknowledge_alert(request: AlertAcknowledgeRequest) -> AlertAcknowledgeResponse:
    try:
        return AlertAcknowledgeResponse(
            **acknowledge_alert(
                alert_id=request.alert_id,
                acknowledged_by=request.acknowledged_by,
                note=request.note,
            )
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/alerts/assign", response_model=AlertAssignResponse)
def post_assign_alert(request: AlertAssignRequest) -> AlertAssignResponse:
    try:
        return AlertAssignResponse(
            **assign_alert(
                alert_id=request.alert_id,
                assigned_to=request.assigned_to,
                assigned_by=request.assigned_by,
                priority=request.priority,
                due_by=request.due_by,
            )
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/ops/alerts/escalate/run", response_model=AlertEscalationRunResponse)
def post_alert_escalation_run(
    older_than_minutes: int = 60,
    limit: int = 50,
    _: None = Depends(require_ops_api_key),
) -> AlertEscalationRunResponse:
    return AlertEscalationRunResponse(
        **run_alert_escalation_scan(older_than_minutes=older_than_minutes, limit=limit)
    )


@router.post("/ops/alerts/sla/check", response_model=AlertSLABreachRunResponse)
def post_alert_sla_check(
    window_hours: int = 24,
    _: None = Depends(require_ops_api_key),
) -> AlertSLABreachRunResponse:
    return AlertSLABreachRunResponse(**run_alert_sla_breach_check(window_hours=window_hours))


@router.get("/exports/alerts.csv", response_class=PlainTextResponse)
def export_alerts_csv(limit: int = 1000) -> PlainTextResponse:
    rows = list_recent_alerts(limit=limit)
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "alert_id",
            "alert_type",
            "severity",
            "title",
            "message",
            "delivery_status",
            "is_acknowledged",
            "acknowledged_by",
            "ack_note",
            "acknowledged_at",
            "is_assigned",
            "assigned_to",
            "assigned_by",
            "assignment_priority",
            "assignment_due_by",
            "assigned_at",
            "created_at",
        ]
    )
    for row in rows:
        writer.writerow(
            [
                row.get("alert_id", ""),
                row.get("alert_type", ""),
                row.get("severity", ""),
                row.get("title", ""),
                row.get("message", ""),
                row.get("delivery_status", ""),
                row.get("is_acknowledged", False),
                row.get("acknowledged_by", ""),
                row.get("ack_note", ""),
                row.get("acknowledged_at", ""),
                row.get("is_assigned", False),
                row.get("assigned_to", ""),
                row.get("assigned_by", ""),
                row.get("assignment_priority", ""),
                row.get("assignment_due_by", ""),
                row.get("assigned_at", ""),
                row.get("created_at", ""),
            ]
        )
    return PlainTextResponse(
        content=output.getvalue(),
        headers={"Content-Disposition": 'attachment; filename="alerts.csv"'},
    )


@router.get("/exports/alerts_acknowledgements.csv", response_class=PlainTextResponse)
def export_alert_acknowledgements_csv(limit: int = 1000) -> PlainTextResponse:
    rows = [r for r in list_recent_alerts(limit=limit) if r.get("is_acknowledged")]
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(["alert_id", "acknowledged_by", "ack_note", "acknowledged_at"])
    for row in rows:
        writer.writerow(
            [
                row.get("alert_id", ""),
                row.get("acknowledged_by", ""),
                row.get("ack_note", ""),
                row.get("acknowledged_at", ""),
            ]
        )
    return PlainTextResponse(
        content=output.getvalue(),
        headers={"Content-Disposition": 'attachment; filename="alerts_acknowledgements.csv"'},
    )


@router.get("/exports/alerts_sla_breaches.csv", response_class=PlainTextResponse)
def export_alert_sla_breaches_csv(
    days: int = 14,
    limit: int = 1000,
    metric: str | None = None,
    severity: str | None = None,
) -> PlainTextResponse:
    try:
        payload = list_alert_sla_breaches(days=days, limit=limit, metric=metric, severity=severity)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    rows = payload["events"]
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "alert_id",
            "alert_type",
            "severity",
            "metric",
            "actual",
            "threshold",
            "window_hours",
            "message",
            "delivery_status",
            "created_at",
        ]
    )
    for row in rows:
        context = row.get("context") or {}
        writer.writerow(
            [
                row.get("alert_id", ""),
                row.get("alert_type", ""),
                row.get("severity", ""),
                context.get("metric", ""),
                context.get("actual", ""),
                context.get("threshold", ""),
                context.get("window_hours", ""),
                row.get("message", ""),
                row.get("delivery_status", ""),
                row.get("created_at", ""),
            ]
        )
    return PlainTextResponse(
        content=output.getvalue(),
        headers={"Content-Disposition": 'attachment; filename="alerts_sla_breaches.csv"'},
    )


@router.post("/ops/cleanup", response_model=OpsCleanupResponse)
def execute_cleanup(
    keep_last_runs: int = 20,
    keep_raw_files: int = 200,
    _: None = Depends(require_ops_api_key),
) -> OpsCleanupResponse:
    if keep_last_runs < 0 or keep_raw_files < 0:
        raise HTTPException(status_code=400, detail="keep_last_runs and keep_raw_files must be >= 0")
    return OpsCleanupResponse(**run_cleanup(keep_last_runs=keep_last_runs, keep_raw_files=keep_raw_files))


@router.post("/ops/pipeline/run", response_model=PipelineRunResponse)
def execute_pipeline_run(
    auto_accept_inference: bool = True,
    _: None = Depends(require_ops_api_key),
) -> PipelineRunResponse:
    return PipelineRunResponse(**run_pipeline_with_observability(auto_accept_inference=auto_accept_inference))


@router.get("/ops/runtime", response_model=RuntimeInfoResponse)
def get_runtime_diagnostics(_: None = Depends(require_ops_api_key)) -> RuntimeInfoResponse:
    return RuntimeInfoResponse(**get_runtime_info())

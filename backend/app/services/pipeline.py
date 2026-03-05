"""Pipeline orchestration service with observability metrics."""

from __future__ import annotations

import time
import uuid
from datetime import datetime, timezone

from app.services.drift import run_schema_drift_scan
from app.services.inference import (
    decide_relationship_candidate,
    list_relationship_candidates,
    run_relationship_inference,
)
from app.services.kpi import run_kpis, seed_kpi_registry
from app.services.lineage import build_lineage_graph
from app.services.validation import run_validation


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def run_pipeline_with_observability(auto_accept_inference: bool = True) -> dict:
    correlation_id = str(uuid.uuid4())
    started_at = _utc_now_iso()
    total_start = time.perf_counter()

    stage_metrics: list[dict] = []

    inf_start = time.perf_counter()
    inference = run_relationship_inference()
    stage_metrics.append(
        {
            "stage": "inference",
            "duration_ms": round((time.perf_counter() - inf_start) * 1000, 2),
            "details": {
                "inference_run_id": inference["inference_run_id"],
                "candidate_count": inference["candidate_count"],
            },
        }
    )

    accepted = 0
    if auto_accept_inference:
        acc_start = time.perf_counter()
        for cand in list_relationship_candidates(inference["inference_run_id"]):
            if cand["confidence_score"] >= 0.6 and cand["status"] != "ACCEPTED":
                decide_relationship_candidate(cand["candidate_id"], "ACCEPTED", "pipeline-observability")
                accepted += 1
        stage_metrics.append(
            {
                "stage": "inference_acceptance",
                "duration_ms": round((time.perf_counter() - acc_start) * 1000, 2),
                "details": {"accepted_count": accepted},
            }
        )

    val_start = time.perf_counter()
    validation = run_validation()
    stage_metrics.append(
        {
            "stage": "validation",
            "duration_ms": round((time.perf_counter() - val_start) * 1000, 2),
            "details": {
                "validation_run_id": validation["validation_run_id"],
                "trust_score": validation["trust_score"],
            },
        }
    )

    drift_start = time.perf_counter()
    drift = run_schema_drift_scan()
    stage_metrics.append(
        {
            "stage": "drift_scan",
            "duration_ms": round((time.perf_counter() - drift_start) * 1000, 2),
            "details": {"run_count": drift["run_count"], "total_events": drift["total_events"]},
        }
    )

    seed_start = time.perf_counter()
    seeded = seed_kpi_registry()
    stage_metrics.append(
        {
            "stage": "kpi_seed",
            "duration_ms": round((time.perf_counter() - seed_start) * 1000, 2),
            "details": {"inserted": seeded},
        }
    )

    kpi_start = time.perf_counter()
    kpi = run_kpis()
    stage_metrics.append(
        {
            "stage": "kpi_run",
            "duration_ms": round((time.perf_counter() - kpi_start) * 1000, 2),
            "details": {"kpi_run_id": kpi["kpi_run_id"]},
        }
    )

    lineage_start = time.perf_counter()
    lineage = build_lineage_graph()
    stage_metrics.append(
        {
            "stage": "lineage_build",
            "duration_ms": round((time.perf_counter() - lineage_start) * 1000, 2),
            "details": {
                "lineage_run_id": lineage["lineage_run_id"],
                "node_count": lineage["node_count"],
                "edge_count": lineage["edge_count"],
            },
        }
    )

    total_duration_ms = round((time.perf_counter() - total_start) * 1000, 2)
    ended_at = _utc_now_iso()

    return {
        "correlation_id": correlation_id,
        "started_at": started_at,
        "ended_at": ended_at,
        "total_duration_ms": total_duration_ms,
        "stage_metrics": stage_metrics,
        "summary": {
            "accepted_inference": accepted,
            "trust_score": validation["trust_score"],
            "kpi_run_id": kpi["kpi_run_id"],
            "lineage_run_id": lineage["lineage_run_id"],
        },
    }

from __future__ import annotations

from pathlib import Path

from app.services.drift import run_schema_drift_scan
from app.services.ingestion import ingest_file
from app.services.inference import list_relationship_candidates, run_relationship_inference, decide_relationship_candidate
from app.services.lineage import build_lineage_graph, get_lineage_graph
from app.services.validation import run_validation
from app.services.kpi import seed_kpi_registry, run_kpis, get_executive_dashboard


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    samples = root / "data" / "samples"

    ingest_order = [
        "customers.csv",
        "products.csv",
        "orders.csv",
        "order_items.csv",
        "inventory_snapshots.csv",
    ]

    print("[1/8] Ingesting sample datasets...")
    for name in ingest_order:
        result = ingest_file(name, (samples / name).read_bytes())
        print(f"  - {name}: dataset_id={result['dataset_id'][:8]}, rows={result['row_count']}")

    print("[2/8] Running relationship inference...")
    inf = run_relationship_inference()
    print(f"  - inference_run_id={inf['inference_run_id'][:8]}, candidates={inf['candidate_count']}")

    print("[3/8] Auto-accepting high-confidence candidates (>=0.60)...")
    accepted = 0
    for cand in list_relationship_candidates(inf["inference_run_id"]):
        if cand["confidence_score"] >= 0.60:
            decide_relationship_candidate(cand["candidate_id"], "ACCEPTED", "smoke-run")
            accepted += 1
    print(f"  - accepted={accepted}")

    print("[4/8] Running validation and trust score...")
    validation = run_validation()
    print(f"  - validation_run_id={validation['validation_run_id'][:8]}, trust_score={validation['trust_score']}")

    print("[5/8] Running schema drift scan...")
    drift = run_schema_drift_scan()
    print(f"  - drift_runs={drift['run_count']}, total_events={drift['total_events']}")

    print("[6/8] Seeding KPI registry and running KPI pipeline...")
    inserted = seed_kpi_registry()
    kpi_run = run_kpis()
    print(f"  - kpi_seeded={inserted}, kpi_run_id={kpi_run['kpi_run_id'][:8]}")

    print("[7/8] Building lineage graph...")
    lineage = build_lineage_graph()
    graph = get_lineage_graph(lineage["lineage_run_id"])
    print(f"  - lineage_run_id={lineage['lineage_run_id'][:8]}, nodes={len(graph['nodes'])}, edges={len(graph['edges'])}")

    print("[8/8] Building executive dashboard payload...")
    dashboard = get_executive_dashboard()
    print(f"  - cards={len(dashboard['cards'])}, has_trust_context={dashboard['trust_context'] is not None}")

    print("Smoke pipeline completed successfully.")


if __name__ == "__main__":
    main()

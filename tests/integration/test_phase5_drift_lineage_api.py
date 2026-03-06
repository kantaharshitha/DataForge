from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient
import pytest

import app.db as db_module
import app.services.ingestion as ingestion_module
from app.main import app
import run_migrations


@pytest.fixture()
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    test_db = tmp_path / "test_dataforge_phase5.duckdb"
    test_raw = tmp_path / "raw"
    test_raw.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(db_module, "DB_PATH", test_db)
    monkeypatch.setattr(ingestion_module, "RAW_DIR", test_raw)

    run_migrations.run_migrations()

    return TestClient(app)


def test_schema_drift_scan_detects_column_and_type_changes(client: TestClient) -> None:
    v1 = b"customer_id,customer_name,amount\nC001,Acme,10.0\n"
    v2 = b"customer_id,amount,promo_code\nC001,ten,NEW10\n"

    assert client.post("/upload", files={"file": ("customers.csv", v1, "text/csv")}).status_code == 200
    assert client.post("/upload", files={"file": ("customers.csv", v2, "text/csv")}).status_code == 200

    run = client.post("/drift/run", params={"dataset_name": "customers"})
    assert run.status_code == 200
    run_body = run.json()
    assert run_body["run_count"] >= 1

    latest = client.get("/drift/latest")
    assert latest.status_code == 200
    assert latest.json()["dataset_name"] == "customers"

    events = client.get("/drift/events/customers")
    assert events.status_code == 200
    change_types = {e["change_type"] for e in events.json()}
    assert "column_added" in change_types
    assert "column_removed" in change_types


def test_lineage_build_and_queries(client: TestClient) -> None:
    customers = b"customer_id,customer_name\nC001,Acme\nC002,Northwind\n"
    products = b"product_id,sku,unit_cost\nP001,SKU-1,10\nP002,SKU-2,15\n"
    orders = b"order_id,customer_id,order_date,ship_date\nO1001,C001,2026-02-01,2026-02-02\nO1002,C002,2026-02-03,2026-02-04\n"
    order_items = b"order_item_id,order_id,product_id,quantity,unit_price,discount_amount\nI1,O1001,P001,2,20,1\nI2,O1002,P002,1,30,0\n"
    inventory = b"snapshot_date,product_id,warehouse_id,stockout_flag\n2026-02-01,P001,W1,false\n2026-02-01,P002,W1,true\n"

    assert client.post("/upload", files={"file": ("customers.csv", customers, "text/csv")}).status_code == 200
    assert client.post("/upload", files={"file": ("products.csv", products, "text/csv")}).status_code == 200
    assert client.post("/upload", files={"file": ("orders.csv", orders, "text/csv")}).status_code == 200
    assert client.post("/upload", files={"file": ("order_items.csv", order_items, "text/csv")}).status_code == 200
    assert client.post("/upload", files={"file": ("inventory_snapshots.csv", inventory, "text/csv")}).status_code == 200

    assert client.post("/inference/run").status_code == 200
    for cand in client.get("/inference/candidates").json():
        if cand["confidence_score"] >= 0.6:
            client.post(
                "/inference/decide",
                json={"candidate_id": cand["candidate_id"], "decision": "ACCEPTED"},
            )

    assert client.post("/validation/run").status_code == 200
    assert client.post("/kpi/seed").status_code == 200
    assert client.post("/kpi/run").status_code == 200

    build = client.post("/lineage/build")
    assert build.status_code == 200
    build_body = build.json()
    assert build_body["node_count"] > 0
    assert build_body["edge_count"] > 0

    graph = client.get("/lineage/graph")
    assert graph.status_code == 200
    assert len(graph.json()["nodes"]) > 0

    by_kpi = client.get("/lineage/kpi/gross_revenue")
    assert by_kpi.status_code == 200
    assert len(by_kpi.json()["nodes"]) > 0

    by_dataset = client.get("/lineage/dataset/orders")
    assert by_dataset.status_code == 200
    assert len(by_dataset.json()["nodes"]) > 0


def test_export_endpoints_for_drift_validation_and_lineage(client: TestClient) -> None:
    v1 = b"customer_id,customer_name,amount\nC001,Acme,10.0\n"
    v2 = b"customer_id,amount,promo_code\nC001,ten,NEW10\n"
    assert client.post("/upload", files={"file": ("customers.csv", v1, "text/csv")}).status_code == 200
    assert client.post("/upload", files={"file": ("customers.csv", v2, "text/csv")}).status_code == 200
    assert client.post("/drift/run", params={"dataset_name": "customers"}).status_code == 200

    drift_export = client.get("/exports/drift/customers.csv")
    assert drift_export.status_code == 200
    assert "change_type" in drift_export.text

    orders = b"order_id,customer_id,order_date,ship_date\nO1001,C001,2026-02-01,2026-02-02\n"
    assert client.post("/upload", files={"file": ("orders.csv", orders, "text/csv")}).status_code == 200
    validation = client.post("/validation/run")
    assert validation.status_code == 200
    validation_run_id = validation.json()["validation_run_id"]

    validation_export = client.get(f"/exports/validation/{validation_run_id}.csv")
    assert validation_export.status_code == 200
    assert "rule_code" in validation_export.text

    build = client.post("/lineage/build")
    assert build.status_code == 200
    lineage_run_id = build.json()["lineage_run_id"]
    lineage_export = client.get(f"/exports/lineage/{lineage_run_id}.json")
    assert lineage_export.status_code == 200
    assert "nodes" in lineage_export.json()


def test_ops_cleanup_endpoint(client: TestClient) -> None:
    customers = b"customer_id,customer_name\nC001,Acme\n"
    assert client.post("/upload", files={"file": ("customers.csv", customers, "text/csv")}).status_code == 200
    assert client.post("/validation/run").status_code == 200
    assert client.post("/kpi/seed").status_code == 200
    assert client.post("/kpi/run").status_code == 200
    assert client.post("/lineage/build").status_code == 200
    assert client.post("/drift/run", params={"dataset_name": "customers"}).status_code == 200

    cleanup = client.post("/ops/cleanup", params={"keep_last_runs": 0, "keep_raw_files": 0})
    assert cleanup.status_code == 200
    payload = cleanup.json()
    assert payload["keep_last_runs"] == 0
    assert payload["keep_raw_files"] == 0
    assert "deleted" in payload


def test_ops_pipeline_run_returns_correlation_and_stage_timings(client: TestClient) -> None:
    customers = b"customer_id,customer_name\nC001,Acme\nC002,Northwind\n"
    products = b"product_id,sku,unit_cost\nP001,SKU-1,10\nP002,SKU-2,15\n"
    orders = b"order_id,customer_id,order_date,ship_date\nO1001,C001,2026-02-01,2026-02-02\nO1002,C002,2026-02-03,2026-02-04\n"
    order_items = b"order_item_id,order_id,product_id,quantity,unit_price,discount_amount\nI1,O1001,P001,2,20,1\nI2,O1002,P002,1,30,0\n"
    inventory = b"snapshot_date,product_id,warehouse_id,stockout_flag\n2026-02-01,P001,W1,false\n2026-02-01,P002,W1,true\n"

    assert client.post("/upload", files={"file": ("customers.csv", customers, "text/csv")}).status_code == 200
    assert client.post("/upload", files={"file": ("products.csv", products, "text/csv")}).status_code == 200
    assert client.post("/upload", files={"file": ("orders.csv", orders, "text/csv")}).status_code == 200
    assert client.post("/upload", files={"file": ("order_items.csv", order_items, "text/csv")}).status_code == 200
    assert client.post("/upload", files={"file": ("inventory_snapshots.csv", inventory, "text/csv")}).status_code == 200

    response = client.post("/ops/pipeline/run", params={"auto_accept_inference": True})
    assert response.status_code == 200
    payload = response.json()
    assert payload["correlation_id"]
    assert payload["total_duration_ms"] >= 0
    assert len(payload["stage_metrics"]) >= 6


def test_export_pipeline_bundle_by_correlation_id(client: TestClient) -> None:
    customers = b"customer_id,customer_name\nC001,Acme\nC002,Northwind\n"
    products = b"product_id,sku,unit_cost\nP001,SKU-1,10\nP002,SKU-2,15\n"
    orders = b"order_id,customer_id,order_date,ship_date\nO1001,C001,2026-02-01,2026-02-02\nO1002,C002,2026-02-03,2026-02-04\n"
    order_items = b"order_item_id,order_id,product_id,quantity,unit_price,discount_amount\nI1,O1001,P001,2,20,1\nI2,O1002,P002,1,30,0\n"
    inventory = b"snapshot_date,product_id,warehouse_id,stockout_flag\n2026-02-01,P001,W1,false\n2026-02-01,P002,W1,true\n"

    assert client.post("/upload", files={"file": ("customers.csv", customers, "text/csv")}).status_code == 200
    assert client.post("/upload", files={"file": ("products.csv", products, "text/csv")}).status_code == 200
    assert client.post("/upload", files={"file": ("orders.csv", orders, "text/csv")}).status_code == 200
    assert client.post("/upload", files={"file": ("order_items.csv", order_items, "text/csv")}).status_code == 200
    assert client.post("/upload", files={"file": ("inventory_snapshots.csv", inventory, "text/csv")}).status_code == 200

    run = client.post("/ops/pipeline/run", params={"auto_accept_inference": True})
    assert run.status_code == 200
    corr_id = run.json()["correlation_id"]

    bundle = client.get(f"/exports/run/{corr_id}.zip")
    assert bundle.status_code == 200
    assert bundle.headers["content-type"].startswith("application/zip")
    assert len(bundle.content) > 100


def test_ops_runtime_returns_runtime_and_db_info(client: TestClient) -> None:
    response = client.get("/ops/runtime")
    assert response.status_code == 200
    payload = response.json()
    assert payload["runtime_mode"] in {"local", "vercel-ephemeral", "persistent"}
    assert payload["is_vercel"] in {True, False}
    assert payload["db_path"]
    assert payload["db_exists"] is True


def test_ops_endpoints_require_api_key_when_configured(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DATAFORGE_OPS_API_KEY", "secret123")

    unauthorized = client.get("/ops/runtime")
    assert unauthorized.status_code == 401

    authorized = client.get("/ops/runtime", headers={"x-api-key": "secret123"})
    assert authorized.status_code == 200


def test_alerts_generated_for_trust_score_drop(client: TestClient) -> None:
    products_ok = b"product_id,sku,unit_cost\nP001,SKU-1,10\nP002,SKU-2,12\n"
    products_bad = b"product_id,sku,unit_cost\nP001,SKU-1,-10\nP002,SKU-2,-12\n"

    assert client.post("/upload", files={"file": ("products.csv", products_ok, "text/csv")}).status_code == 200
    assert client.post("/validation/run").status_code == 200

    assert client.post("/upload", files={"file": ("products.csv", products_bad, "text/csv")}).status_code == 200
    second = client.post("/validation/run")
    assert second.status_code == 200
    assert second.json()["trust_score"] < 100

    alerts = client.get("/alerts/recent", params={"limit": 20})
    assert alerts.status_code == 200
    alert_types = {a["alert_type"] for a in alerts.json()}
    assert "TRUST_SCORE_DROP" in alert_types or "TRUST_SCORE_LOW" in alert_types


def test_alerts_generated_for_high_severity_drift(client: TestClient) -> None:
    v1 = b"customer_id,customer_name,amount\nC001,Acme,10.0\n"
    v2 = b"customer_id,amount\nC001,ten\n"

    assert client.post("/upload", files={"file": ("customers.csv", v1, "text/csv")}).status_code == 200
    assert client.post("/upload", files={"file": ("customers.csv", v2, "text/csv")}).status_code == 200
    run = client.post("/drift/run", params={"dataset_name": "customers"})
    assert run.status_code == 200

    alerts = client.get("/alerts/recent", params={"limit": 20})
    assert alerts.status_code == 200
    high_drift = [
        a for a in alerts.json() if a["alert_type"] == "DRIFT_HIGH_SEVERITY" and a["severity"] == "HIGH"
    ]
    assert len(high_drift) >= 1

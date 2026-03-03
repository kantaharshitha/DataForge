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
    test_db = tmp_path / "test_dataforge_phase4.duckdb"
    test_raw = tmp_path / "raw"
    test_raw.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(db_module, "DB_PATH", test_db)
    monkeypatch.setattr(ingestion_module, "RAW_DIR", test_raw)

    run_migrations.run_migrations()

    return TestClient(app)


def test_kpi_registry_run_and_executive_dashboard(client: TestClient) -> None:
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

    infer = client.post("/inference/run")
    assert infer.status_code == 200
    for cand in client.get("/inference/candidates").json():
        if cand["confidence_score"] >= 0.6:
            client.post(
                "/inference/decide",
                json={
                    "candidate_id": cand["candidate_id"],
                    "decision": "ACCEPTED",
                    "reviewer_notes": "phase4",
                },
            )

    assert client.post("/validation/run").status_code == 200

    seed = client.post("/kpi/seed")
    assert seed.status_code == 200

    registry = client.get("/kpi/registry")
    assert registry.status_code == 200
    assert len(registry.json()) >= 10

    run = client.post("/kpi/run")
    assert run.status_code == 200
    run_body = run.json()
    assert run_body["status"] == "SUCCESS"
    assert "gross_revenue" in run_body["kpi_values"]

    latest = client.get("/kpi/latest")
    assert latest.status_code == 200
    latest_body = latest.json()
    assert latest_body["kpi_run_id"] == run_body["kpi_run_id"]

    dashboard = client.get("/dashboard/executive")
    assert dashboard.status_code == 200
    dash_body = dashboard.json()
    assert len(dash_body["cards"]) >= 1
    assert dash_body["trust_context"] is not None


def test_kpi_latest_without_runs_returns_404(client: TestClient) -> None:
    response = client.get("/kpi/latest")
    assert response.status_code == 404
    assert response.json()["detail"] == "No KPI runs found."

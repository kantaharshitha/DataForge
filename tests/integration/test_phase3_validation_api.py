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
    test_db = tmp_path / "test_dataforge_phase3.duckdb"
    test_raw = tmp_path / "raw"
    test_raw.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(db_module, "DB_PATH", test_db)
    monkeypatch.setattr(ingestion_module, "RAW_DIR", test_raw)

    run_migrations.run_migrations()

    return TestClient(app)


def test_validation_run_and_trust_endpoints(client: TestClient) -> None:
    customers = b"customer_id,customer_name\nC001,Acme\nC002,Northwind\n"
    orders = b"order_id,customer_id,order_date,ship_date\nO1001,C001,2026-02-01,2026-02-02\nO1002,C002,2026-02-03,2026-02-04\n"

    assert client.post("/upload", files={"file": ("customers.csv", customers, "text/csv")}).status_code == 200
    assert client.post("/upload", files={"file": ("orders.csv", orders, "text/csv")}).status_code == 200

    infer_run = client.post("/inference/run")
    assert infer_run.status_code == 200

    candidates = client.get("/inference/candidates").json()
    fk = [
        c for c in candidates
        if c["child_dataset_name"] == "orders" and c["child_column"] == "customer_id"
    ]
    assert fk

    accept = client.post(
        "/inference/decide",
        json={
            "candidate_id": fk[0]["candidate_id"],
            "decision": "ACCEPTED",
            "reviewer_notes": "phase3-test",
        },
    )
    assert accept.status_code == 200

    run = client.post("/validation/run")
    assert run.status_code == 200
    run_body = run.json()
    assert run_body["rule_count"] >= 1
    assert 0 <= run_body["trust_score"] <= 100
    assert "completeness" in run_body["dimension_scores"]

    latest = client.get("/trust/latest")
    assert latest.status_code == 200
    latest_body = latest.json()
    assert latest_body["validation_run_id"] == run_body["validation_run_id"]

    runs = client.get("/validation/runs")
    assert runs.status_code == 200
    assert len(runs.json()) >= 1

    details = client.get(f"/validation/results/{run_body['validation_run_id']}")
    assert details.status_code == 200
    details_body = details.json()
    assert len(details_body["results"]) >= 1


def test_validation_results_unknown_run_returns_404(client: TestClient) -> None:
    response = client.get("/validation/results/not-real-run")
    assert response.status_code == 404
    assert response.json()["detail"] == "Validation run not found."

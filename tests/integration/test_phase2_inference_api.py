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
    test_db = tmp_path / "test_dataforge_phase2.duckdb"
    test_raw = tmp_path / "raw"
    test_raw.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(db_module, "DB_PATH", test_db)
    monkeypatch.setattr(ingestion_module, "RAW_DIR", test_raw)

    run_migrations.run_migrations()

    return TestClient(app)


def test_run_inference_and_decide_candidate(client: TestClient) -> None:
    customers = b"customer_id,customer_name\nC001,Acme\nC002,Northwind\nC003,Blue\n"
    orders = b"order_id,customer_id\nO1001,C001\nO1002,C002\nO1003,C001\n"

    up1 = client.post("/upload", files={"file": ("customers.csv", customers, "text/csv")})
    assert up1.status_code == 200

    up2 = client.post("/upload", files={"file": ("orders.csv", orders, "text/csv")})
    assert up2.status_code == 200

    run = client.post("/inference/run")
    assert run.status_code == 200
    run_body = run.json()
    assert run_body["candidate_count"] >= 1

    candidates = client.get("/inference/candidates")
    assert candidates.status_code == 200
    rows = candidates.json()
    assert len(rows) >= 1

    target = None
    for row in rows:
        if (
            row["child_dataset_name"] == "orders"
            and row["child_column"] == "customer_id"
            and row["parent_dataset_name"] == "customers"
            and row["parent_column"] == "customer_id"
        ):
            target = row
            break

    assert target is not None
    assert target["confidence_score"] >= 0.6

    decide = client.post(
        "/inference/decide",
        json={
            "candidate_id": target["candidate_id"],
            "decision": "ACCEPTED",
            "reviewer_notes": "looks correct",
        },
    )
    assert decide.status_code == 200
    assert decide.json()["decision"] == "ACCEPTED"

    updated = client.get("/inference/candidates").json()
    chosen = [r for r in updated if r["candidate_id"] == target["candidate_id"]][0]
    assert chosen["status"] == "ACCEPTED"


def test_inference_invalid_decision_returns_400(client: TestClient) -> None:
    response = client.post(
        "/inference/decide",
        json={"candidate_id": "bad-id", "decision": "MAYBE"},
    )

    assert response.status_code == 400
    assert "Decision must be ACCEPTED or REJECTED" in response.json()["detail"]

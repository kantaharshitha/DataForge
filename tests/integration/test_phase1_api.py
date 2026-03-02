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
    test_db = tmp_path / "test_dataforge.duckdb"
    test_raw = tmp_path / "raw"
    test_raw.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(db_module, "DB_PATH", test_db)
    monkeypatch.setattr(ingestion_module, "RAW_DIR", test_raw)

    run_migrations.run_migrations()

    return TestClient(app)


def test_health(client: TestClient) -> None:
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_upload_then_list_and_profile(client: TestClient) -> None:
    csv_payload = b"customer_id,customer_name\nC001,Acme\nC002,Northwind\n"

    upload = client.post(
        "/upload",
        files={"file": ("customers.csv", csv_payload, "text/csv")},
    )
    assert upload.status_code == 200

    body = upload.json()
    assert body["dataset_name"] == "customers"
    assert body["row_count"] == 2
    assert body["schema_version"] == 1
    assert "customer_id" in body["key_candidates"]

    datasets = client.get("/datasets")
    assert datasets.status_code == 200
    dataset_rows = datasets.json()
    assert len(dataset_rows) == 1
    assert dataset_rows[0]["dataset_name"] == "customers"

    profile = client.get(f"/profiles/{body['dataset_id']}")
    assert profile.status_code == 200
    profile_body = profile.json()

    assert profile_body["dataset_name"] == "customers"
    assert profile_body["row_count"] == 2
    assert profile_body["column_count"] == 2
    assert profile_body["duplicate_rows"] == 0
    assert len(profile_body["columns"]) == 2


def test_upload_unsupported_extension_returns_400(client: TestClient) -> None:
    response = client.post(
        "/upload",
        files={"file": ("bad.json", b"{}", "application/json")},
    )

    assert response.status_code == 400
    assert "Unsupported file type" in response.json()["detail"]


def test_profile_missing_dataset_returns_404(client: TestClient) -> None:
    response = client.get("/profiles/not-a-real-dataset")

    assert response.status_code == 404
    assert response.json()["detail"] == "Profile run not found for dataset_id"

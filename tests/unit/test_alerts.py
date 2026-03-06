from __future__ import annotations

from pathlib import Path
import urllib.error

import pytest

import app.db as db_module
import app.services.alerts as alerts_module
import run_migrations


@pytest.fixture()
def setup_alert_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    test_db = tmp_path / "test_alerts.duckdb"
    monkeypatch.setattr(db_module, "DB_PATH", test_db)
    monkeypatch.setattr(db_module, "_SCHEMA_READY", False)
    run_migrations.run_migrations()


def test_emit_alert_skipped_when_webhook_not_configured(
    setup_alert_db: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("DATAFORGE_ALERT_WEBHOOK_URL", raising=False)

    payload = alerts_module.emit_alert(
        alert_type="TEST_ALERT",
        severity="LOW",
        title="Test",
        message="No webhook configured",
        context={"dataset_name": "orders"},
    )

    assert payload["delivery_status"] == "SKIPPED"
    recent = alerts_module.list_recent_alerts(limit=5)
    assert len(recent) == 1
    assert recent[0]["delivery_status"] == "SKIPPED"


def test_emit_alert_delivered_with_webhook_mock(
    setup_alert_db: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    class _DummyResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def _fake_urlopen(request, timeout=5):  # noqa: ARG001
        return _DummyResponse()

    monkeypatch.setenv("DATAFORGE_ALERT_WEBHOOK_URL", "https://example.test/hook")
    monkeypatch.setattr(alerts_module.urllib.request, "urlopen", _fake_urlopen)

    payload = alerts_module.emit_alert(
        alert_type="TEST_ALERT",
        severity="HIGH",
        title="Delivered",
        message="Webhook delivered",
        context={"dataset_name": "orders"},
    )
    assert payload["delivery_status"] == "DELIVERED"


def test_emit_alert_failed_with_webhook_error(
    setup_alert_db: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    def _failing_urlopen(request, timeout=5):  # noqa: ARG001
        raise urllib.error.URLError("network down")

    monkeypatch.setenv("DATAFORGE_ALERT_WEBHOOK_URL", "https://example.test/hook")
    monkeypatch.setattr(alerts_module.urllib.request, "urlopen", _failing_urlopen)

    payload = alerts_module.emit_alert(
        alert_type="TEST_ALERT",
        severity="HIGH",
        title="Failed",
        message="Webhook failure",
        context={"dataset_name": "orders"},
    )
    assert payload["delivery_status"] == "FAILED"
    assert "network down" in (payload["delivery_error"] or "")


def test_emit_alert_deduped_within_window(
    setup_alert_db: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("DATAFORGE_ALERT_WEBHOOK_URL", raising=False)
    monkeypatch.setenv("DATAFORGE_ALERT_DEDUP_MINUTES", "30")

    first = alerts_module.emit_alert(
        alert_type="TRUST_SCORE_DROP",
        severity="HIGH",
        title="Trust drop",
        message="first",
        context={"dataset_name": "orders"},
    )
    second = alerts_module.emit_alert(
        alert_type="TRUST_SCORE_DROP",
        severity="HIGH",
        title="Trust drop",
        message="second",
        context={"dataset_name": "orders"},
    )

    assert first["delivery_status"] in {"SKIPPED", "DELIVERED", "FAILED"}
    assert second["delivery_status"] == "DEDUPED"

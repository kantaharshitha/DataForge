from app.services.drift import diff_schema_versions


def test_diff_schema_versions_detects_all_drift_types() -> None:
    previous_schema = {
        "id": "int64",
        "name": "object",
        "amount": "float64",
    }
    current_schema = {
        "id": "int64",
        "amount": "object",
        "promo_code": "object",
    }

    events = diff_schema_versions(
        previous_schema=previous_schema,
        current_schema=current_schema,
        previous_key_candidates=["id"],
        current_key_candidates=["id", "promo_code"],
    )

    types = {event["change_type"] for event in events}
    assert "column_added" in types
    assert "column_removed" in types
    assert "type_changed" in types
    assert "key_candidates_changed" in types

    severity = {event["change_type"]: event["severity"] for event in events}
    assert severity["column_removed"] == "HIGH"
    assert severity["type_changed"] == "MEDIUM"
    assert severity["column_added"] == "LOW"

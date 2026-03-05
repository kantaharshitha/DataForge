from app.services.lineage import _dataset_dependencies_for_kpi


def test_dataset_dependencies_for_kpi_mapping() -> None:
    deps = _dataset_dependencies_for_kpi(["quantity", "unit_price", "order_id", "customer_id"])

    assert "order_items" in deps
    assert "orders" in deps

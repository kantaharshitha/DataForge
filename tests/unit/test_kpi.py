from app.services.kpi import _safe_div


def test_safe_div_handles_zero_denominator() -> None:
    assert _safe_div(10.0, 0.0) == 0.0


def test_safe_div_regular_division() -> None:
    assert _safe_div(9.0, 3.0) == 3.0

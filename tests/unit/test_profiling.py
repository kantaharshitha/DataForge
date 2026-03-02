from app.services.profiling import profile_dataframe


def test_profile_dataframe_basic():
    import pandas as pd

    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "value": ["a", "a", None],
        }
    )

    duplicate_rows, profiles, keys = profile_dataframe(df)

    assert duplicate_rows == 0
    assert len(profiles) == 2
    assert "id" in keys



def test_profile_dataframe_richer_metrics():
    import pandas as pd

    df = pd.DataFrame(
        {
            "order_id": ["O1", "O2", "O3"],
            "amount": [10.0, 20.0, 30.0],
        }
    )

    _, profiles, keys = profile_dataframe(df)
    amount = next(p for p in profiles if p.column_name == "amount")
    order_id = next(p for p in profiles if p.column_name == "order_id")

    assert amount.mean_value == 20.0
    assert amount.min_value == "10.0"
    assert amount.max_value == "30.0"
    assert amount.unique_pct == 100.0
    assert order_id.is_candidate_key is True
    assert "order_id" in keys

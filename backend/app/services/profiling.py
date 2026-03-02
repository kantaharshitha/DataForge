"""Core profiling logic for Phase 1."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass
class ColumnProfile:
    column_name: str
    inferred_type: str
    null_count: int
    non_null_count: int
    null_pct: float
    unique_pct: float
    distinct_count: int
    duplicate_value_count: int
    is_candidate_key: bool
    min_value: str | None
    max_value: str | None
    mean_value: float | None
    sample_values: list[str]


def candidate_keys(df: pd.DataFrame) -> list[str]:
    candidates: list[str] = []
    row_count = len(df)
    if row_count == 0:
        return candidates

    for col in df.columns:
        non_null = df[col].dropna()
        if non_null.empty:
            continue
        unique_ratio = non_null.nunique(dropna=True) / row_count
        null_ratio = df[col].isna().mean()
        if unique_ratio >= 0.99 and null_ratio <= 0.01:
            candidates.append(col)
    return candidates


def _safe_min_max(series: pd.Series) -> tuple[str | None, str | None]:
    cleaned = series.dropna()
    if cleaned.empty:
        return None, None

    try:
        return str(cleaned.min()), str(cleaned.max())
    except Exception:
        as_text = cleaned.astype(str)
        return str(as_text.min()), str(as_text.max())


def _safe_mean(series: pd.Series) -> float | None:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        return None
    return round(float(numeric.mean()), 4)


def profile_dataframe(df: pd.DataFrame) -> tuple[int, list[ColumnProfile], list[str]]:
    row_count = len(df)
    duplicate_rows = int(df.duplicated().sum())
    key_candidates = candidate_keys(df)
    candidate_set = set(key_candidates)
    profiles: list[ColumnProfile] = []

    for col in df.columns:
        null_count = int(df[col].isna().sum())
        non_null_count = int(row_count - null_count)
        null_pct = float((null_count / row_count) * 100) if row_count else 0.0
        distinct_count = int(df[col].nunique(dropna=True))
        unique_pct = float((distinct_count / non_null_count) * 100) if non_null_count else 0.0
        duplicate_value_count = int(max(0, non_null_count - distinct_count))
        sample_values = [str(v) for v in df[col].dropna().head(5).tolist()]
        min_value, max_value = _safe_min_max(df[col])

        profiles.append(
            ColumnProfile(
                column_name=col,
                inferred_type=str(df[col].dtype),
                null_count=null_count,
                non_null_count=non_null_count,
                null_pct=round(null_pct, 2),
                unique_pct=round(unique_pct, 2),
                distinct_count=distinct_count,
                duplicate_value_count=duplicate_value_count,
                is_candidate_key=col in candidate_set,
                min_value=min_value,
                max_value=max_value,
                mean_value=_safe_mean(df[col]),
                sample_values=sample_values,
            )
        )

    return duplicate_rows, profiles, key_candidates

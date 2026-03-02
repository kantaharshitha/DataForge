from app.services.inference import compute_confidence_score, infer_cardinality_hint


def test_compute_confidence_score_weighted() -> None:
    score = compute_confidence_score(
        overlap_ratio=0.9,
        parent_coverage_ratio=0.8,
        name_score=1.0,
        type_score=1.0,
    )

    assert score == 0.91


def test_infer_cardinality_hint_rules() -> None:
    assert infer_cardinality_hint(parent_unique=True, child_unique=True) == "one_to_one"
    assert infer_cardinality_hint(parent_unique=True, child_unique=False) == "one_to_many"
    assert infer_cardinality_hint(parent_unique=False, child_unique=True) == "many_to_one"
    assert infer_cardinality_hint(parent_unique=False, child_unique=False) == "many_to_many"

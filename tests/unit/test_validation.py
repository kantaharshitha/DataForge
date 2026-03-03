from app.services.validation import compute_trust_score_from_rules


def test_compute_trust_score_from_rules_weighted_dimensions() -> None:
    rules = [
        {"dimension": "completeness", "penalty_points": 0.10},
        {"dimension": "integrity", "penalty_points": 0.20},
        {"dimension": "conformance", "penalty_points": 0.00},
        {"dimension": "temporal", "penalty_points": 0.05},
        {"dimension": "drift", "penalty_points": 0.00},
    ]

    dimension_scores, trust_score = compute_trust_score_from_rules(rules)

    assert dimension_scores["completeness"] == 90.0
    assert dimension_scores["integrity"] == 80.0
    assert dimension_scores["temporal"] == 95.0
    assert trust_score == 90

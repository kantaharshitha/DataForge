CREATE TABLE IF NOT EXISTS relationship_candidates (
    candidate_id TEXT PRIMARY KEY,
    inference_run_id TEXT NOT NULL,
    child_dataset_id TEXT NOT NULL,
    child_dataset_name TEXT NOT NULL,
    child_column TEXT NOT NULL,
    parent_dataset_id TEXT NOT NULL,
    parent_dataset_name TEXT NOT NULL,
    parent_column TEXT NOT NULL,
    overlap_ratio DOUBLE NOT NULL,
    parent_coverage_ratio DOUBLE NOT NULL,
    name_score DOUBLE NOT NULL,
    type_score DOUBLE NOT NULL,
    confidence_score DOUBLE NOT NULL,
    cardinality_hint TEXT NOT NULL,
    status TEXT NOT NULL,
    rationale TEXT,
    evidence_json TEXT,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS relationship_decisions (
    decision_id TEXT PRIMARY KEY,
    candidate_id TEXT NOT NULL,
    decision TEXT NOT NULL,
    reviewer_notes TEXT,
    decided_at TIMESTAMP NOT NULL
);

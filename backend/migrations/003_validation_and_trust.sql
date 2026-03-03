CREATE TABLE IF NOT EXISTS validation_runs (
    validation_run_id TEXT PRIMARY KEY,
    started_at TIMESTAMP NOT NULL,
    ended_at TIMESTAMP NOT NULL,
    status TEXT NOT NULL,
    trust_score INTEGER NOT NULL,
    dimension_scores_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS validation_results (
    result_id TEXT PRIMARY KEY,
    validation_run_id TEXT NOT NULL,
    dimension TEXT NOT NULL,
    rule_code TEXT NOT NULL,
    dataset_name TEXT NOT NULL,
    severity TEXT NOT NULL,
    base_weight DOUBLE NOT NULL,
    evaluated_records BIGINT NOT NULL,
    failed_records BIGINT NOT NULL,
    failure_rate DOUBLE NOT NULL,
    penalty_points DOUBLE NOT NULL,
    message TEXT NOT NULL,
    sample_rows_json TEXT
);

CREATE TABLE IF NOT EXISTS validation_exceptions (
    exception_id TEXT PRIMARY KEY,
    validation_run_id TEXT NOT NULL,
    result_id TEXT NOT NULL,
    dataset_name TEXT NOT NULL,
    rule_code TEXT NOT NULL,
    sample_rows_json TEXT,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_event_log (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    payload_json TEXT,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS dataset_registry (
    dataset_id TEXT PRIMARY KEY,
    dataset_name TEXT NOT NULL,
    source_file TEXT NOT NULL,
    ingested_at TIMESTAMP NOT NULL,
    row_count BIGINT NOT NULL,
    file_hash TEXT NOT NULL,
    status TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS schema_versions (
    version_id TEXT PRIMARY KEY,
    dataset_name TEXT NOT NULL,
    version_no INTEGER NOT NULL,
    created_at TIMESTAMP NOT NULL,
    schema_json TEXT NOT NULL,
    key_candidates_json TEXT
);

CREATE TABLE IF NOT EXISTS profiling_runs (
    run_id TEXT PRIMARY KEY,
    dataset_id TEXT NOT NULL,
    dataset_name TEXT NOT NULL,
    run_at TIMESTAMP NOT NULL,
    row_count BIGINT NOT NULL,
    column_count INTEGER NOT NULL,
    duplicate_rows BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS profiling_results (
    result_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    column_name TEXT NOT NULL,
    inferred_type TEXT NOT NULL,
    null_count BIGINT NOT NULL,
    non_null_count BIGINT NOT NULL,
    null_pct DOUBLE NOT NULL,
    unique_pct DOUBLE NOT NULL,
    distinct_count BIGINT NOT NULL,
    duplicate_value_count BIGINT NOT NULL,
    is_candidate_key BOOLEAN NOT NULL,
    min_value TEXT,
    max_value TEXT,
    mean_value DOUBLE,
    sample_values_json TEXT
);

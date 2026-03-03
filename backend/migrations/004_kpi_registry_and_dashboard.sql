CREATE TABLE IF NOT EXISTS kpi_registry (
    kpi_id TEXT PRIMARY KEY,
    kpi_code TEXT UNIQUE NOT NULL,
    kpi_name TEXT NOT NULL,
    definition TEXT NOT NULL,
    formula TEXT NOT NULL,
    required_fields_json TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS kpi_run_log (
    kpi_run_id TEXT PRIMARY KEY,
    validation_run_id TEXT,
    generated_at TIMESTAMP NOT NULL,
    status TEXT NOT NULL,
    kpi_values_json TEXT NOT NULL,
    metadata_json TEXT
);

CREATE TABLE IF NOT EXISTS sla_breach_runs (
    run_id TEXT PRIMARY KEY,
    window_hours INTEGER NOT NULL,
    thresholds_json TEXT,
    sla_json TEXT,
    breach_count INTEGER NOT NULL,
    suppressed_count INTEGER NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sla_breach_runs_created_at
ON sla_breach_runs (created_at DESC);

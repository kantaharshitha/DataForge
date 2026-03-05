CREATE TABLE IF NOT EXISTS pipeline_run_log (
    correlation_id TEXT PRIMARY KEY,
    started_at TIMESTAMP NOT NULL,
    ended_at TIMESTAMP NOT NULL,
    total_duration_ms DOUBLE NOT NULL,
    summary_json TEXT
);

CREATE TABLE IF NOT EXISTS pipeline_stage_metrics (
    metric_id TEXT PRIMARY KEY,
    correlation_id TEXT NOT NULL,
    stage_order INTEGER NOT NULL,
    stage_name TEXT NOT NULL,
    duration_ms DOUBLE NOT NULL,
    details_json TEXT,
    created_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pipeline_stage_metrics_corr
ON pipeline_stage_metrics (correlation_id, stage_order);

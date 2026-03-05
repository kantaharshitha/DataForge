CREATE TABLE IF NOT EXISTS schema_drift_runs (
    drift_run_id TEXT PRIMARY KEY,
    dataset_name TEXT NOT NULL,
    from_version INTEGER,
    to_version INTEGER,
    run_at TIMESTAMP NOT NULL,
    event_count INTEGER NOT NULL,
    high_count INTEGER NOT NULL,
    medium_count INTEGER NOT NULL,
    low_count INTEGER NOT NULL,
    status TEXT NOT NULL,
    summary_json TEXT
);

CREATE TABLE IF NOT EXISTS schema_drift_events (
    event_id TEXT PRIMARY KEY,
    drift_run_id TEXT NOT NULL,
    dataset_name TEXT NOT NULL,
    change_type TEXT NOT NULL,
    column_name TEXT,
    old_value TEXT,
    new_value TEXT,
    severity TEXT NOT NULL,
    details_json TEXT,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS lineage_runs (
    lineage_run_id TEXT PRIMARY KEY,
    run_at TIMESTAMP NOT NULL,
    status TEXT NOT NULL,
    source_context_json TEXT
);

CREATE TABLE IF NOT EXISTS lineage_nodes (
    node_id TEXT PRIMARY KEY,
    node_type TEXT NOT NULL,
    node_key TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL,
    metadata_json TEXT,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS lineage_edges (
    edge_id TEXT PRIMARY KEY,
    lineage_run_id TEXT NOT NULL,
    from_node_id TEXT NOT NULL,
    to_node_id TEXT NOT NULL,
    edge_type TEXT NOT NULL,
    metadata_json TEXT,
    created_at TIMESTAMP NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_lineage_edge_unique
ON lineage_edges (lineage_run_id, from_node_id, to_node_id, edge_type);

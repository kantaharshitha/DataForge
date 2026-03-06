CREATE TABLE IF NOT EXISTS alert_acknowledgements (
    ack_id TEXT PRIMARY KEY,
    alert_id TEXT NOT NULL,
    acknowledged_by TEXT NOT NULL,
    note TEXT,
    acknowledged_at TIMESTAMP NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_alert_ack_one_per_alert
ON alert_acknowledgements (alert_id);

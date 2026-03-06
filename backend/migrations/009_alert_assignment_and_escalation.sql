CREATE TABLE IF NOT EXISTS alert_assignments (
    assignment_id TEXT PRIMARY KEY,
    alert_id TEXT NOT NULL,
    assigned_to TEXT NOT NULL,
    assigned_by TEXT NOT NULL,
    priority TEXT NOT NULL,
    due_by TIMESTAMP,
    assigned_at TIMESTAMP NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_alert_assignment_one_per_alert
ON alert_assignments (alert_id);

CREATE TABLE IF NOT EXISTS alert_escalations (
    escalation_id TEXT PRIMARY KEY,
    alert_id TEXT NOT NULL,
    emitted_alert_id TEXT,
    reason TEXT NOT NULL,
    source_age_minutes INTEGER NOT NULL,
    escalated_at TIMESTAMP NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_alert_escalation_one_per_alert
ON alert_escalations (alert_id);

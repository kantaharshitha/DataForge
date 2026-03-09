# Changelog

## v1.5.1-alert-sla-inbox-filters
Date: 2026-03-09

### Added
- SLA breach inbox API filters:
  - `metric` and `severity` query params on `/alerts/sla/breaches`.
- SLA breach CSV export:
  - `/exports/alerts_sla_breaches.csv` with optional `days`, `limit`, `metric`, `severity`.
- Frontend breach inbox controls:
  - Days/limit/metric/severity filters.
  - CSV export button for filtered breach rows.

### Quality
- Integration coverage for filtered breach inbox responses.
- Integration coverage for SLA breach CSV export.

## v1.4-alerting-hooks
Date: 2026-03-06

### Added
- Phase 6 alerting hooks:
  - Trust-score drop alert events from validation runs.
  - High-severity schema drift alert events from drift scans.
- Alerts API:
  - `/alerts/recent?limit=50`
- Alert persistence:
  - `alert_events` table and migration `007_alerting.sql`.
- Alert dedup/rate-limit:
  - Suppresses duplicate alerts by `alert_type + dataset` within configurable window.
- Frontend ops-auth badge:
  - Shows enabled/disabled/unreachable status based on `/ops/runtime`.
- Frontend alerts panel:
  - Recent alerts table with severity/delivery filters and pagination.
  - Alerts summary stats view.
- Alerts summary API:
  - `/alerts/summary?window_hours=24`
- Alert acknowledgement:
  - `POST /alerts/acknowledge`
  - `alert_acknowledgements` storage with acknowledged_by and note.
- Alert assignment + escalation:
  - `POST /alerts/assign`
  - `POST /ops/alerts/escalate/run`
  - `alert_assignments` and `alert_escalations` storage.
- Alert audit exports:
  - `/exports/alerts.csv`
  - `/exports/alerts_acknowledgements.csv`
- Alert SLA metrics:
  - `/alerts/sla?window_hours=24`
  - Frontend SLA cards for open high alerts, MTTA, and escalations/day.
- Scheduled escalation workflow:
  - `.github/workflows/nightly_alert_escalation.yml`
- SLA breach detection:
  - `POST /ops/alerts/sla/check?window_hours=24`
  - Threshold-driven breach alerts for open high, MTTA, and escalations/day.
  - Added metric+day dedup guard to prevent repeated breach spam in same day.
- Scheduled SLA workflow:
  - `.github/workflows/nightly_alert_sla_check.yml`
- SLA history:
  - `/alerts/sla/history?days=14`
  - Frontend trend table for acknowledged alerts, MTTA, escalations, and SLA breaches.
- SLA breach inbox:
  - `/alerts/sla/breaches?days=14&limit=50`
  - Includes recent breach events and `suppressed_last_24h`.

### Quality
- Integration tests for trust-drop and drift-high alert scenarios.
- Unit tests for alert webhook paths (`SKIPPED`, `DELIVERED`, `FAILED`) and dedup behavior.

## v1.3-ops-observability
Date: 2026-03-05

### Added
- Ops security guard:
  - Optional API key requirement for `/ops/*` using `DATAFORGE_OPS_API_KEY` and `x-api-key`.
- Pipeline observability persistence:
  - `pipeline_run_log` and `pipeline_stage_metrics` storage for correlation-based audit tracking.
- Audit artifact bundle export:
  - `/exports/run/{correlation_id}.zip` with manifest, stage metrics, drift events, and available lineage/validation payloads.
- Retention scheduler utility:
  - `scripts/nightly_cleanup.py` for daily cleanup automation.
- Frontend bundle workflow:
  - Correlation ID field and `Download Run Bundle (.zip)` action in the observability panel.

### Quality
- Integration coverage for bundle export and ops API-key authorization behavior.
- Full local test suite passing.

## v1.2-governance-observability
Date: 2026-03-05

### Added
- Phase 5 governance capabilities:
  - Schema drift detection service and APIs.
  - Lineage graph build/query services and APIs.
- Vercel-ready deployment structure:
  - `api/` serverless FastAPI entrypoint.
  - `frontend/` static web console.
- Governance exports:
  - Drift CSV export.
  - Validation CSV export.
  - Lineage JSON export.
- Ops and observability:
  - Cleanup retention API (`/ops/cleanup`).
  - Pipeline run API with correlation ID and stage timings (`/ops/pipeline/run`).
- CI workflow:
  - GitHub Actions test pipeline for push/PR on main.

### Changed
- Frontend explorer enhanced with:
  - Drift severity filtering + pagination.
  - Lineage edge-type filtering + pagination.
  - Export controls.
- Smoke pipeline expanded to include drift and lineage stages.

### Quality
- Test suite expanded to include Phase 5 and ops endpoints.
- Full suite passing in local validation.

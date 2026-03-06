# DataForge v1.5-alert-operations

Release tag: `v1.5-alert-operations`

## Highlights
- Alert operations workflow completed:
  - Acknowledge alerts (`/alerts/acknowledge`)
  - Assign alerts (`/alerts/assign`)
  - Escalate unacknowledged high alerts (`/ops/alerts/escalate/run`)
- Alert analytics and visibility:
  - Alerts summary endpoint (`/alerts/summary`)
  - Alerts panel summary cards and operational controls in frontend
- Alert governance exports:
  - `/exports/alerts.csv`
  - `/exports/alerts_acknowledgements.csv`

## Required Runtime Configuration
- `DATAFORGE_OPS_API_KEY` (recommended)
- `DATAFORGE_ALERT_WEBHOOK_URL` (for live delivery)
- `DATAFORGE_ALERT_DEDUP_MINUTES` (default `30`)
- `DATAFORGE_ESCALATION_MINUTES` (default `60`)

## Verification Checklist
- `GET /api/alerts/summary?window_hours=24` returns counts.
- `POST /api/alerts/assign` stores assignment fields in `/api/alerts/recent`.
- `POST /api/ops/alerts/escalate/run` emits `ALERT_ESCALATED` when applicable.
- CSV exports return expected columns:
  - `alerts.csv`
  - `alerts_acknowledgements.csv`
- Webhook-enabled alerts show `delivery_status=DELIVERED` in `/api/alerts/recent`.

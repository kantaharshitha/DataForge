# DataForge (Vercel-Ready Architecture)

DataForge is a local-first enterprise analytics platform simulation.

## Architecture (Updated)
- `backend/`: core business logic and FastAPI routes
- `api/index.py`: Vercel serverless entrypoint for FastAPI
- `frontend/`: static web console (Vercel static hosting)
- `ui/`: Streamlit console (kept for local/internal use)
- `backend/migrations/`: schema migrations auto-applied by API startup

Why this change:
- Streamlit is not a good primary deployment target for Vercel.
- Vercel deployment now uses static frontend + serverless Python API.

## Key Runtime Notes for Vercel
- DB defaults to `/tmp/dataforge.duckdb` on Vercel (`VERCEL=1`).
- Uploaded raw files default to `/tmp/dataforge_raw` on Vercel.
- This is suitable for simulation/demo, not durable production persistence.
- Runtime mode can be set with `DATAFORGE_RUNTIME_MODE`:
  - `local` (default outside Vercel)
  - `vercel-ephemeral` (default on Vercel)
  - `persistent` (force project `db/` path if writable)
- Optional ops API guard: set `DATAFORGE_OPS_API_KEY` and pass it via `x-api-key` for `/ops/*`.
- Optional alert webhook:
  - `DATAFORGE_ALERT_WEBHOOK_URL`
  - `DATAFORGE_ALERT_TRUST_DROP` (default `10`)
  - `DATAFORGE_ALERT_TRUST_FLOOR` (default `80`)
  - `DATAFORGE_ALERT_DEDUP_MINUTES` (default `30`)
  - `DATAFORGE_ESCALATION_MINUTES` (default `60`)
  - `DATAFORGE_SLA_MAX_OPEN_HIGH` (default `5`)
  - `DATAFORGE_SLA_MAX_MTTA_MINUTES` (default `60`)
  - `DATAFORGE_SLA_MAX_ESCALATIONS_PER_DAY` (default `3`)

## Local Run (Recommended)
1. Setup:
```powershell
./setup_phase1.ps1
```

2. Start API:
```powershell
.\.venv\Scripts\python.exe -m uvicorn app.main:app --app-dir backend --host 127.0.0.1 --port 8000
```

3. Open frontend (simple option):
```powershell
.\.venv\Scripts\python.exe -m http.server 3000 --directory frontend
```
Then open `http://127.0.0.1:3000` and set API Base to `http://127.0.0.1:8000`.

4. Optional Streamlit UI (legacy local console):
```powershell
.\.venv\Scripts\python.exe -m streamlit run ui/app.py
```

## Deploy to Vercel
Project already includes:
- `vercel.json`
- `api/index.py`
- `api/requirements.txt`
- `frontend/index.html`

Deploy:
1. Push repo to GitHub.
2. Import project in Vercel.
3. Deploy without custom build command.
4. Open deployed URL.

In production, frontend calls API via `/api/*` automatically.

Before production verification, set these Vercel environment variables:
- `DATAFORGE_RUNTIME_MODE=vercel-ephemeral`
- `DATAFORGE_OPS_API_KEY=<strong-random-value>`

## API Endpoints
- Upload/Profile: `/health`, `/upload`, `/datasets`, `/profiles/{dataset_id}`
- Inference: `/inference/run`, `/inference/candidates`, `/inference/decide`
- Drift: `/drift/run`, `/drift/runs`, `/drift/latest`, `/drift/events/{dataset_name}`
- Lineage: `/lineage/build`, `/lineage/runs`, `/lineage/graph`, `/lineage/kpi/{kpi_code}`, `/lineage/dataset/{dataset_name}`
- Data model explorer: `/model/er`
- Validation/Trust: `/validation/run`, `/validation/runs`, `/validation/results/{validation_run_id}`, `/trust/latest`
- KPI/Dashboard: `/kpi/seed`, `/kpi/registry`, `/kpi/run`, `/kpi/latest`, `/dashboard/executive`
- Alerts: `/alerts/recent?limit=50`
- Alerts summary: `/alerts/summary?window_hours=24`
- Alerts SLA: `/alerts/sla?window_hours=24`
- Alerts SLA history: `/alerts/sla/history?days=14`
- Alerts SLA breaches inbox: `/alerts/sla/breaches?days=14&limit=50&metric=open_high_alerts&severity=HIGH`
- Alerts SLA check (ops): `POST /ops/alerts/sla/check?window_hours=24`
- SLA breach dedup guard: one breach alert per metric per day.
- Alerts acknowledge: `POST /alerts/acknowledge`
- Alerts assign: `POST /alerts/assign`
- Exports: `/exports/drift/{dataset_name}.csv`, `/exports/validation/{validation_run_id}.csv`, `/exports/lineage/{lineage_run_id}.json`
- Export bundle: `/exports/run/{correlation_id}.zip`
- Alert exports: `/exports/alerts.csv`, `/exports/alerts_acknowledgements.csv`, `/exports/alerts_sla_breaches.csv`
- Ops: `/ops/cleanup?keep_last_runs=20&keep_raw_files=200`
- Ops pipeline observability: `/ops/pipeline/run?auto_accept_inference=true`
- Ops runtime diagnostics: `/ops/runtime`
- Ops alerts escalation: `/ops/alerts/escalate/run?older_than_minutes=60&limit=50`
- Ops alerts SLA check: `/ops/alerts/sla/check?window_hours=24`

## Validation and Smoke Tests
Run all tests:
```powershell
./scripts/run_tests.ps1
```

Run full pipeline smoke flow:
```powershell
$env:PYTHONPATH="./backend"
.\.venv\Scripts\python.exe .\scripts\smoke_pipeline.py
```
This now executes: ingest -> inference -> validation -> drift scan -> KPI -> lineage build -> dashboard.

Run retention cleanup manually:
```powershell
$env:PYTHONPATH="./backend"
.\.venv\Scripts\python.exe .\scripts\nightly_cleanup.py --keep-last-runs 20 --keep-raw-files 200
```
Use Windows Task Scheduler to run this command daily.

Automated cleanup is also available with GitHub Actions:
- Workflow: `.github/workflows/nightly_cleanup.yml`
- Required repository secrets:
  - `DATAFORGE_BASE_URL` (example: `https://your-app.vercel.app/api`)
  - `DATAFORGE_OPS_API_KEY`

Automated escalation scan is available with GitHub Actions:
- Workflow: `.github/workflows/nightly_alert_escalation.yml`
- Required repository secrets:
  - `DATAFORGE_BASE_URL`
  - `DATAFORGE_OPS_API_KEY`
  - Optional: `DATAFORGE_ESCALATION_MINUTES` (defaults to `60`)

Automated SLA check is available with GitHub Actions:
- Workflow: `.github/workflows/nightly_alert_sla_check.yml`
- Required repository secrets:
  - `DATAFORGE_BASE_URL`
  - `DATAFORGE_OPS_API_KEY`
  - Optional: `DATAFORGE_SLA_WINDOW_HOURS` (defaults to `24`)

Deployment verification automation:
```powershell
.\scripts\verify_deployment.ps1 -BaseUrl "https://your-app.vercel.app/api" -OpsApiKey "<DATAFORGE_OPS_API_KEY>"
```

Phase 6 production alert verification:
```powershell
.\scripts\verify_phase6_production.ps1 -BaseUrl "https://your-app.vercel.app/api" -OpsApiKey "<DATAFORGE_OPS_API_KEY>" -EvidenceFile "phase6_production_verification.json"
```

Manual workflow dispatch helper (GitHub API):
```powershell
$env:GITHUB_TOKEN="<token>"
.\scripts\trigger_cleanup_workflow.ps1 -Repo "kantaharshitha/DataForge" -Ref "main"
```

## Frontend Explorer Features
- Drift explorer with severity filter and pagination.
- Lineage explorer with edge-type filter and pagination.
- Export buttons for drift CSV and lineage JSON.
- Validation results CSV export by run ID.
- Pipeline artifact bundle export by correlation ID (`/exports/run/{correlation_id}.zip`).
- Vercel deployment warning banner when running on `*.vercel.app`.
- Pipeline observability response with correlation ID and stage-level durations.
- Runtime diagnostics and DB-path visibility from `/ops/runtime`.
- Alerts panel with severity/delivery filters and pagination.
- Alert acknowledgement workflow (ack by + note).
- Alert assignment and manual escalation scan controls.
- Alert SLA cards (open high alerts, MTTA, escalations/day).
- Manual SLA breach-check trigger from frontend (`Run SLA Check`).
- SLA history trend table from frontend (`Load SLA History`).
- SLA breach inbox table with suppressed count metadata (`Load Breach Inbox`).
- SLA breach inbox filters (days, limit, metric, severity) and breach CSV export.
- Executive Dashboard visual panel (KPI cards + trust/status chips + KPI table).
- ER Model explorer with Cytoscape graph (tables/relationships) and table-column side panel on node click.

## Operations
- Runbook: `OPERATIONS.md`
- Environment template: `.env.example`
- Release notes: `RELEASE_NOTES_v1.3-ops-observability.md`
- Release notes (alert operations): `RELEASE_NOTES_v1.5-alert-operations.md`

Create GitHub release via API token:
```powershell
$env:GITHUB_TOKEN="<token>"
.\scripts\create_github_release.ps1 -Repo "kantaharshitha/DataForge" -Tag "v1.5-alert-operations" -Title "DataForge v1.5-alert-operations" -NotesFile ".\RELEASE_NOTES_v1.5-alert-operations.md"
```

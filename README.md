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

## API Endpoints
- Upload/Profile: `/health`, `/upload`, `/datasets`, `/profiles/{dataset_id}`
- Inference: `/inference/run`, `/inference/candidates`, `/inference/decide`
- Drift: `/drift/run`, `/drift/runs`, `/drift/latest`, `/drift/events/{dataset_name}`
- Lineage: `/lineage/build`, `/lineage/runs`, `/lineage/graph`, `/lineage/kpi/{kpi_code}`, `/lineage/dataset/{dataset_name}`
- Validation/Trust: `/validation/run`, `/validation/runs`, `/validation/results/{validation_run_id}`, `/trust/latest`
- KPI/Dashboard: `/kpi/seed`, `/kpi/registry`, `/kpi/run`, `/kpi/latest`, `/dashboard/executive`
- Exports: `/exports/drift/{dataset_name}.csv`, `/exports/validation/{validation_run_id}.csv`, `/exports/lineage/{lineage_run_id}.json`
- Ops: `/ops/cleanup?keep_last_runs=20&keep_raw_files=200`
- Ops pipeline observability: `/ops/pipeline/run?auto_accept_inference=true`
- Ops runtime diagnostics: `/ops/runtime`

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

## Frontend Explorer Features
- Drift explorer with severity filter and pagination.
- Lineage explorer with edge-type filter and pagination.
- Export buttons for drift CSV and lineage JSON.
- Validation results CSV export by run ID.
- Vercel deployment warning banner when running on `*.vercel.app`.
- Pipeline observability response with correlation ID and stage-level durations.
- Runtime diagnostics and DB-path visibility from `/ops/runtime`.

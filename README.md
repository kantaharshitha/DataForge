# DataForge (Phase 4)

DataForge is a local-first enterprise analytics platform simulation.

Implemented capabilities:
- Upload and ingest 3-5 CSV/XLSX datasets
- Profiling (null %, duplicates, inferred types, candidate keys)
- Deterministic relationship inference + admin accept/reject
- Validation engine with dimension-based rules and Data Trust Score
- KPI registry seeding + deterministic KPI execution
- Executive dashboard payload generation with trust context

## Stack
- FastAPI
- DuckDB
- Streamlit
- pytest

## Project Structure
- `backend/` API, services, migrations
- `ui/` Streamlit console
- `data/samples/` canonical demo datasets
- `scripts/` test and smoke-run utilities
- `tests/` unit + integration

## Quick Start
1. Set up environment and dependencies:
```powershell
./setup_phase1.ps1
```

2. Start API (terminal 1):
```powershell
.\.venv\Scripts\python.exe -m uvicorn backend.app.main:app --reload
```

3. Start UI (terminal 2):
```powershell
.\.venv\Scripts\python.exe -m streamlit run ui/app.py
```

4. Open UI at:
- `http://localhost:8501`

## Recommended Flow (UI)
1. Upload datasets (`customers`, `products`, `orders`, `order_items`, `inventory_snapshots`)
2. Run Relationship Inference and accept valid candidates
3. Run Validation and review trust score
4. Seed KPI registry and run KPI calculation
5. Open Executive Dashboard

## API Endpoints (Core)
- Upload/Profile: `/upload`, `/datasets`, `/profiles/{dataset_id}`
- Inference: `/inference/run`, `/inference/candidates`, `/inference/decide`
- Validation/Trust: `/validation/run`, `/validation/runs`, `/validation/results/{validation_run_id}`, `/trust/latest`
- KPI/Dashboard: `/kpi/seed`, `/kpi/registry`, `/kpi/run`, `/kpi/latest`, `/dashboard/executive`

## Validation and Tests
Run migrations + all tests:
```powershell
./scripts/run_tests.ps1
```

## End-to-End Smoke Pipeline
Runs ingest -> inference -> validation -> KPI -> dashboard using sample data:
```powershell
$env:PYTHONPATH = "./backend"
.\.venv\Scripts\python.exe .\scripts\smoke_pipeline.py
```

## Notes
- DuckDB file: `db/dataforge.duckdb`
- Uploaded raw files: `data/raw/`
- This is an internal simulation, not production security/compliance architecture.

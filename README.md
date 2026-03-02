# DataForge Phase 1

This workspace contains the Phase 1 baseline implementation for DataForge:
- CSV/XLSX upload + ingestion
- DuckDB metadata registry + schema versioning
- Profiling engine (nulls, duplicates, inferred types, key candidates)
- FastAPI endpoints for upload/inventory/profile retrieval
- Streamlit MVP screens (Upload, Dataset Inventory, Profiling Summary)

## Quick start
1. Create venv and install backend deps from `backend/requirements.txt`.
2. Run migrations:
   - `python backend/run_migrations.py`
3. Start API:
   - `uvicorn backend.app.main:app --reload`
4. Start UI:
   - `streamlit run ui/app.py`

## Notes
- DuckDB database file: `db/dataforge.duckdb`
- Uploaded files are copied to `data/raw/`

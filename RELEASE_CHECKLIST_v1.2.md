# Release Checklist — v1.2-governance-observability

1. Confirm tests pass:
   - `./scripts/run_tests.ps1`
2. Confirm smoke flow passes:
   - `$env:PYTHONPATH="./backend"`
   - `.\.venv\Scripts\python.exe .\scripts\smoke_pipeline.py`
3. Verify local API/UI:
   - API: `python -m uvicorn app.main:app --app-dir backend --host 127.0.0.1 --port 8000`
   - Frontend: `python -m http.server 3000 --directory frontend`
4. Verify governance APIs:
   - `/drift/runs`, `/lineage/runs`, `/exports/*`, `/ops/cleanup`, `/ops/pipeline/run`
5. Confirm Vercel warning banner displays on `*.vercel.app`.
6. Create and push release tag:
   - `git tag v1.2-governance-observability`
   - `git push origin v1.2-governance-observability`

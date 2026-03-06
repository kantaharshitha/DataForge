# DataForge v1.3-ops-observability

Release tag: `v1.3-ops-observability`

## Highlights
- Protected operations endpoints with optional API-key enforcement (`DATAFORGE_OPS_API_KEY`).
- Persisted pipeline observability (`pipeline_run_log`, `pipeline_stage_metrics`) keyed by `correlation_id`.
- Audit artifact export bundle: `/exports/run/{correlation_id}.zip`.
- Nightly retention automation workflow and local cleanup utility.
- Frontend observability improvements:
  - Runtime diagnostics
  - Correlation ID copy
  - Download bundle by correlation ID

## Required Configuration
- Vercel env vars:
  - `DATAFORGE_RUNTIME_MODE=vercel-ephemeral`
  - `DATAFORGE_OPS_API_KEY=<strong-random-value>`
- GitHub repo secrets:
  - `DATAFORGE_BASE_URL` (example: `https://your-app.vercel.app/api`)
  - `DATAFORGE_OPS_API_KEY`

## Verification Checklist
- `GET /api/health` returns `200`.
- `GET /api/ops/runtime` without key returns `401`.
- `GET /api/ops/runtime` with valid `x-api-key` returns `200`.
- `POST /api/ops/pipeline/run` returns a `correlation_id`.
- `GET /api/exports/run/{correlation_id}.zip` downloads bundle successfully.
- Nightly cleanup workflow runs successfully from `workflow_dispatch`.

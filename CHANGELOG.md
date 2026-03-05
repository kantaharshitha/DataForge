# Changelog

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

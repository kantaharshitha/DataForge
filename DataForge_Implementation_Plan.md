# DataForge — Implementation Plan (Execution Blueprint)

## 1. Objective
Translate the Phase 0 PRD into a buildable, solo-developer execution plan using FastAPI + DuckDB + Streamlit. Prioritize deterministic behavior, explainability, and auditability.

## 1.1 Current Delivery Status (as of 2026-03-06)
| Phase | Status | Notes | Evidence |
|---|---|---|---|
| Phase 0 — Scope/PRD | Done | Scope, constraints, datasets, KPIs, governance defined | `PRD_DataForge_Phase0.md` |
| Phase 1 — Ingestion/Profiling | Done | Upload, profiling engine, metadata persistence complete | API + tests on `main` |
| Phase 2 — Inference/Curated model | Done | Deterministic relationship inference + admin decisions complete | API + tests on `main` |
| Phase 3 — Validation/Trust | Done | Validation rules, trust scoring, audit logs complete | API + tests on `main` |
| Phase 4 — KPI/Dashboard | Done | KPI registry/run and executive dashboard complete | API + tests on `main` |
| Phase 5 — Drift/Lineage | Done | Drift detection, lineage graph, exports complete | Tag `v1.2-governance-observability` |
| Ops hardening | Done | Runtime diagnostics, cleanup, pipeline observability, run bundle exports | Tag `v1.3-ops-observability` |
| Phase 6 — Alerting | Done (Code Complete) | Trust-drop + drift-high alerts, dedup, alerts API/UI + acknowledgement/assignment/escalation + exports | Tag `v1.4-alerting-hooks` + post-tag commits |

Production verification pending (manual, not code-blocking):
1. Verify webhook delivery in deployed Vercel env (`DATAFORGE_ALERT_WEBHOOK_URL`).
2. Capture production evidence for summary/ack/assignment/escalation checks.
3. Publish GitHub release notes for current alert-operations tag.

Phase 7 started (in progress):
1. Added alert SLA metrics endpoint (`/alerts/sla`) and dashboard cards.
2. Added scheduled escalation workflow (`nightly_alert_escalation.yml`).
3. Next: tune SLA targets and add breach notifications.

## 2. Delivery Principles
- Build in vertical slices: ingest -> profile -> validate -> trust -> KPI -> dashboard.
- Keep all logic deterministic and traceable.
- Ship usable internal screens early; avoid UI polish scope creep.
- Add lightweight tests for every scoring/inference module before expansion.

## 3. Technical Baseline
- Backend API: FastAPI
- Storage/compute: DuckDB (local file DB)
- UI: Streamlit
- File support: CSV/XLSX only
- Runtime: local desktop only (single-user simulation)
- Versioning: Git with phase-based branches/tags

## 4. Work Breakdown Structure

### Phase 1 — Ingestion + Profiling Foundation (Week 1)
Deliverables:
- Upload pipeline for 3-5 datasets
- Raw and staging layer persistence
- Dataset registry and schema version tables
- Profiling engine (nulls, duplicates, type checks, key candidates)
- Profiling UI pages

Tasks:
1. Set up project skeleton (`server/`, `client/` or `ui/`, shared config).
2. Implement file intake service with schema detection.
3. Create metadata tables:
   - `dataset_registry`
   - `schema_versions`
   - `profiling_runs`
   - `profiling_results`
4. Implement profiling calculations per table/column.
5. Expose API endpoints for upload + profile results.
6. Build Streamlit screens: Upload, Dataset Inventory, Profiling Summary.
7. Add tests for profiling calculations and schema persistence.

Exit criteria:
- 5 canonical datasets can be uploaded and profiled.
- Profiling results persist and are queryable.
- No hardcoded sample metrics in UI.

### Phase 2 — Relationship Inference + Curated Modeling (Week 2)
Deliverables:
- Deterministic FK candidate inference
- Cardinality hints
- Admin review workflow for relationship acceptance
- Curated model suggestion (star schema draft)

Tasks:
1. Implement inference heuristics:
   - value-overlap score
   - naming similarity score
   - type compatibility check
   - null/uniqueness checks
2. Create tables:
   - `relationship_candidates`
   - `relationship_decisions`
3. Implement confidence scoring and rationale output.
4. Build admin screen to accept/reject inferred relationships.
5. Generate curated model graph and materialized curated views.
6. Add tests for inference precision on canonical seed data.

Exit criteria:
- Inference output is deterministic and reproducible.
- Admin can approve/reject and persist decisions.
- Curated model generated from approved relationships.

### Phase 3 — Validation Engine + Data Trust Score (Week 3)
Deliverables:
- Validation framework by dimensions
- Rule-level logs and exceptions
- Trust score (0-100) with weighted breakdown
- Audit-ready run history

Tasks:
1. Implement rule registry with severity + base weights.
2. Implement rule sets:
   - completeness
   - integrity
   - conformance
   - temporal
   - drift
3. Create tables:
   - `validation_runs`
   - `validation_results`
   - `validation_exceptions`
   - `audit_event_log`
4. Implement trust score calculation and penalty model.
5. Build UI screens: Validation Center, Trust Score Breakdown, Audit View.
6. Add tests for score reproducibility and penalty math.

Exit criteria:
- Same input produces same trust score and rule outcomes.
- Rule failures include actionable context.
- Full validation history is viewable per run.

### Phase 4 — KPI Registry + Executive Dashboard (Week 4)
Deliverables:
- KPI registry with deterministic formulas
- KPI dependency tracking
- Starter executive dashboard generation
- Traceability from KPI to source + validation run

Tasks:
1. Create tables:
   - `kpi_registry`
   - `kpi_run_log`
2. Implement KPI calculator for defined KPI set.
3. Add API endpoints for KPI evaluation and dashboard data.
4. Build Streamlit screens: KPI Registry, Executive Dashboard.
5. Add quality/status indicators on KPI cards.
6. Add tests for KPI formula correctness and dependency resolution.

Exit criteria:
- 8-12 KPIs compute from curated model without manual SQL edits.
- Dashboard loads with current run data and quality badges.
- KPI-to-source traceability available in UI.

## 5. Cross-Cutting Streams

### A. Data Contracts
- Freeze canonical column names for the 5 datasets.
- Maintain schema mapping config for controlled flexibility.
- Reject or quarantine incompatible uploads with clear messages.

### B. Test Strategy
- Unit tests: profiling metrics, validation rules, scoring, KPI formulas.
- Integration tests: upload -> profile -> validate -> KPI pipeline.
- Regression tests: schema drift and repeated-run determinism.

### C. Observability
- Structured logs per run with correlation IDs.
- Persist execution timings for pipeline stages.
- Capture exception payloads in `audit_event_log`.

### D. Performance Guardrails
- Dataset volume limits enforced at upload.
- Sampling mode for expensive profiling checks.
- Clear warning banners for oversized inputs.

## 6. Milestones and Checkpoints
- M1 (end Week 1): Ingestion + profiling stable.
- M2 (end Week 2): Relationship inference and curated model review flow complete.
- M3 (end Week 3): Validation + trust score operational and reproducible.
- M4 (end Week 4): KPI registry and executive dashboard usable end-to-end.

## 7. Risks and Mitigation Actions
- Scope creep -> lock backlog to milestone goals only.
- Inference ambiguity -> keep human approval step mandatory.
- Unclear trust score -> publish formula and penalty contributors in UI.
- Time overruns -> prefer functional completeness over UI enhancements.

## 8. Definition of Done (Project)
- End-to-end flow works locally for canonical 5 datasets.
- All core metadata and audit tables are populated by actual runs.
- Trust score and KPI outputs are deterministic and test-backed.
- Minimal UI supports Analyst, Admin, and Auditor core workflows.
- Documented setup and runbook exist for repeatable local execution.

## 9. Immediate Next Actions (This Week)
1. Initialize DuckDB schema and migration scripts.
2. Build upload endpoint with dataset registry writes.
3. Implement profiling service and profiling UI page.
4. Seed canonical Sales/Retail sample datasets for testing.
5. Set up baseline test suite and CI command for local validation.

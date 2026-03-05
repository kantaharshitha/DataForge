# DataForge — Phase 5 Engineering Milestones and Tickets

## Milestone M5.1 — Data Model Foundation
Goal: Add persistence structures for drift and lineage metadata.

### Tickets
- `DF-501` Add migration `005_drift_and_lineage.sql` with five new tables.
Acceptance:
- Migration applies cleanly on empty and existing DB.
- New tables are queryable via smoke SQL checks.

- `DF-502` Add/extend Pydantic models for drift and lineage API contracts.
Acceptance:
- Models cover run summaries, events, nodes, edges, query responses.
- Existing endpoint contracts remain backward compatible.

## Milestone M5.2 — Schema Drift Engine
Goal: Implement deterministic drift comparison and metadata logging.

### Tickets
- `DF-510` Implement schema diff function (added/removed/type/key changes).
Acceptance:
- Unit tests cover each drift type.
- Deterministic output order and stable payload shape.

- `DF-511` Implement severity mapping and drift event normalization.
Acceptance:
- HIGH/MEDIUM/LOW mapping follows PRD rules.
- Unit tests verify severity behavior.

- `DF-512` Implement drift run persistence (`schema_drift_runs`, `schema_drift_events`).
Acceptance:
- Events and summary counts persisted consistently.
- Run IDs traceable across tables.

- `DF-513` Integrate drift execution into ingestion flow after schema version write.
Acceptance:
- New upload version triggers drift analysis automatically.
- No regression in existing ingestion tests.

## Milestone M5.3 — Lineage Engine
Goal: Build lineage graph artifacts across pipeline stages.

### Tickets
- `DF-520` Implement lineage node upsert with deterministic node keys.
Acceptance:
- Repeated builds do not duplicate logical nodes.

- `DF-521` Implement lineage edge upsert/dedupe.
Acceptance:
- Duplicate edge writes are idempotent.
- Edge metadata includes run context.

- `DF-522` Implement lineage build orchestration from existing artifacts.
Acceptance:
- Graph includes dataset, staging, validation, KPI, dashboard links.

- `DF-523` Implement lineage query functions (full graph, by KPI, by dataset).
Acceptance:
- Query outputs include nodes + edges sufficient for UI rendering.

## Milestone M5.4 — API Surface
Goal: Expose drift and lineage through REST endpoints.

### Tickets
- `DF-530` Add drift endpoints:
- `POST /drift/run`
- `GET /drift/runs`
- `GET /drift/events/{dataset_name}`
- `GET /drift/latest`
Acceptance:
- Valid responses for empty and populated states.
- Proper error handling for unknown dataset/run.

- `DF-531` Add lineage endpoints:
- `POST /lineage/build`
- `GET /lineage/graph`
- `GET /lineage/kpi/{kpi_code}`
- `GET /lineage/dataset/{dataset_name}`
Acceptance:
- Query filters work and response contracts are stable.

## Milestone M5.5 — Frontend Visibility
Goal: Add governance observability views in frontend.

### Tickets
- `DF-540` Add Schema Drift page.
Acceptance:
- Can run drift scan and inspect recent events by dataset.
- Severity and change details visible.

- `DF-541` Add Lineage Explorer page.
Acceptance:
- Can view graph JSON/table.
- Can filter by dataset/KPI.

## Milestone M5.6 — Quality and Operationalization
Goal: Ensure reliable behavior and update runbooks.

### Tickets
- `DF-550` Add unit tests (`test_drift.py`, `test_lineage.py`).
Acceptance:
- Coverage for diff logic, severity, node/edge dedupe, query functions.

- `DF-551` Add integration tests (`test_phase5_drift_lineage_api.py`).
Acceptance:
- Upload schema change generates drift events.
- Full run creates expected lineage paths.

- `DF-552` Update smoke pipeline to execute drift and lineage steps.
Acceptance:
- Script prints drift summary + lineage counts.

- `DF-553` Update README/runbook for Phase 5 APIs and usage.
Acceptance:
- Local run + verification instructions reflect new features.

## Suggested Delivery Sequence
1. M5.1 (DB + contracts)
2. M5.2 (drift core)
3. M5.3 (lineage core)
4. M5.4 (API wiring)
5. M5.5 (frontend)
6. M5.6 (tests + docs)

## Definition of Done (Phase 5)
- Drift events automatically generated on schema version changes.
- Lineage graph queryable and traceable from source dataset to dashboard KPI card.
- New APIs pass integration tests.
- Smoke pipeline includes drift and lineage checks.
- Documentation updated and runnable by another engineer end-to-end.

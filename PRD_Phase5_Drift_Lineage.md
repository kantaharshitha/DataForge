# DataForge — Phase 5 PRD (Schema Drift Detection + Data Lineage Tracking)

## 1. Problem Statement
As datasets evolve, silent schema changes can break joins, validations, and KPI logic. At the same time, missing lineage prevents teams from tracing metrics back to source data. Together, these gaps reduce reliability, auditability, and trust in analytics outputs.

This phase adds deterministic schema drift detection and lineage tracking to strengthen governance while preserving the current automated pipeline.

## 2. Goals
- Detect schema drift between dataset versions.
- Classify drift events by severity and persist audit records.
- Build a lineage graph from raw/staging data through KPI and dashboard outputs.
- Expose drift and lineage via REST APIs.
- Integrate with existing services without changing core product purpose.

## 3. Non-Goals
- ML-based anomaly or drift prediction.
- External lineage platform integrations.
- New auth/RBAC systems.
- Re-architecting ingestion/validation/KPI engines.
- Distributed storage or graph database adoption.

## 4. System Architecture Impact
Integration points in the existing pipeline:
- Ingestion/schema versioning: drift comparison against prior schema.
- Inference/validation/KPI: lineage node/edge capture for transformations and outputs.
- API layer: new drift and lineage endpoints.
- Frontend: drift and lineage visibility views.
- Metadata layer (DuckDB): drift and lineage tables.

## 5. Schema Drift Detection

### Definition
Schema drift is any structural change between dataset versions that can impact downstream processing.

### Drift types detected
- Added columns
- Removed columns
- Type changes
- Candidate key changes

### Detection workflow
1. Dataset ingestion writes a new `schema_versions` row.
2. Drift service loads latest and previous schema snapshots.
3. Computes diff by column/type/key candidates.
4. Assigns severity:
- HIGH: removed columns, key changes
- MEDIUM: type changes
- LOW: added columns
5. Persists run + events.
6. Returns summary to API/UI.

### Metadata storage
- Drift runs (dataset, versions, summary counts, timestamps)
- Drift events (column-level changes, severity, details)

### API endpoints
- `POST /drift/run`
- `GET /drift/runs`
- `GET /drift/events/{dataset_name}`
- `GET /drift/latest`

### Example drift report
- Dataset: `orders`
- Versions: 3 -> 4
- Events:
- removed `payment_status` (HIGH)
- type changed `order_date` STRING -> DATE (MEDIUM)
- added `promo_code` (LOW)

## 6. Data Lineage Tracking

### Lineage model
Directed graph representing data movement and derivations from source datasets to dashboard outputs.

### Nodes
- Raw dataset
- Staging table
- Curated entity
- Validation rule
- KPI
- Dashboard card

### Edges
- `raw -> staging`
- `staging -> curated`
- `staging/curated -> validation rule`
- `staging/curated -> KPI`
- `KPI -> dashboard card`

### Capture strategy
- Ingest: dataset/staging nodes and edges.
- Inference/model approval: relationship lineage edges.
- Validation run: rule lineage edges.
- KPI run: KPI dependency edges.
- Dashboard generation: KPI-to-card edges.

### Lineage queries
- Upstream by KPI
- Downstream impact by dataset
- Full graph snapshot by run

### Visualization strategy
- Initial: JSON graph + tabular node/edge explorer.
- Filters: dataset, KPI, run ID.
- Optional later: interactive graph rendering.

## 7. Database Schema Changes
Migration file: `005_drift_and_lineage.sql`

New tables:
- `schema_drift_runs`
- `schema_drift_events`
- `lineage_runs`
- `lineage_nodes`
- `lineage_edges`

## 8. API Design
Drift APIs:
- `POST /drift/run`
- `GET /drift/runs`
- `GET /drift/events/{dataset_name}`
- `GET /drift/latest`

Lineage APIs:
- `POST /lineage/build`
- `GET /lineage/graph`
- `GET /lineage/kpi/{kpi_code}`
- `GET /lineage/dataset/{dataset_name}`

## 9. Implementation Plan
1. Add drift/lineage migration.
2. Implement `services/drift.py`.
3. Implement `services/lineage.py`.
4. Add request/response models.
5. Add routes.
6. Hook drift in ingestion flow.
7. Hook lineage build into KPI or explicit endpoint.
8. Add frontend views.
9. Extend smoke pipeline.
10. Update docs.

## 10. Testing Strategy
Unit tests:
- Schema diff logic
- Drift severity mapping
- Lineage node/edge upsert behavior
- Lineage query correctness

Integration tests:
- v1 -> v2 upload with schema changes generates drift events
- end-to-end run creates expected lineage paths
- API contracts for all drift/lineage endpoints

## 11. Risks and Edge Cases
- Benign type normalization causing noisy drift events
- Duplicate lineage edges across repeated runs
- Partial pipeline runs creating incomplete graphs
- Metadata growth over time
- Ephemeral storage constraints on Vercel

Mitigations:
- Normalize data types before diff
- Deterministic node keys and edge dedupe
- Run status tagging and partial-run handling
- Pagination/retention policies
- Explicit deployment notes for persistence limits

## 12. Success Metrics
- Drift detection coverage per schema update
- Drift event accuracy in controlled test cases
- Lineage completeness from KPI to source datasets
- API reliability (integration pass rate)
- Reduced time-to-root-cause during debugging
- Deterministic repeatability of drift/lineage outputs

# DataForge — Phase 0 Product Requirements Document (PRD)

## 1. Problem Statement (Enterprise Pain Points)
Business teams frequently receive small but critical datasets from multiple systems with inconsistent schemas and weak documentation. This causes manual reconciliation, conflicting KPI outputs, and low confidence in decision-making.

DataForge addresses this by simulating an internal enterprise analytics platform workflow that converts uploaded tabular files into explainable, trust-scored, KPI-ready analytics outputs.

## 2. Vision & Positioning
**Vision:** Build a practical, local-first simulation of enterprise analytics operations from ingestion to trusted KPI delivery.

**Positioning:**
- Not a BI replacement.
- Not a full production data platform.
- A realistic simulation of internal platform behavior for data quality, governance, and repeatable KPI logic.

## 3. Goals (Phase 0 Only)
- Define project boundaries and implementation constraints.
- Freeze initial domain and canonical datasets.
- Define deterministic validation and trust scoring models.
- Define governance metadata required for traceability.
- Define KPI registry scope and measurable success criteria.

## 4. Non-Goals
- Production-grade IAM/SSO/security architecture.
- Streaming ingestion or CDC.
- ML-heavy inference or anomaly detection.
- Distributed systems, microservices, or big data stack.
- Full dashboard authoring platform.

## 5. Target Personas
- **Business Analyst:** uploads files, views trust score and dashboards, exports reports.
- **Data Engineer/Admin:** reviews inferred model, validates rules, manages quality exceptions.
- **Auditor/Compliance (Read-only):** reviews validation history, schema drift, and trust evidence.

## 6. System Architecture Overview (High-Level)
```text
[CSV/XLSX Upload]
      |
      v
[Raw Layer] -> ingest metadata + source registry
      |
      v
[Staging Layer] -> type normalization + profiling
      |
      v
[Validation Engine] -> rule outcomes + trust score
      |
      v
[Relationship Inference] -> FK candidates + cardinality hints
      |
      v
[Curated Layer] -> star-schema suggestion + KPI-ready views
      |
      v
[KPI Registry + Dashboard Generator]
      |
      v
[Analyst/Admin/Audit UI]
```

**Implementation baseline:** FastAPI + DuckDB + Streamlit (local-first).

## 7. Domain Selection Justification (Sales/Retail)
Sales/Retail is selected because it provides:
- Clear fact-dimension modeling patterns.
- Strong KPI coverage (revenue, margin, AOV, fulfillment, stockout).
- Reliable FK inference opportunities.
- High realism with manageable complexity for a solo builder.

## 8. Canonical Dataset Blueprint (5 Datasets)

| Dataset | Purpose | Key Columns | Relationship Targets |
|---|---|---|---|
| `customers` | Customer master data | `customer_id` (PK), `email` (candidate unique) | Referenced by `orders.customer_id` |
| `products` | Product catalog | `product_id` (PK), `sku` (unique) | Referenced by `order_items.product_id`, `inventory_snapshots.product_id` |
| `orders` | Order header transactions | `order_id` (PK), `customer_id` (FK), `order_date` | Parent of `order_items`; child of `customers` |
| `order_items` | Order line facts | `order_item_id` (PK), `order_id` (FK), `product_id` (FK) | Child of `orders` and `products` |
| `inventory_snapshots` | Daily stock status | Composite PK: `snapshot_date`,`product_id`,`warehouse_id` | Child of `products` |

Expected cardinality:
- `customers (1) -> (N) orders`
- `orders (1) -> (N) order_items`
- `products (1) -> (N) order_items`
- `products (1) -> (N) inventory_snapshots`

## 9. Validation Rule Framework

### Completeness
- Required keys must be non-null.
- Mandatory dates must be non-null.
- Critical business fields (`quantity`, `unit_price`) must be present.

### Integrity
- PK uniqueness per dataset.
- FK referential integrity across inferred/approved relations.
- Duplicate candidate detection for business keys (e.g., `sku`).

### Conformance
- Type checks by canonical schema.
- Domain checks for statuses/codes.
- Numeric bound checks (`quantity > 0`, `price >= 0`, `discount >= 0`).

### Temporal
- `ship_date >= order_date`.
- Future-date checks with configured tolerance.
- Snapshot continuity checks.

### Drift
- Detect added/removed/type-changed columns per schema version.
- Detect key candidate changes over time.

## 10. Data Trust Score Model

Dimension weights:
- Completeness: 30%
- Integrity: 30%
- Conformance: 20%
- Temporal: 10%
- Drift Stability: 10%

Severity multipliers:
- Critical: 1.0
- High: 0.7
- Medium: 0.4
- Low: 0.2

Formula:
- `failure_rate = failed_records / evaluated_records`
- `rule_penalty = base_weight * severity_multiplier * failure_rate`
- `dimension_score = max(0, 100 - sum(rule_penalty * 100))`
- `trust_score = round(sum(dimension_score * dimension_weight))`

Output requirements:
- Overall score (0–100)
- Dimension-level breakdown
- Top penalty contributors
- Immutable run snapshot for audit

## 11. Relationship Inference Strategy (Deterministic)
1. Detect PK candidates by uniqueness + low null ratio + naming conventions.
2. Generate FK candidates using value-overlap ratio against PK candidates.
3. Add confidence boost for naming alignment (`customer_id` -> `customers.customer_id`).
4. Confirm type compatibility and null behavior.
5. Infer cardinality from distinct-count patterns and duplication distributions.
6. Require admin accept/reject decision for promoted relationships.

## 12. Governance Metadata Model (Required Tables)
- `dataset_registry`: dataset inventory, source metadata, ingest status.
- `schema_versions`: versioned schemas and inferred type maps.
- `profiling_runs`: profiling execution metadata.
- `profiling_results`: column-level profiling metrics.
- `validation_runs`: run metadata + trust score snapshot.
- `validation_results`: rule outcomes and penalty details.
- `validation_exceptions`: failing-record samples/references.
- `relationship_candidates`: inferred joins with confidence evidence.
- `relationship_decisions`: admin approval/rejection trail.
- `kpi_registry`: KPI definitions, dependencies, and versioning.
- `kpi_run_log`: KPI generation/calculation history.
- `audit_event_log`: immutable operation timeline.

## 13. KPI Registry Plan (10 KPIs)

| KPI | Definition | Required Fields |
|---|---|---|
| Gross Revenue | Sum of `quantity * unit_price` | `quantity`, `unit_price` |
| Net Revenue | Gross revenue minus discounts | `quantity`, `unit_price`, `discount_amount` |
| Orders Count | Count of unique orders | `order_id` |
| Average Order Value | Net revenue / order count | net revenue fields, `order_id` |
| Units Sold | Sum of quantities sold | `quantity` |
| Gross Margin % | `(Net Revenue - COGS) / Net Revenue` | `unit_cost`, sales fields |
| Discount Rate % | `Total Discount / Gross Revenue` | `discount_amount`, gross revenue fields |
| Repeat Customer Rate | % customers with more than one order | `customer_id`, `order_id` |
| Fulfillment Lag | Avg(`ship_date - order_date`) | `order_date`, `ship_date` |
| Stockout Rate | % snapshots with stockout flag | `stockout_flag`, `snapshot_date`, `product_id` |

## 14. UX Screen Inventory (MVP)
- Upload/Ingestion screen
- Dataset Registry screen
- Profiling Summary screen
- Validation + Trust Score screen
- Relationship Inference review screen
- Curated Model preview screen
- KPI Registry screen
- Executive Dashboard screen
- Audit/Traceability screen

## 15. Risks & Mitigations
- Scope creep -> enforce phase gates and explicit out-of-scope list.
- Inference false positives -> conservative thresholds + human review.
- Trust score opacity -> expose weighted formula and penalty drivers.
- UI time sink -> prioritize utility over polish.
- Schema variability -> canonical mapping + drift reporting.
- Performance bottlenecks -> row/file limits + sampling fallback.

## 16. Success Metrics (Quantifiable)
- 100% dataset uploads recorded in registry.
- End-to-end processing <= 120s for baseline workload (5 files, <=200k rows total).
- FK inference precision >= 90% on canonical sample set.
- Trust score reproducible for same input + same rules.
- >=95% validation failures produce actionable error context.
- Auditor can trace KPI-to-source and validation evidence in <=3 screens.

## 17. Phase Gate Definition (Ready for Phase 1)
Phase 0 is complete only when:
- Domain and 5-dataset blueprint are approved.
- Validation catalog + severity model are frozen.
- Trust score weighting and formula are finalized.
- Governance metadata model is documented and accepted.
- KPI registry v1 (8–12 KPIs) is finalized.
- MVP screen inventory is approved.
- Success metrics and test scenarios are documented.
- Out-of-scope boundaries are explicitly signed off.

---

## Phase Plan Summary (Phases 0–4)
- **Phase 0:** Scope, constraints, datasets, validation/trust model, success criteria.
- **Phase 1:** Build ingestion + profiling + dataset registry.
- **Phase 2:** Add relationship inference + curated model suggestion.
- **Phase 3:** Implement validation runs, trust scoring, governance logs.
- **Phase 4:** Deliver KPI registry and starter executive dashboard.

## Glossary
- **Medallion Architecture:** Raw -> Staging -> Curated layer pattern.
- **Data Trust Score:** Deterministic 0–100 quality score from weighted validation dimensions.
- **Schema Drift:** Structural schema changes across dataset versions.
- **Cardinality:** Relationship multiplicity (1:1, 1:N, N:1, N:N).

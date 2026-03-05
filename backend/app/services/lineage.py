"""Data lineage graph service for Phase 5."""

from __future__ import annotations

import json
import uuid
from collections import defaultdict, deque
from datetime import datetime, timezone

from app.db import get_conn


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _upsert_node(conn, node_type: str, node_key: str, display_name: str, metadata: dict | None = None) -> str:
    now = _utc_now()
    row = conn.execute(
        "SELECT node_id FROM lineage_nodes WHERE node_key = ?",
        [node_key],
    ).fetchone()

    if row:
        node_id = row[0]
        conn.execute(
            """
            UPDATE lineage_nodes
            SET node_type = ?, display_name = ?, metadata_json = ?, updated_at = ?
            WHERE node_id = ?
            """,
            [node_type, display_name, json.dumps(metadata or {}), now, node_id],
        )
        return node_id

    node_id = str(uuid.uuid4())
    conn.execute(
        """
        INSERT INTO lineage_nodes (node_id, node_type, node_key, display_name, metadata_json, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [node_id, node_type, node_key, display_name, json.dumps(metadata or {}), now, now],
    )
    return node_id


def _upsert_edge(
    conn,
    lineage_run_id: str,
    from_node_id: str,
    to_node_id: str,
    edge_type: str,
    metadata: dict | None = None,
) -> str:
    now = _utc_now()
    existing = conn.execute(
        """
        SELECT edge_id FROM lineage_edges
        WHERE lineage_run_id = ? AND from_node_id = ? AND to_node_id = ? AND edge_type = ?
        """,
        [lineage_run_id, from_node_id, to_node_id, edge_type],
    ).fetchone()

    if existing:
        edge_id = existing[0]
        conn.execute(
            "UPDATE lineage_edges SET metadata_json = ? WHERE edge_id = ?",
            [json.dumps(metadata or {}), edge_id],
        )
        return edge_id

    edge_id = str(uuid.uuid4())
    conn.execute(
        """
        INSERT INTO lineage_edges (edge_id, lineage_run_id, from_node_id, to_node_id, edge_type, metadata_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [edge_id, lineage_run_id, from_node_id, to_node_id, edge_type, json.dumps(metadata or {}), now],
    )
    return edge_id


def _latest_dataset_names(conn) -> list[str]:
    rows = conn.execute(
        """
        WITH ranked AS (
            SELECT dataset_name, ingested_at,
                   ROW_NUMBER() OVER (PARTITION BY dataset_name ORDER BY ingested_at DESC) AS rn
            FROM dataset_registry
        )
        SELECT dataset_name
        FROM ranked
        WHERE rn = 1
        ORDER BY dataset_name
        """
    ).fetchall()
    return [row[0] for row in rows]


def _dataset_dependencies_for_kpi(required_fields: list[str]) -> set[str]:
    field_set = set(required_fields)
    deps = set()

    if field_set & {"order_id", "customer_id", "order_date", "ship_date", "channel", "order_status"}:
        deps.add("orders")
    if field_set & {"quantity", "unit_price", "discount_amount", "tax_amount", "line_total", "product_id"}:
        deps.add("order_items")
    if field_set & {"unit_cost", "list_price", "sku", "brand", "category"}:
        deps.add("products")
    if field_set & {"stockout_flag", "snapshot_date", "warehouse_id", "on_hand_qty", "reserved_qty", "reorder_point"}:
        deps.add("inventory_snapshots")
    if field_set & {"segment", "region", "signup_date", "status", "email", "customer_name"}:
        deps.add("customers")

    return deps


def build_lineage_graph() -> dict:
    lineage_run_id = str(uuid.uuid4())
    run_at = _utc_now()

    with get_conn() as conn:
        conn.execute(
            "INSERT INTO lineage_runs (lineage_run_id, run_at, status, source_context_json) VALUES (?, ?, ?, ?)",
            [lineage_run_id, run_at, "RUNNING", json.dumps({})],
        )

        node_ids: set[str] = set()
        edge_ids: set[str] = set()

        # Raw and staging nodes.
        for dataset_name in _latest_dataset_names(conn):
            raw_node = _upsert_node(
                conn,
                node_type="raw_dataset",
                node_key=f"raw:{dataset_name}",
                display_name=f"raw.{dataset_name}",
                metadata={"dataset_name": dataset_name},
            )
            staging_node = _upsert_node(
                conn,
                node_type="staging_table",
                node_key=f"staging:stg_{dataset_name}",
                display_name=f"stg_{dataset_name}",
                metadata={"dataset_name": dataset_name},
            )
            node_ids.update([raw_node, staging_node])
            edge_ids.add(
                _upsert_edge(
                    conn,
                    lineage_run_id=lineage_run_id,
                    from_node_id=raw_node,
                    to_node_id=staging_node,
                    edge_type="raw_to_staging",
                    metadata={"dataset_name": dataset_name},
                )
            )

        # Inferred relationships.
        rel_rows = conn.execute(
            """
            SELECT parent_dataset_name, child_dataset_name, parent_column, child_column
            FROM relationship_candidates
            WHERE status = 'ACCEPTED'
            """
        ).fetchall()

        for parent_ds, child_ds, parent_col, child_col in rel_rows:
            parent_node = _upsert_node(
                conn,
                node_type="staging_table",
                node_key=f"staging:stg_{parent_ds}",
                display_name=f"stg_{parent_ds}",
                metadata={"dataset_name": parent_ds},
            )
            child_node = _upsert_node(
                conn,
                node_type="staging_table",
                node_key=f"staging:stg_{child_ds}",
                display_name=f"stg_{child_ds}",
                metadata={"dataset_name": child_ds},
            )
            node_ids.update([parent_node, child_node])
            edge_ids.add(
                _upsert_edge(
                    conn,
                    lineage_run_id=lineage_run_id,
                    from_node_id=parent_node,
                    to_node_id=child_node,
                    edge_type="staging_fk_relation",
                    metadata={
                        "parent_column": parent_col,
                        "child_column": child_col,
                    },
                )
            )

        # Validation rule nodes from latest run.
        latest_validation = conn.execute(
            "SELECT validation_run_id FROM validation_runs ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
        if latest_validation:
            validation_run_id = latest_validation[0]
            val_rows = conn.execute(
                """
                SELECT DISTINCT dataset_name, rule_code
                FROM validation_results
                WHERE validation_run_id = ?
                """,
                [validation_run_id],
            ).fetchall()
            for dataset_name, rule_code in val_rows:
                stage_node = _upsert_node(
                    conn,
                    node_type="staging_table",
                    node_key=f"staging:stg_{dataset_name}",
                    display_name=f"stg_{dataset_name}",
                    metadata={"dataset_name": dataset_name},
                )
                rule_node = _upsert_node(
                    conn,
                    node_type="validation_rule",
                    node_key=f"validation_rule:{rule_code}",
                    display_name=rule_code,
                    metadata={"validation_run_id": validation_run_id},
                )
                node_ids.update([stage_node, rule_node])
                edge_ids.add(
                    _upsert_edge(
                        conn,
                        lineage_run_id=lineage_run_id,
                        from_node_id=stage_node,
                        to_node_id=rule_node,
                        edge_type="staging_to_validation_rule",
                        metadata={"validation_run_id": validation_run_id},
                    )
                )

        # KPI and dashboard nodes from latest KPI run.
        latest_kpi = conn.execute(
            "SELECT kpi_run_id, kpi_values_json FROM kpi_run_log ORDER BY generated_at DESC LIMIT 1"
        ).fetchone()

        registry_rows = conn.execute(
            "SELECT kpi_code, kpi_name, required_fields_json FROM kpi_registry WHERE status = 'ACTIVE'"
        ).fetchall()

        for kpi_code, kpi_name, req_json in registry_rows:
            req_fields = json.loads(req_json) if req_json else []
            kpi_node = _upsert_node(
                conn,
                node_type="kpi",
                node_key=f"kpi:{kpi_code}",
                display_name=kpi_name,
                metadata={"kpi_code": kpi_code, "required_fields": req_fields},
            )
            node_ids.add(kpi_node)

            for dataset_dep in _dataset_dependencies_for_kpi(req_fields):
                stage_node = _upsert_node(
                    conn,
                    node_type="staging_table",
                    node_key=f"staging:stg_{dataset_dep}",
                    display_name=f"stg_{dataset_dep}",
                    metadata={"dataset_name": dataset_dep},
                )
                node_ids.add(stage_node)
                edge_ids.add(
                    _upsert_edge(
                        conn,
                        lineage_run_id=lineage_run_id,
                        from_node_id=stage_node,
                        to_node_id=kpi_node,
                        edge_type="staging_to_kpi",
                        metadata={"kpi_code": kpi_code},
                    )
                )

            if latest_kpi:
                card_node = _upsert_node(
                    conn,
                    node_type="dashboard_card",
                    node_key=f"dashboard_card:{kpi_code}",
                    display_name=f"card_{kpi_code}",
                    metadata={"kpi_code": kpi_code, "kpi_run_id": latest_kpi[0]},
                )
                node_ids.add(card_node)
                edge_ids.add(
                    _upsert_edge(
                        conn,
                        lineage_run_id=lineage_run_id,
                        from_node_id=kpi_node,
                        to_node_id=card_node,
                        edge_type="kpi_to_dashboard_card",
                        metadata={"kpi_run_id": latest_kpi[0]},
                    )
                )

        conn.execute(
            "UPDATE lineage_runs SET status = ?, source_context_json = ? WHERE lineage_run_id = ?",
            [
                "SUCCESS",
                json.dumps({"node_count": len(node_ids), "edge_count": len(edge_ids)}),
                lineage_run_id,
            ],
        )

    return {
        "lineage_run_id": lineage_run_id,
        "run_at": run_at,
        "status": "SUCCESS",
        "node_count": len(node_ids),
        "edge_count": len(edge_ids),
    }


def list_lineage_runs() -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT lineage_run_id, run_at, status, source_context_json FROM lineage_runs ORDER BY run_at DESC"
        ).fetchall()

    return [
        {
            "lineage_run_id": row[0],
            "run_at": row[1],
            "status": row[2],
            "source_context": json.loads(row[3]) if row[3] else {},
        }
        for row in rows
    ]


def _latest_lineage_run_id(conn) -> str:
    row = conn.execute("SELECT lineage_run_id FROM lineage_runs ORDER BY run_at DESC LIMIT 1").fetchone()
    if not row:
        raise ValueError("No lineage runs found.")
    return row[0]


def get_lineage_graph(lineage_run_id: str | None = None) -> dict:
    with get_conn() as conn:
        if lineage_run_id is None:
            lineage_run_id = _latest_lineage_run_id(conn)

        edge_rows = conn.execute(
            """
            SELECT edge_id, lineage_run_id, from_node_id, to_node_id, edge_type, metadata_json, created_at
            FROM lineage_edges
            WHERE lineage_run_id = ?
            ORDER BY created_at
            """,
            [lineage_run_id],
        ).fetchall()

        if not edge_rows:
            return {"lineage_run_id": lineage_run_id, "nodes": [], "edges": []}

        node_ids = sorted({row[2] for row in edge_rows} | {row[3] for row in edge_rows})
        placeholders = ", ".join(["?"] * len(node_ids))
        node_rows = conn.execute(
            f"""
            SELECT node_id, node_type, node_key, display_name, metadata_json, created_at, updated_at
            FROM lineage_nodes
            WHERE node_id IN ({placeholders})
            ORDER BY node_type, display_name
            """,
            node_ids,
        ).fetchall()

    nodes = [
        {
            "node_id": row[0],
            "node_type": row[1],
            "node_key": row[2],
            "display_name": row[3],
            "metadata": json.loads(row[4]) if row[4] else {},
            "created_at": row[5],
            "updated_at": row[6],
        }
        for row in node_rows
    ]

    edges = [
        {
            "edge_id": row[0],
            "lineage_run_id": row[1],
            "from_node_id": row[2],
            "to_node_id": row[3],
            "edge_type": row[4],
            "metadata": json.loads(row[5]) if row[5] else {},
            "created_at": row[6],
        }
        for row in edge_rows
    ]

    return {
        "lineage_run_id": lineage_run_id,
        "nodes": nodes,
        "edges": edges,
    }


def _filter_graph_by_seed(graph: dict, seed_node_keys: set[str], direction: str) -> dict:
    nodes_by_id = {n["node_id"]: n for n in graph["nodes"]}
    key_to_id = {n["node_key"]: n["node_id"] for n in graph["nodes"]}

    seed_ids = {key_to_id[k] for k in seed_node_keys if k in key_to_id}
    if not seed_ids:
        return {"lineage_run_id": graph["lineage_run_id"], "nodes": [], "edges": []}

    adjacency = defaultdict(list)
    if direction == "upstream":
        for edge in graph["edges"]:
            adjacency[edge["to_node_id"]].append(edge)
    else:
        for edge in graph["edges"]:
            adjacency[edge["from_node_id"]].append(edge)

    seen_nodes = set(seed_ids)
    seen_edges = set()
    queue = deque(seed_ids)

    while queue:
        current = queue.popleft()
        for edge in adjacency[current]:
            seen_edges.add(edge["edge_id"])
            neighbor = edge["from_node_id"] if direction == "upstream" else edge["to_node_id"]
            if neighbor not in seen_nodes:
                seen_nodes.add(neighbor)
                queue.append(neighbor)

    return {
        "lineage_run_id": graph["lineage_run_id"],
        "nodes": [nodes_by_id[nid] for nid in sorted(seen_nodes)],
        "edges": [edge for edge in graph["edges"] if edge["edge_id"] in seen_edges],
    }


def get_lineage_for_kpi(kpi_code: str, lineage_run_id: str | None = None) -> dict:
    graph = get_lineage_graph(lineage_run_id=lineage_run_id)
    return _filter_graph_by_seed(graph, {f"kpi:{kpi_code}"}, direction="upstream")


def get_lineage_for_dataset(dataset_name: str, lineage_run_id: str | None = None) -> dict:
    graph = get_lineage_graph(lineage_run_id=lineage_run_id)
    seeds = {f"raw:{dataset_name}", f"staging:stg_{dataset_name}"}
    return _filter_graph_by_seed(graph, seeds, direction="downstream")

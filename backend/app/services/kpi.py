"""KPI registry, execution, and executive dashboard service for Phase 4."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

from app.db import get_conn

DEFAULT_KPIS = [
    {
        "kpi_code": "gross_revenue",
        "kpi_name": "Gross Revenue",
        "definition": "Sum of quantity multiplied by unit price.",
        "formula": "SUM(quantity * unit_price)",
        "required_fields": ["quantity", "unit_price"],
    },
    {
        "kpi_code": "net_revenue",
        "kpi_name": "Net Revenue",
        "definition": "Gross revenue minus discounts.",
        "formula": "SUM(quantity*unit_price - discount_amount)",
        "required_fields": ["quantity", "unit_price", "discount_amount"],
    },
    {
        "kpi_code": "orders_count",
        "kpi_name": "Orders Count",
        "definition": "Count of distinct orders.",
        "formula": "COUNT(DISTINCT order_id)",
        "required_fields": ["order_id"],
    },
    {
        "kpi_code": "average_order_value",
        "kpi_name": "Average Order Value",
        "definition": "Net revenue divided by order count.",
        "formula": "net_revenue / orders_count",
        "required_fields": ["order_id", "quantity", "unit_price", "discount_amount"],
    },
    {
        "kpi_code": "units_sold",
        "kpi_name": "Units Sold",
        "definition": "Total sold units.",
        "formula": "SUM(quantity)",
        "required_fields": ["quantity"],
    },
    {
        "kpi_code": "discount_rate_pct",
        "kpi_name": "Discount Rate %",
        "definition": "Total discount divided by gross revenue, as percent.",
        "formula": "SUM(discount_amount) / gross_revenue * 100",
        "required_fields": ["discount_amount", "quantity", "unit_price"],
    },
    {
        "kpi_code": "repeat_customer_rate_pct",
        "kpi_name": "Repeat Customer Rate %",
        "definition": "Percent of customers having more than one order.",
        "formula": "repeat_customers / all_customers * 100",
        "required_fields": ["customer_id", "order_id"],
    },
    {
        "kpi_code": "fulfillment_lag_days",
        "kpi_name": "Fulfillment Lag (Days)",
        "definition": "Average day difference between ship_date and order_date.",
        "formula": "AVG(ship_date - order_date)",
        "required_fields": ["order_date", "ship_date"],
    },
    {
        "kpi_code": "stockout_rate_pct",
        "kpi_name": "Stockout Rate %",
        "definition": "Percent of inventory snapshots flagged as stockout.",
        "formula": "SUM(stockout_flag=true) / COUNT(*) * 100",
        "required_fields": ["stockout_flag"],
    },
    {
        "kpi_code": "gross_margin_pct",
        "kpi_name": "Gross Margin %",
        "definition": "(Net revenue - COGS) divided by net revenue.",
        "formula": "(net_revenue - cogs) / net_revenue * 100",
        "required_fields": ["unit_cost", "quantity", "unit_price", "discount_amount"],
    },
]


def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table_name],
    ).fetchone()
    return bool(row and row[0] > 0)


def _table_columns(conn, table_name: str) -> set[str]:
    if not _table_exists(conn, table_name):
        return set()
    return {row[0] for row in conn.execute(f"DESCRIBE {table_name}").fetchall()}


def _safe_div(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return numerator / denominator


def seed_kpi_registry() -> int:
    now = datetime.now(timezone.utc).isoformat()
    inserted = 0

    with get_conn() as conn:
        for kpi in DEFAULT_KPIS:
            existing = conn.execute(
                "SELECT kpi_id FROM kpi_registry WHERE kpi_code = ?",
                [kpi["kpi_code"]],
            ).fetchone()
            if existing:
                continue

            conn.execute(
                """
                INSERT INTO kpi_registry (
                    kpi_id, kpi_code, kpi_name, definition, formula,
                    required_fields_json, status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    str(uuid.uuid4()),
                    kpi["kpi_code"],
                    kpi["kpi_name"],
                    kpi["definition"],
                    kpi["formula"],
                    json.dumps(kpi["required_fields"]),
                    "ACTIVE",
                    now,
                    now,
                ],
            )
            inserted += 1

    return inserted


def list_kpi_registry() -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT kpi_id, kpi_code, kpi_name, definition, formula,
                   required_fields_json, status, created_at, updated_at
            FROM kpi_registry
            ORDER BY kpi_code
            """
        ).fetchall()

    return [
        {
            "kpi_id": row[0],
            "kpi_code": row[1],
            "kpi_name": row[2],
            "definition": row[3],
            "formula": row[4],
            "required_fields": json.loads(row[5]) if row[5] else [],
            "status": row[6],
            "created_at": row[7],
            "updated_at": row[8],
        }
        for row in rows
    ]


def _latest_validation_run_id(conn) -> str | None:
    row = conn.execute(
        "SELECT validation_run_id FROM validation_runs ORDER BY started_at DESC LIMIT 1"
    ).fetchone()
    return row[0] if row else None


def _kpi_values(conn) -> dict[str, float]:
    has_order_items = _table_exists(conn, "stg_order_items")
    has_orders = _table_exists(conn, "stg_orders")
    has_inventory = _table_exists(conn, "stg_inventory_snapshots")
    has_products = _table_exists(conn, "stg_products")
    order_item_cols = _table_columns(conn, "stg_order_items")
    order_cols = _table_columns(conn, "stg_orders")
    inventory_cols = _table_columns(conn, "stg_inventory_snapshots")
    product_cols = _table_columns(conn, "stg_products")

    gross_revenue = 0.0
    net_revenue = 0.0
    units_sold = 0.0
    total_discount = 0.0
    cogs = 0.0

    if has_order_items:
        qty_expr = "COALESCE(quantity, 0)" if "quantity" in order_item_cols else "0"
        unit_price_expr = "COALESCE(unit_price, 0)" if "unit_price" in order_item_cols else "0"
        discount_expr = "COALESCE(discount_amount, 0)" if "discount_amount" in order_item_cols else "0"
        row = conn.execute(
            f"""
            SELECT
                COALESCE(SUM({qty_expr} * {unit_price_expr}), 0) AS gross_revenue,
                COALESCE(SUM({qty_expr} * {unit_price_expr} - {discount_expr}), 0) AS net_revenue,
                COALESCE(SUM({qty_expr}), 0) AS units_sold,
                COALESCE(SUM({discount_expr}), 0) AS total_discount
            FROM stg_order_items
            """
        ).fetchone()
        gross_revenue = float(row[0])
        net_revenue = float(row[1])
        units_sold = float(row[2])
        total_discount = float(row[3])

        if (
            has_products
            and "quantity" in order_item_cols
            and "product_id" in order_item_cols
            and "product_id" in product_cols
            and "unit_cost" in product_cols
        ):
            cogs_row = conn.execute(
                """
                SELECT COALESCE(SUM(COALESCE(oi.quantity,0) * COALESCE(p.unit_cost,0)), 0)
                FROM stg_order_items oi
                LEFT JOIN stg_products p ON oi.product_id = p.product_id
                """
            ).fetchone()
            cogs = float(cogs_row[0])

    orders_count = 0
    repeat_customer_rate = 0.0
    fulfillment_lag = 0.0

    if has_orders:
        if "order_id" in order_cols:
            orders_count = int(
                conn.execute("SELECT COUNT(DISTINCT order_id) FROM stg_orders").fetchone()[0]
            )

        if "customer_id" in order_cols and "order_id" in order_cols:
            repeat_row = conn.execute(
                """
                WITH customer_counts AS (
                    SELECT customer_id, COUNT(DISTINCT order_id) AS order_cnt
                    FROM stg_orders
                    WHERE customer_id IS NOT NULL
                    GROUP BY customer_id
                )
                SELECT
                    COALESCE(SUM(CASE WHEN order_cnt > 1 THEN 1 ELSE 0 END), 0) AS repeat_customers,
                    COUNT(*) AS total_customers
                FROM customer_counts
                """
            ).fetchone()
            repeat_customer_rate = _safe_div(float(repeat_row[0]), float(repeat_row[1])) * 100.0

        if "order_date" in order_cols and "ship_date" in order_cols:
            lag_row = conn.execute(
                """
                SELECT COALESCE(AVG(DATEDIFF('day', TRY_CAST(order_date AS DATE), TRY_CAST(ship_date AS DATE))), 0)
                FROM stg_orders
                WHERE order_date IS NOT NULL AND ship_date IS NOT NULL
                """
            ).fetchone()
            fulfillment_lag = float(lag_row[0])

    stockout_rate = 0.0
    if has_inventory and "stockout_flag" in inventory_cols:
        stock_row = conn.execute(
            """
            SELECT
                COALESCE(SUM(CASE WHEN LOWER(CAST(stockout_flag AS VARCHAR)) IN ('1','true','t','yes','y') THEN 1 ELSE 0 END), 0),
                COUNT(*)
            FROM stg_inventory_snapshots
            """
        ).fetchone()
        stockout_rate = _safe_div(float(stock_row[0]), float(stock_row[1])) * 100.0

    aov = _safe_div(net_revenue, float(orders_count))
    discount_rate = _safe_div(total_discount, gross_revenue) * 100.0
    gross_margin = _safe_div((net_revenue - cogs), net_revenue) * 100.0 if net_revenue else 0.0

    return {
        "gross_revenue": round(gross_revenue, 2),
        "net_revenue": round(net_revenue, 2),
        "orders_count": float(orders_count),
        "average_order_value": round(aov, 2),
        "units_sold": round(units_sold, 2),
        "discount_rate_pct": round(discount_rate, 2),
        "repeat_customer_rate_pct": round(repeat_customer_rate, 2),
        "fulfillment_lag_days": round(fulfillment_lag, 2),
        "stockout_rate_pct": round(stockout_rate, 2),
        "gross_margin_pct": round(gross_margin, 2),
    }


def run_kpis() -> dict:
    generated_at = datetime.now(timezone.utc).isoformat()
    kpi_run_id = str(uuid.uuid4())

    with get_conn() as conn:
        validation_run_id = _latest_validation_run_id(conn)
        values = _kpi_values(conn)

        conn.execute(
            """
            INSERT INTO kpi_run_log (kpi_run_id, validation_run_id, generated_at, status, kpi_values_json, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                kpi_run_id,
                validation_run_id,
                generated_at,
                "SUCCESS",
                json.dumps(values),
                json.dumps({"kpi_count": len(values)}),
            ],
        )

    return {
        "kpi_run_id": kpi_run_id,
        "validation_run_id": validation_run_id,
        "generated_at": generated_at,
        "status": "SUCCESS",
        "kpi_values": values,
    }


def get_latest_kpi_run() -> dict:
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT kpi_run_id, validation_run_id, generated_at, status, kpi_values_json
            FROM kpi_run_log
            ORDER BY generated_at DESC
            LIMIT 1
            """
        ).fetchone()

    if not row:
        raise ValueError("No KPI runs found.")

    return {
        "kpi_run_id": row[0],
        "validation_run_id": row[1],
        "generated_at": row[2],
        "status": row[3],
        "kpi_values": json.loads(row[4]) if row[4] else {},
    }


def get_executive_dashboard() -> dict:
    latest_kpi = get_latest_kpi_run()

    with get_conn() as conn:
        trust = conn.execute(
            """
            SELECT validation_run_id, trust_score, dimension_scores_json, status
            FROM validation_runs
            ORDER BY started_at DESC
            LIMIT 1
            """
        ).fetchone()

    trust_payload = None
    if trust:
        trust_payload = {
            "validation_run_id": trust[0],
            "trust_score": trust[1],
            "dimension_scores": json.loads(trust[2]) if trust[2] else {},
            "status": trust[3],
        }

    cards = [
        {"kpi_code": code, "value": value}
        for code, value in latest_kpi["kpi_values"].items()
    ]

    return {
        "kpi_run_id": latest_kpi["kpi_run_id"],
        "generated_at": latest_kpi["generated_at"],
        "cards": cards,
        "trust_context": trust_payload,
    }

#!/usr/bin/env python3
"""
local_demo.py

Zero-dependency, end-to-end run of the ELT logic against SQLite instead
of S3/Glue/Redshift, so the pipeline's correctness can be verified with
nothing installed beyond the Python standard library + pandas. It
implements the *same* staging -> star-schema transform that
sql/redshift/02_dimension_tables.sql + dbt/retail_dw/models/marts encode
for production, so passing here is a real signal the logic is sound —
this is a local dev/demo convenience, not a replacement for the
Redshift + dbt + Airflow production path described in the README.

Usage:
    python scripts/generate_sample_data.py --rows 5000
    python warehouse/local_demo.py
"""
import hashlib
import logging
import sqlite3
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent / "local_demo.db"
RAW_DIR = Path(__file__).parent.parent / "data" / "raw"


def surrogate_key(natural_key: str) -> str:
    """Same idea as the md5() surrogate keys the dbt marts models use."""
    return hashlib.md5(natural_key.encode("utf-8")).hexdigest()


def load_staging(conn, raw_dir: Path):
    for table in ["customers", "stores", "products", "orders"]:
        path = raw_dir / f"{table}.csv"
        df = pd.read_csv(path)
        df.to_sql(f"stg_{table}", conn, if_exists="replace", index=False)
        logger.info("staged %-9s %5d rows <- %s", table, len(df), path)


def build_star_schema(conn):
    cur = conn.cursor()

    # ---- dim_customer -------------------------------------------------
    customers = pd.read_sql("SELECT * FROM stg_customers", conn)
    customers = customers.drop_duplicates(subset="customer_id", keep="last")
    customers["customer_key"] = customers["customer_id"].apply(surrogate_key)
    customers.to_sql("dim_customer", conn, if_exists="replace", index=False)

    # ---- dim_product ----------------------------------------------------
    products = pd.read_sql("SELECT * FROM stg_products", conn)
    products = products[products["unit_price"] >= 0]
    products["product_key"] = products["product_id"].apply(surrogate_key)
    products["margin_pct"] = (
        (products["unit_price"] - products["unit_cost"]) / products["unit_price"].replace(0, pd.NA) * 100
    ).round(2).fillna(0)
    products.to_sql("dim_product", conn, if_exists="replace", index=False)

    # ---- dim_store ------------------------------------------------------
    stores = pd.read_sql("SELECT * FROM stg_stores", conn)
    stores["store_key"] = stores["store_id"].apply(surrogate_key)
    stores.to_sql("dim_store", conn, if_exists="replace", index=False)

    # ---- dim_date (only the dates actually present in the order data) ---
    orders = pd.read_sql("SELECT * FROM stg_orders", conn)
    dates = pd.to_datetime(orders["order_date"]).dt.date.drop_duplicates().sort_values()
    dim_date = pd.DataFrame({"full_date": dates})
    dim_date["date_key"] = dim_date["full_date"].apply(lambda d: int(d.strftime("%Y%m%d")))
    dim_date["day_of_week"] = dim_date["full_date"].apply(lambda d: d.strftime("%A"))
    dim_date["month_num"] = dim_date["full_date"].apply(lambda d: d.month)
    dim_date["month_name"] = dim_date["full_date"].apply(lambda d: d.strftime("%B"))
    dim_date["quarter"] = dim_date["full_date"].apply(lambda d: (d.month - 1) // 3 + 1)
    dim_date["year"] = dim_date["full_date"].apply(lambda d: d.year)
    dim_date["is_weekend"] = dim_date["full_date"].apply(lambda d: d.weekday() >= 5)
    dim_date["full_date"] = dim_date["full_date"].astype(str)
    dim_date.to_sql("dim_date", conn, if_exists="replace", index=False)

    # ---- fact_sales -------------------------------------------------------
    orders["date_key"] = pd.to_datetime(orders["order_date"]).dt.strftime("%Y%m%d").astype(int)
    fact = orders.merge(customers[["customer_id", "customer_key"]], on="customer_id", how="left")
    fact = fact.merge(products[["product_id", "product_key"]], on="product_id", how="left")
    fact = fact.merge(stores[["store_id", "store_key"]], on="store_id", how="left")
    fact_cols = ["order_id", "date_key", "customer_key", "product_key", "store_key",
                 "quantity", "unit_price", "discount_pct", "gross_amount",
                 "discount_amount", "net_amount", "channel", "payment_method"]
    fact = fact[fact_cols]
    fact.to_sql("fact_sales", conn, if_exists="replace", index=False)

    logger.info("built star schema: dim_customer=%d dim_product=%d dim_store=%d dim_date=%d fact_sales=%d",
                len(customers), len(products), len(stores), len(dim_date), len(fact))
    return fact


def build_bi_views(conn):
    cur = conn.cursor()
    cur.executescript("""
    DROP VIEW IF EXISTS vw_daily_sales;
    CREATE VIEW vw_daily_sales AS
    SELECT
        d.full_date,
        d.year,
        d.month_name,
        COUNT(DISTINCT f.order_id)                       AS order_count,
        SUM(f.quantity)                                   AS units_sold,
        ROUND(SUM(f.net_amount), 2)                       AS net_revenue,
        ROUND(SUM(f.net_amount) - SUM(f.quantity * p.unit_cost), 2) AS gross_margin
    FROM fact_sales f
    JOIN dim_date d    ON f.date_key = d.date_key
    JOIN dim_product p ON f.product_key = p.product_key
    GROUP BY d.full_date, d.year, d.month_name;

    DROP VIEW IF EXISTS vw_top_products;
    CREATE VIEW vw_top_products AS
    SELECT
        p.product_id, p.product_name, p.category,
        SUM(f.quantity) AS units_sold,
        ROUND(SUM(f.net_amount), 2) AS net_revenue,
        RANK() OVER (ORDER BY SUM(f.net_amount) DESC) AS revenue_rank
    FROM fact_sales f
    JOIN dim_product p ON f.product_key = p.product_key
    GROUP BY p.product_id, p.product_name, p.category;

    DROP VIEW IF EXISTS vw_customer_segments;
    CREATE VIEW vw_customer_segments AS
    SELECT
        c.customer_segment, c.country,
        COUNT(DISTINCT c.customer_key) AS customer_count,
        COUNT(DISTINCT f.order_id)     AS order_count,
        ROUND(SUM(f.net_amount), 2)    AS net_revenue,
        ROUND(SUM(f.net_amount) * 1.0 / NULLIF(COUNT(DISTINCT c.customer_key), 0), 2) AS revenue_per_customer
    FROM dim_customer c
    LEFT JOIN fact_sales f ON c.customer_key = f.customer_key
    GROUP BY c.customer_segment, c.country;

    DROP VIEW IF EXISTS vw_store_performance;
    CREATE VIEW vw_store_performance AS
    SELECT
        s.store_id, s.store_name, s.city, s.country,
        COUNT(DISTINCT f.order_id) AS order_count,
        ROUND(SUM(f.net_amount), 2) AS net_revenue
    FROM fact_sales f
    JOIN dim_store s ON f.store_key = s.store_key
    GROUP BY s.store_id, s.store_name, s.city, s.country;
    """)
    conn.commit()
    logger.info("created BI views: vw_daily_sales, vw_top_products, vw_customer_segments, vw_store_performance")


def run_data_quality_checks(conn) -> list[str]:
    """Mirrors etl/data_quality_checks.py + the dbt schema tests, against SQLite."""
    failures = []
    checks = {
        "no_null_fact_keys": "SELECT COUNT(*) FROM fact_sales WHERE customer_key IS NULL "
                              "OR product_key IS NULL OR store_key IS NULL OR date_key IS NULL",
        "no_duplicate_orders": "SELECT COUNT(*) FROM (SELECT order_id FROM fact_sales "
                                "GROUP BY order_id HAVING COUNT(*) > 1)",
        "orphaned_customer_keys": "SELECT COUNT(*) FROM fact_sales f LEFT JOIN dim_customer c "
                                   "ON f.customer_key = c.customer_key WHERE c.customer_key IS NULL",
        "orphaned_product_keys": "SELECT COUNT(*) FROM fact_sales f LEFT JOIN dim_product p "
                                  "ON f.product_key = p.product_key WHERE p.product_key IS NULL",
        "negative_net_amount": "SELECT COUNT(*) FROM fact_sales WHERE net_amount < 0",
        "duplicate_customer_ids": "SELECT COUNT(*) FROM (SELECT customer_id FROM dim_customer "
                                   "GROUP BY customer_id HAVING COUNT(*) > 1)",
    }
    cur = conn.cursor()
    for name, sql in checks.items():
        bad = cur.execute(sql).fetchone()[0]
        if bad > 0:
            failures.append(f"{name}: {bad} failing row(s)")
        else:
            logger.info("PASS  %s", name)
    return failures


def main():
    if not RAW_DIR.exists() or not (RAW_DIR / "orders.csv").exists():
        raise SystemExit(
            "no sample data found — run: python scripts/generate_sample_data.py --rows 5000"
        )

    if DB_PATH.exists():
        DB_PATH.unlink()

    conn = sqlite3.connect(DB_PATH)
    try:
        load_staging(conn, RAW_DIR)
        build_star_schema(conn)
        build_bi_views(conn)

        failures = run_data_quality_checks(conn)
        if failures:
            logger.error("data quality gate FAILED:")
            for f in failures:
                logger.error("  - %s", f)
            raise SystemExit(1)

        logger.info("data quality gate PASSED")

        # Print a couple of KPI samples so the demo is self-verifying end to end.
        print("\nTop 5 products by revenue:")
        print(pd.read_sql("SELECT * FROM vw_top_products ORDER BY revenue_rank LIMIT 5", conn).to_string(index=False))

        print("\nRevenue by customer segment:")
        print(pd.read_sql("SELECT * FROM vw_customer_segments ORDER BY net_revenue DESC", conn).to_string(index=False))

        print(f"\nlocal demo warehouse written to: {DB_PATH}")
        print("Try the API against it:  DATABASE_MODE=local uvicorn api.main:app --reload")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

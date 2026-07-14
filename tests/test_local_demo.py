"""
Runs the full local (SQLite) ELT pipeline against a small synthetic
dataset and asserts the star schema and BI views come out consistent.
This is the CI-facing equivalent of the dbt schema tests in
dbt/retail_dw/models/**/schema.yml — same invariants, checked in plain
Python + SQLite so they run in any CI runner with zero setup.

Run:  pytest tests/test_local_demo.py -v
"""
import sqlite3
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from scripts.generate_sample_data import gen_customers, gen_orders, gen_products, gen_stores
from warehouse.local_demo import build_bi_views, build_star_schema, run_data_quality_checks


@pytest.fixture()
def demo_conn(tmp_path):
    from datetime import date

    customers = gen_customers(50)
    stores = gen_stores(4)
    products = gen_products(20)
    orders = gen_orders(300, customers, stores, products, date(2026, 1, 1), date(2026, 6, 30))

    conn = sqlite3.connect(":memory:")
    pd.DataFrame(customers).to_sql("stg_customers", conn, index=False)
    pd.DataFrame(stores).to_sql("stg_stores", conn, index=False)
    pd.DataFrame(products).to_sql("stg_products", conn, index=False)
    pd.DataFrame(orders).to_sql("stg_orders", conn, index=False)

    build_star_schema(conn)
    build_bi_views(conn)
    yield conn
    conn.close()


def test_fact_row_count_matches_source_orders(demo_conn):
    fact_count = demo_conn.execute("SELECT COUNT(*) FROM fact_sales").fetchone()[0]
    orders_count = demo_conn.execute("SELECT COUNT(*) FROM stg_orders").fetchone()[0]
    assert fact_count == orders_count


def test_no_orphaned_foreign_keys(demo_conn):
    orphans = demo_conn.execute("""
        SELECT COUNT(*) FROM fact_sales f
        LEFT JOIN dim_customer c ON f.customer_key = c.customer_key
        WHERE c.customer_key IS NULL
    """).fetchone()[0]
    assert orphans == 0


def test_dimension_natural_keys_are_unique(demo_conn):
    for table, key in [("dim_customer", "customer_id"), ("dim_product", "product_id"),
                        ("dim_store", "store_id")]:
        total = demo_conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        distinct = demo_conn.execute(f"SELECT COUNT(DISTINCT {key}) FROM {table}").fetchone()[0]
        assert total == distinct, f"{table}.{key} has duplicates"


def test_net_amount_is_never_negative(demo_conn):
    negative = demo_conn.execute("SELECT COUNT(*) FROM fact_sales WHERE net_amount < 0").fetchone()[0]
    assert negative == 0


def test_bi_views_are_queryable_and_non_empty(demo_conn):
    for view in ["vw_daily_sales", "vw_top_products", "vw_customer_segments", "vw_store_performance"]:
        rows = demo_conn.execute(f"SELECT COUNT(*) FROM {view}").fetchone()[0]
        assert rows > 0, f"{view} returned no rows"


def test_data_quality_gate_passes_on_clean_data(demo_conn):
    failures = run_data_quality_checks(demo_conn)
    assert failures == []

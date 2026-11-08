"""
Unit tests for warehouse.local_demo.clean_orders, which must apply the
same rules as dbt/retail_dw/models/staging/stg_orders.sql so the local
(SQLite) pipeline and the dbt/Redshift pipeline agree on dirty input.

Run:  pytest tests/test_clean_orders.py -v
"""
import sqlite3
import sys
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from scripts.generate_sample_data import gen_customers, gen_orders, gen_products, gen_stores
from warehouse.local_demo import build_star_schema, clean_orders, run_data_quality_checks


def order_line(**overrides):
    line = {
        "order_id": "O000001",
        "order_date": "2026-03-01",
        "customer_id": "C0001",
        "store_id": "S001",
        "product_id": "P0001",
        "quantity": 2,
        "unit_price": 10.0,
        "discount_pct": 0,
        "gross_amount": 20.0,
        "discount_amount": 0.0,
        "net_amount": 20.0,
        "channel": "web",
        "payment_method": "card",
    }
    line.update(overrides)
    return line


def test_clean_lines_pass_through_unchanged():
    result = clean_orders(pd.DataFrame([order_line()]))
    assert len(result) == 1
    assert result.iloc[0]["order_id"] == "O000001"
    assert result.iloc[0]["net_amount"] == 20.0


def test_drops_null_order_id():
    df = pd.DataFrame([order_line(), order_line(order_id=None)])
    assert list(clean_orders(df)["order_id"]) == ["O000001"]


@pytest.mark.parametrize("quantity", [0, -1, None])
def test_drops_non_positive_or_missing_quantity(quantity):
    df = pd.DataFrame([order_line(), order_line(order_id="O000002", quantity=quantity)])
    assert list(clean_orders(df)["order_id"]) == ["O000001"]


def test_drops_negative_net_amount_but_keeps_zero():
    df = pd.DataFrame([
        order_line(),
        order_line(order_id="REFUND", net_amount=-25.0),
        order_line(order_id="FREE", net_amount=0.0),
    ])
    assert list(clean_orders(df)["order_id"]) == ["O000001", "FREE"]


def test_trims_id_columns():
    df = pd.DataFrame([order_line(order_id=" O000001 ", customer_id=" C0001",
                                  store_id="S001 ", product_id="\tP0001")])
    row = clean_orders(df).iloc[0]
    assert (row["order_id"], row["customer_id"], row["store_id"], row["product_id"]) == \
        ("O000001", "C0001", "S001", "P0001")


def test_normalises_channel_and_payment_method():
    df = pd.DataFrame([order_line(channel=" Online ", payment_method="CARD")])
    row = clean_orders(df).iloc[0]
    assert (row["channel"], row["payment_method"]) == ("online", "card")


def test_rounds_money_columns_to_two_decimals():
    df = pd.DataFrame([order_line(unit_price=10.005, gross_amount=20.0149,
                                  discount_amount=1.239, net_amount=18.7759)])
    row = clean_orders(df).iloc[0]
    assert row["gross_amount"] == 20.01
    assert row["discount_amount"] == 1.24
    assert row["net_amount"] == 18.78


def test_does_not_mutate_input_frame():
    df = pd.DataFrame([order_line(channel=" Web ")])
    clean_orders(df)
    assert df.iloc[0]["channel"] == " Web "


def test_quality_gate_passes_when_raw_orders_contain_dirty_lines():
    customers = gen_customers(20)
    stores = gen_stores(3)
    products = gen_products(10)
    orders = gen_orders(50, customers, stores, products, date(2026, 1, 1), date(2026, 1, 31))
    orders[0] = dict(orders[0], order_id="RET-1", quantity=-1, net_amount=-25.0)
    orders[1] = dict(orders[1], order_id=None)
    orders[2] = dict(orders[2], customer_id=" " + orders[2]["customer_id"], channel=" Online ")

    conn = sqlite3.connect(":memory:")
    try:
        for name, rows in [("customers", customers), ("stores", stores),
                           ("products", products), ("orders", orders)]:
            pd.DataFrame(rows).to_sql(f"stg_{name}", conn, index=False)

        build_star_schema(conn)

        assert run_data_quality_checks(conn) == []
        assert conn.execute("SELECT COUNT(*) FROM fact_sales").fetchone()[0] == 48
        assert conn.execute("SELECT COUNT(*) FROM fact_sales WHERE order_id IS NULL").fetchone()[0] == 0
        channels = {r[0] for r in conn.execute("SELECT DISTINCT channel FROM fact_sales")}
        assert " Online " not in channels
    finally:
        conn.close()

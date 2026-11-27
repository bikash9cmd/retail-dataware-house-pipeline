"""
Tests for the FastAPI layer (api/).

The local tests build a small demo warehouse on disk and call every
endpoint through FastAPI's TestClient. The Redshift tests swap in a fake
psycopg2-style connection so the SQL the API would send to the cluster
(placeholder style, schema-qualified view names) and the handling of
Redshift's native types can be checked without a live cluster.

Run:  pytest tests/test_api.py -v
"""
import sqlite3
import sys
from contextlib import contextmanager
from datetime import date
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")
from fastapi.testclient import TestClient  # noqa: E402

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from api import db  # noqa: E402
from api.main import app  # noqa: E402
from scripts.generate_sample_data import gen_customers, gen_orders, gen_products, gen_stores  # noqa: E402
from warehouse.local_demo import build_bi_views, build_star_schema  # noqa: E402


@pytest.fixture()
def local_client(tmp_path, monkeypatch):
    customers = gen_customers(40)
    stores = gen_stores(3)
    products = gen_products(15)
    orders = gen_orders(250, customers, stores, products, date(2026, 1, 1), date(2026, 3, 31))

    db_path = tmp_path / "warehouse.db"
    conn = sqlite3.connect(db_path)
    pd.DataFrame(customers).to_sql("stg_customers", conn, index=False)
    pd.DataFrame(stores).to_sql("stg_stores", conn, index=False)
    pd.DataFrame(products).to_sql("stg_products", conn, index=False)
    pd.DataFrame(orders).to_sql("stg_orders", conn, index=False)
    build_star_schema(conn)
    build_bi_views(conn)
    conn.close()

    monkeypatch.setattr(db, "DATABASE_MODE", "local")
    monkeypatch.setattr(db, "SQLITE_PATH", str(db_path))
    return TestClient(app)


# ---------------------------------------------------------------- local mode

def test_health_reports_mode(local_client):
    resp = local_client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok", "database_mode": "local"}


def test_daily_sales_respects_limit_and_order(local_client):
    resp = local_client.get("/kpis/daily-sales", params={"limit": 5})
    assert resp.status_code == 200
    rows = resp.json()
    assert len(rows) == 5
    dates = [r["full_date"] for r in rows]
    assert dates == sorted(dates, reverse=True)
    # still serialised as an ISO date string
    date.fromisoformat(dates[0])


def test_top_products_ranked(local_client):
    rows = local_client.get("/kpis/top-products", params={"limit": 3}).json()
    assert [r["revenue_rank"] for r in rows] == sorted(r["revenue_rank"] for r in rows)
    assert len(rows) == 3


def test_customer_segments_and_store_performance(local_client):
    segments = local_client.get("/kpis/customer-segments").json()
    assert segments and {"customer_segment", "country", "net_revenue"} <= segments[0].keys()
    stores = local_client.get("/kpis/store-performance").json()
    assert len(stores) == 3


@pytest.mark.parametrize("path,bad", [
    ("/kpis/daily-sales", 0),
    ("/kpis/daily-sales", 366),
    ("/kpis/top-products", 101),
    ("/kpis/store-performance", -1),
])
def test_limit_out_of_range_is_rejected(local_client, path, bad):
    assert local_client.get(path, params={"limit": bad}).status_code == 422


def test_top_products_404_when_warehouse_empty(local_client, monkeypatch):
    monkeypatch.setattr("api.main.run_query", lambda sql, params=(): [])
    assert local_client.get("/kpis/top-products").status_code == 404


# ------------------------------------------------------------- redshift mode

class FakeCursor:
    def __init__(self, rows, log):
        self._rows, self._log = rows, log

    def execute(self, sql, params=None):
        # psycopg2 raises on qmark-style SQL with parameters; mimic that.
        if params is not None:
            sql % tuple(params)
        self._log.append((sql, params))

    def fetchall(self):
        return self._rows


@pytest.fixture()
def redshift(monkeypatch):
    state = {"rows": [], "log": []}

    @contextmanager
    def fake_connection():
        class Conn:
            def cursor(self):
                return FakeCursor(state["rows"], state["log"])
        yield Conn()

    monkeypatch.setattr(db, "DATABASE_MODE", "redshift")
    monkeypatch.setattr(db, "get_connection", fake_connection)
    return state


def test_redshift_queries_use_pyformat_and_analytics_schema(redshift):
    client = TestClient(app)
    for path in ("/kpis/daily-sales", "/kpis/store-performance"):
        assert client.get(path, params={"limit": 7}).status_code == 200
    client.get("/kpis/customer-segments")

    (daily_sql, daily_params), (store_sql, _), (seg_sql, seg_params) = redshift["log"]
    assert "FROM analytics.vw_daily_sales" in daily_sql
    assert daily_sql.endswith("LIMIT %s") and daily_params == (7,)
    assert "FROM analytics.vw_store_performance" in store_sql
    assert "FROM analytics.vw_customer_segments" in seg_sql and seg_params is None


def test_redshift_native_types_validate(redshift):
    redshift["rows"] = [{
        "full_date": date(2026, 3, 1), "year": 2026, "month_name": "March",
        "order_count": 4, "units_sold": 9,
        "net_revenue": Decimal("123.45"), "gross_margin": Decimal("40.10"),
    }]
    resp = TestClient(app).get("/kpis/daily-sales")
    assert resp.status_code == 200
    assert resp.json()[0]["full_date"] == "2026-03-01"
    assert resp.json()[0]["net_revenue"] == pytest.approx(123.45)


def test_to_backend_sql_escapes_literal_percent():
    sql, params = db.to_backend_sql("SELECT 'a%' WHERE x = ?", "redshift", (1,))
    assert sql == "SELECT 'a%%' WHERE x = %s" and params == (1,)
    assert db.to_backend_sql("SELECT 1 WHERE x = ?", "local", (1,)) == ("SELECT 1 WHERE x = ?", (1,))

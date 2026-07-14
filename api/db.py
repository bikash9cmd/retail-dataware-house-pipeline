"""
db.py

Thin connection helper for the API layer. Reads from Redshift's
analytics.vw_* views in production (never the base tables directly, so
the physical model can change without breaking API clients); falls back
to the local SQLite demo warehouse (warehouse/local_demo.db) when
DATABASE_MODE=local, so the API is runnable without any AWS access.
"""
import os
import sqlite3
from contextlib import contextmanager

DATABASE_MODE = os.getenv("DATABASE_MODE", "local")  # "local" | "redshift"
SQLITE_PATH = os.getenv("SQLITE_PATH", "warehouse/local_demo.db")


@contextmanager
def get_connection():
    if DATABASE_MODE == "redshift":
        import psycopg2
        import psycopg2.extras
        from etl.config import REDSHIFT
        conn = psycopg2.connect(REDSHIFT.dsn, cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            yield conn
        finally:
            conn.close()
    else:
        conn = sqlite3.connect(SQLITE_PATH)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()


def run_query(sql: str, params: tuple = ()) -> list[dict]:
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()
        return [dict(row) for row in rows]

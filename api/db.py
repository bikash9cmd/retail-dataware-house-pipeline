"""
db.py

Thin connection helper for the API layer. Reads from Redshift's
analytics.vw_* views in production (never the base tables directly, so
the physical model can change without breaking API clients); falls back
to the local SQLite demo warehouse (warehouse/local_demo.db) when
DATABASE_MODE=local, so the API is runnable without any AWS access.

Queries are written once, backend-neutral:
  * parameters use the ``?`` placeholder (translated to ``%s`` for psycopg2)
  * views are referenced through ``view("vw_name")``, which adds the
    analytics schema on Redshift and leaves the name bare for SQLite
"""
import os
import sqlite3
from contextlib import contextmanager

DATABASE_MODE = os.getenv("DATABASE_MODE", "local")  # "local" | "redshift"
SQLITE_PATH = os.getenv("SQLITE_PATH", "warehouse/local_demo.db")
ANALYTICS_SCHEMA = os.getenv("ANALYTICS_SCHEMA", "analytics")


def view(name: str) -> str:
    """Return the BI view name qualified for the active backend.

    The Redshift DDL creates the views in the analytics schema, which is
    not on the default search_path; the SQLite demo has no schemas.
    """
    if DATABASE_MODE == "redshift":
        return f"{ANALYTICS_SCHEMA}.{name}"
    return name


def to_backend_sql(sql: str, mode: str, params: tuple = ()) -> tuple[str, tuple | None]:
    """Translate a ``?``-placeholder query into the active driver's paramstyle.

    sqlite3 uses qmark (``?``); psycopg2 uses pyformat (``%s``), where a
    literal ``%`` must also be doubled. psycopg2 only interpolates when it
    is given parameters, so a parameterless query is passed through as-is.
    """
    if mode != "redshift":
        return sql, params
    if not params:
        return sql, None
    return sql.replace("%", "%%").replace("?", "%s"), params


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
    """Run a read-only query written with ``?`` placeholders on the active backend."""
    with get_connection() as conn:
        cur = conn.cursor()
        backend_sql, backend_params = to_backend_sql(sql, DATABASE_MODE, params)
        if backend_params is None:
            cur.execute(backend_sql)
        else:
            cur.execute(backend_sql, backend_params)
        rows = cur.fetchall()
        return [dict(row) for row in rows]

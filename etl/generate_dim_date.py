#!/usr/bin/env python3
"""
generate_dim_date.py

Populates analytics.dim_date with one row per calendar day. Run once at
setup and whenever the date range needs extending — a plain calendar
dimension doesn't belong in dbt's incremental-model machinery, so it's
kept as a small standalone utility, same as most Kimball-style
warehouses do in practice.

Production use:
    python etl/generate_dim_date.py --start 2023-01-01 --end 2028-12-31
"""
import argparse
import logging
from datetime import date, timedelta

from etl.config import REDSHIFT

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

INSERT_SQL = """
    INSERT INTO analytics.dim_date
        (date_key, full_date, day_of_week, day_num, month_num, month_name, quarter, year, is_weekend)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


def build_rows(start: date, end: date):
    rows = []
    current = start
    while current <= end:
        rows.append((
            int(current.strftime("%Y%m%d")),
            current.isoformat(),
            current.strftime("%A"),
            current.day,
            current.month,
            current.strftime("%B"),
            (current.month - 1) // 3 + 1,
            current.year,
            current.weekday() >= 5,
        ))
        current += timedelta(days=1)
    return rows


def main():
    import psycopg2

    parser = argparse.ArgumentParser(description="Populate analytics.dim_date.")
    parser.add_argument("--start", default="2023-01-01")
    parser.add_argument("--end", default="2028-12-31")
    args = parser.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    rows = build_rows(start, end)

    conn = psycopg2.connect(REDSHIFT.dsn)
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM analytics.dim_date WHERE date_key BETWEEN %s AND %s",
                        (rows[0][0], rows[-1][0]))
            cur.executemany(INSERT_SQL, rows)
        conn.commit()
        logger.info("dim_date populated: %d rows (%s -> %s)", len(rows), start, end)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

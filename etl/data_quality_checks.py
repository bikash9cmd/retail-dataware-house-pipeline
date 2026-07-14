#!/usr/bin/env python3
"""
data_quality_checks.py

Standalone data-quality gate, run by Airflow immediately after the dbt
`run`/`test` step. dbt's schema tests (dbt/retail_dw/models/**/schema.yml)
already cover column-level not-null/unique/relationships checks; this
script adds pipeline-level checks — freshness and row-count sanity —
that are awkward to express as a single dbt test, and fails the DAG
loudly (non-zero exit) if anything is out of bounds.

Usage:
    python etl/data_quality_checks.py --run-date 2026-07-14
"""
import argparse
import logging
import sys
from datetime import date

from etl.config import REDSHIFT

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


CHECKS = {
    "no_null_fact_keys": """
        SELECT COUNT(*) FROM analytics.fact_sales
        WHERE customer_key IS NULL OR product_key IS NULL
           OR store_key IS NULL OR date_key IS NULL
    """,
    "no_duplicate_orders": """
        SELECT COUNT(*) FROM (
            SELECT order_id FROM analytics.fact_sales
            GROUP BY order_id HAVING COUNT(*) > 1
        ) dupes
    """,
    "orphaned_customer_keys": """
        SELECT COUNT(*) FROM analytics.fact_sales f
        LEFT JOIN analytics.dim_customer c ON f.customer_key = c.customer_key
        WHERE c.customer_key IS NULL
    """,
    "negative_net_amount": """
        SELECT COUNT(*) FROM analytics.fact_sales WHERE net_amount < 0
    """,
}

MIN_EXPECTED_ROWS = 1


def run_checks(run_date: str) -> list[str]:
    import psycopg2

    failures = []
    conn = psycopg2.connect(REDSHIFT.dsn)
    try:
        with conn.cursor() as cur:
            for name, sql in CHECKS.items():
                cur.execute(sql)
                bad_count = cur.fetchone()[0]
                if bad_count > 0:
                    failures.append(f"{name}: {bad_count} failing row(s)")
                else:
                    logger.info("PASS  %s", name)

            cur.execute("SELECT COUNT(*) FROM analytics.fact_sales f "
                        "JOIN analytics.dim_date d ON f.date_key = d.date_key "
                        "WHERE d.full_date = %s", (run_date,))
            loaded_today = cur.fetchone()[0]
            if loaded_today < MIN_EXPECTED_ROWS:
                failures.append(f"freshness: expected at least {MIN_EXPECTED_ROWS} row(s) "
                                 f"loaded for {run_date}, found {loaded_today}")
            else:
                logger.info("PASS  freshness (%d rows for %s)", loaded_today, run_date)
    finally:
        conn.close()

    return failures


def main():
    parser = argparse.ArgumentParser(description="Post-load data quality gate.")
    parser.add_argument("--run-date", default=date.today().isoformat())
    args = parser.parse_args()

    failures = run_checks(args.run_date)
    if failures:
        logger.error("data quality gate FAILED:")
        for f in failures:
            logger.error("  - %s", f)
        sys.exit(1)

    logger.info("data quality gate PASSED for run_date=%s", args.run_date)


if __name__ == "__main__":
    main()

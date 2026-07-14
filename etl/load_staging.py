#!/usr/bin/env python3
"""
load_staging.py

The "load" leg of ELT: bulk-loads S3 raw-zone CSVs into Redshift staging
tables using the Redshift COPY command (far faster than row-by-row
INSERTs for this volume). This is the step a Glue Python-Shell job or
an Airflow PythonOperator/GlueJobOperator triggers after extract_s3.py
has landed files in S3.

Production use:
    python etl/load_staging.py --run-date 2026-07-14

Requires psycopg2 + network access to the Redshift cluster.
"""
import argparse
import logging
from datetime import date

from etl.config import REDSHIFT, S3, STAGING_TABLES

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

COPY_SQL = """
    COPY staging.{table}
    FROM 's3://{bucket}/{prefix}{table}/run_date={run_date}/{table}.csv'
    IAM_ROLE '{iam_role}'
    CSV
    IGNOREHEADER 1
    TIMEFORMAT 'auto'
    DATEFORMAT 'auto'
    BLANKSASNULL
    EMPTYASNULL;
"""

TRUNCATE_SQL = "TRUNCATE TABLE staging.{table};"


def load_table(cursor, table: str, run_date: str):
    cursor.execute(TRUNCATE_SQL.format(table=table))
    cursor.execute(COPY_SQL.format(
        table=table,
        bucket=S3.raw_bucket,
        prefix=S3.raw_prefix,
        run_date=run_date,
        iam_role=REDSHIFT.iam_role_arn,
    ))
    logger.info("loaded staging.%s for run_date=%s", table, run_date)


def main():
    import psycopg2  # imported lazily; not required to import/lint this module

    parser = argparse.ArgumentParser(description="Load S3 raw-zone CSVs into Redshift staging tables.")
    parser.add_argument("--run-date", default=date.today().isoformat())
    args = parser.parse_args()

    conn = psycopg2.connect(REDSHIFT.dsn)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            for table in STAGING_TABLES:
                load_table(cur, table, args.run_date)
        conn.commit()
        logger.info("staging load committed for run_date=%s", args.run_date)
    except Exception:
        conn.rollback()
        logger.exception("staging load failed, transaction rolled back")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()

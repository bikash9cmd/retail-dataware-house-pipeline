#!/usr/bin/env python3
"""
extract_s3.py

Simulates the "extract" leg of the ELT pipeline: source-system exports
(here, CSVs from data/raw/) are uploaded to the S3 raw zone, partitioned
by ingestion date, exactly as an upstream OLTP export or CDC job would
drop files for Glue/Airflow to pick up.

Production use:
    python etl/extract_s3.py --source data/raw --run-date 2026-07-14

Requires boto3 + valid AWS credentials (IAM role or ~/.aws/credentials).
For a fully offline run of the pipeline logic, see warehouse/local_demo.py.
"""
import argparse
import logging
from datetime import date
from pathlib import Path

from etl.config import S3, STAGING_TABLES

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def upload_to_s3(source_dir: Path, run_date: str) -> list[str]:
    """Upload each staging CSV to s3://<raw_bucket>/<prefix>/<table>/run_date=<date>/file.csv"""
    import boto3  # imported lazily so the module can be inspected/tested without boto3 installed

    s3 = boto3.client("s3", region_name=S3.region)
    uploaded = []

    for table in STAGING_TABLES:
        local_path = source_dir / f"{table}.csv"
        if not local_path.exists():
            logger.warning("skip %s (no local file at %s)", table, local_path)
            continue

        key = f"{S3.raw_prefix}{table}/run_date={run_date}/{table}.csv"
        s3.upload_file(str(local_path), S3.raw_bucket, key)
        uploaded.append(f"s3://{S3.raw_bucket}/{key}")
        logger.info("uploaded %s -> s3://%s/%s", local_path, S3.raw_bucket, key)

    return uploaded


def main():
    parser = argparse.ArgumentParser(description="Extract source CSVs into the S3 raw zone.")
    parser.add_argument("--source", default="data/raw", help="local directory containing source CSVs")
    parser.add_argument("--run-date", default=date.today().isoformat(), help="logical run date (YYYY-MM-DD)")
    args = parser.parse_args()

    source_dir = Path(args.source)
    if not source_dir.exists():
        raise SystemExit(f"source directory not found: {source_dir} "
                          f"(run scripts/generate_sample_data.py first)")

    uploaded = upload_to_s3(source_dir, args.run_date)
    logger.info("extract complete: %d file(s) uploaded", len(uploaded))


if __name__ == "__main__":
    main()

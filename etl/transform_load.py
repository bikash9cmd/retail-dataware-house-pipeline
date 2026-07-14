#!/usr/bin/env python3
"""
transform_load.py

Thin automation wrapper around dbt: runs `dbt run` (staging -> marts
transform) followed by `dbt test` (schema + data tests), and fails loudly
if either step errors. This is the task the Airflow DAG (dags/retail_dw_pipeline_dag.py)
invokes after the staging load succeeds — kept as its own script so it
can also be run by hand or from a CI job without spinning up Airflow.

Usage:
    python etl/transform_load.py --target prod
"""
import argparse
import logging
import subprocess
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DBT_PROJECT_DIR = "dbt/retail_dw"


def run_dbt_command(args: list[str], target: str) -> None:
    cmd = ["dbt", *args, "--project-dir", DBT_PROJECT_DIR, "--profiles-dir", DBT_PROJECT_DIR, "--target", target]
    logger.info("running: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True)
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError(f"dbt command failed ({' '.join(cmd)}), exit code {result.returncode}")


def main():
    parser = argparse.ArgumentParser(description="Run the dbt transform + test layer.")
    parser.add_argument("--target", default="prod", choices=["dev", "prod"])
    parser.add_argument("--skip-tests", action="store_true")
    args = parser.parse_args()

    try:
        run_dbt_command(["run"], args.target)
        if not args.skip_tests:
            run_dbt_command(["test"], args.target)
    except RuntimeError as exc:
        logger.error(str(exc))
        sys.exit(1)

    logger.info("dbt transform + test completed successfully for target=%s", args.target)


if __name__ == "__main__":
    main()

"""
retail_dw_pipeline_dag.py

Daily orchestration for the retail data warehouse ELT pipeline:

    extract_to_s3 -> load_staging -> [dbt run, generate_dim_date] -> dbt_test
        -> data_quality_gate -> refresh_product_margin -> notify_success

Each Python step reuses the same etl/*.py modules that can also be run
by hand (see README "Running the pipeline"), so there is exactly one
implementation of the ETL logic — Airflow just schedules and monitors it.

Deploy by copying this file into your Airflow DAGs folder and mounting
the repo root (or installing it as a package) so `from etl...` imports
resolve, and by setting the Airflow Variables / Connections listed
under "Required configuration" below.

Required configuration (Airflow Variables or env vars, matching etl/config.py):
    RAW_BUCKET, CURATED_BUCKET, RAW_PREFIX, AWS_REGION
    REDSHIFT_HOST, REDSHIFT_PORT, REDSHIFT_DB, REDSHIFT_USER,
    REDSHIFT_PASSWORD, REDSHIFT_COPY_ROLE_ARN
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["data-alerts@example.com"],
}


def _extract_to_s3(**context):
    from etl.extract_s3 import upload_to_s3
    from pathlib import Path
    run_date = context["ds"]
    uploaded = upload_to_s3(Path("data/raw"), run_date)
    context["ti"].xcom_push(key="uploaded_files", value=uploaded)


def _load_staging(**context):
    import psycopg2
    from etl.config import REDSHIFT, STAGING_TABLES
    from etl.load_staging import load_table

    run_date = context["ds"]
    conn = psycopg2.connect(REDSHIFT.dsn)
    try:
        with conn.cursor() as cur:
            for table in STAGING_TABLES:
                load_table(cur, table, run_date)
        conn.commit()
    finally:
        conn.close()


def _generate_dim_date(**context):
    from etl.generate_dim_date import build_rows, INSERT_SQL
    from etl.config import REDSHIFT
    from datetime import date
    import psycopg2

    rows = build_rows(date(2023, 1, 1), date(2028, 12, 31))
    conn = psycopg2.connect(REDSHIFT.dsn)
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM analytics.dim_date")
            cur.executemany(INSERT_SQL, rows)
        conn.commit()
    finally:
        conn.close()


def _run_data_quality_gate(**context):
    from etl.data_quality_checks import run_checks
    run_date = context["ds"]
    failures = run_checks(run_date)
    if failures:
        raise ValueError(f"data quality gate failed: {failures}")


def _refresh_product_margin(**context):
    import psycopg2
    from etl.config import REDSHIFT
    conn = psycopg2.connect(REDSHIFT.dsn)
    try:
        with conn.cursor() as cur:
            cur.execute("CALL analytics.sp_refresh_product_margin();")
        conn.commit()
    finally:
        conn.close()


with DAG(
    dag_id="retail_dw_pipeline",
    description="Daily ELT: S3 raw -> Redshift staging -> dbt marts -> data quality gate",
    default_args=default_args,
    schedule_interval="0 4 * * *",  # 04:00 UTC daily, ahead of business-hours BI refresh
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["retail", "data-warehouse", "elt"],
) as dag:

    extract_to_s3 = PythonOperator(
        task_id="extract_to_s3",
        python_callable=_extract_to_s3,
    )

    load_staging = PythonOperator(
        task_id="load_staging",
        python_callable=_load_staging,
    )

    generate_dim_date = PythonOperator(
        task_id="generate_dim_date",
        python_callable=_generate_dim_date,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "dbt run --project-dir dbt/retail_dw --profiles-dir dbt/retail_dw --target prod"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "dbt test --project-dir dbt/retail_dw --profiles-dir dbt/retail_dw --target prod"
        ),
    )

    data_quality_gate = PythonOperator(
        task_id="data_quality_gate",
        python_callable=_run_data_quality_gate,
    )

    refresh_product_margin = PythonOperator(
        task_id="refresh_product_margin",
        python_callable=_refresh_product_margin,
    )

    notify_success = BashOperator(
        task_id="notify_success",
        bash_command='echo "retail_dw_pipeline succeeded for {{ ds }}"',
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # extract/load must finish before either transform path starts;
    # dim_date can be (re)generated in parallel with dbt_run since dbt
    # only reads it as a source, never writes it.
    extract_to_s3 >> load_staging
    load_staging >> [dbt_run, generate_dim_date]
    dbt_run >> dbt_test >> data_quality_gate >> refresh_product_margin >> notify_success

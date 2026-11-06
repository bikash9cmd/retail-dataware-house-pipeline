# Retail Data Warehouse & ETL Pipeline

> **Tech stack:** Python · ETL · Data warehouse · Analytics & BI

A dimensional data warehouse and ELT pipeline for retail sales analytics —
built to mirror a real production stack: **S3 (raw zone) → AWS Glue/Airflow
(ingest) → Redshift (star schema) → dbt (transform + test) → FastAPI (serve)
→ Power BI (visualize)**, with a Kimball-style dimensional model
(`fact_sales` + `dim_customer` / `dim_product` / `dim_store` / `dim_date`)
underneath it.

Because a review environment can't hold live AWS credentials, the same
transformation logic also ships as a **zero-dependency local demo**
(SQLite + pandas) so anyone cloning the repo can run the whole pipeline
end-to-end in seconds and see it pass its own data-quality gate — see
[Quickstart](#quickstart) below.

## Architecture

```
                                   ┌───────────────────────────────┐
  Source CSVs  ──extract_s3.py──▶ │  S3 raw zone (partitioned by   │
  (OLTP export)                   │  run_date)                     │
                                   └───────────────┬─────────────────┘
                                                    │  Glue / Airflow
                                                    ▼
                                   ┌───────────────────────────────┐
                                   │  Redshift: staging schema      │
                                   │  (load_staging.py, COPY)       │
                                   └───────────────┬─────────────────┘
                                                    │  dbt run (staging → marts)
                                                    ▼
                                   ┌───────────────────────────────┐
                                   │  Redshift: analytics schema    │
                                   │  dim_customer / dim_product /  │
                                   │  dim_store / dim_date          │
                                   │  fact_sales (star schema)      │
                                   └──────┬─────────────┬────────────┘
                                          │             │
                              dbt test +  │             │  Athena (Parquet,
                          data_quality_   │             │  external table)
                          checks.py       ▼             ▼
                                   ┌────────────┐  ┌───────────────┐
                                   │ FastAPI    │  │ Power BI /    │
                                   │ (api/)     │  │ Tableau       │
                                   └────────────┘  └───────────────┘
```

Orchestration (`dags/retail_dw_pipeline_dag.py`) runs the whole chain daily:
`extract_to_s3 → load_staging → [dbt_run, generate_dim_date] → dbt_test →
data_quality_gate → refresh_product_margin → notify_success`.

## Repository layout

| Path | Purpose |
|---|---|
| `scripts/generate_sample_data.py` | Synthetic OLTP export (customers, stores, products, orders) — stdlib only |
| `sql/redshift/` | Star-schema DDL: staging tables, dimensions, fact table, BI views, a stored procedure |
| `etl/` | Production ETL: S3 extract, Redshift `COPY` load, `dim_date` generator, dbt runner, data-quality gate |
| `dbt/retail_dw/` | dbt project: staging models, dimensional marts, schema + custom tests |
| `dags/retail_dw_pipeline_dag.py` | Airflow DAG orchestrating the full pipeline |
| `api/` | FastAPI service exposing warehouse KPIs as REST endpoints |
| `warehouse/local_demo.py` | Runs the same star-schema transform against SQLite — no AWS/Docker needed |
| `tests/` | pytest suite validating the transform logic and data-quality invariants |
| `.github/workflows/ci.yml` | CI: generates data, runs the pipeline, runs tests, compiles all modules |

## Quickstart (local, no AWS account needed)

```bash
pip install pandas pytest

python scripts/generate_sample_data.py --rows 5000
python warehouse/local_demo.py
pytest tests/ -v
```

`local_demo.py` builds the full star schema in `warehouse/local_demo.db`,
creates the same BI views the production Redshift schema exposes, runs the
data-quality gate, and prints top-products and revenue-by-segment as a
sanity check.

To browse the API against that local database:

```bash
pip install fastapi "uvicorn[standard]"
DATABASE_MODE=local uvicorn api.main:app --reload
# http://localhost:8000/docs
```

## Running against Postgres (closer to Redshift)

Redshift is PostgreSQL-derived, so a local Postgres container is a closer
stand-in than SQLite for testing the actual Redshift DDL and dbt models:

```bash
docker compose up -d
psql "postgresql://etl_service:etl_password@localhost:5439/retail_dw" \
  -f sql/redshift/01_staging_schema.sql \
  -f sql/redshift/02_dimension_tables.sql \
  -f sql/redshift/03_fact_tables.sql \
  -f sql/redshift/04_views_and_procedures.sql
```

## Running the real pipeline (AWS)

1. Create the S3 buckets, Redshift cluster/serverless workgroup, and an IAM
   role Redshift can assume for `COPY` — set `REDSHIFT_COPY_ROLE_ARN`.
2. Copy `.env.example` to `.env` and fill in real values (never commit `.env`).
3. Apply the DDL in `sql/redshift/` against the cluster.
4. `python etl/generate_dim_date.py`
5. `python etl/extract_s3.py --run-date 2026-07-14`
6. `python etl/load_staging.py --run-date 2026-07-14`
7. `cp dbt/retail_dw/profiles.yml.example ~/.dbt/profiles.yml` (fill in env vars), then `python etl/transform_load.py --target prod`
8. `python etl/data_quality_checks.py --run-date 2026-07-14`
9. Copy `dags/retail_dw_pipeline_dag.py` into your Airflow `dags/` folder to run steps 4–8 on a daily schedule instead of by hand.

## Why a hash-based surrogate key in dbt but `IDENTITY` in the DDL?

`sql/redshift/02_dimension_tables.sql` documents the target physical model
(used to bootstrap the cluster and by anything querying outside dbt, like
the stored procedure and the Athena external table). dbt's `marts` models
rebuild tables with `CREATE TABLE AS` on every run, so they use a
deterministic `MD5(natural_key)` surrogate key instead — stable across
rebuilds, which is the standard dbt pattern, rather than relying on a
sequence that would reset every run.

## Data quality strategy (three layers, each catching different failure modes)

1. **dbt schema tests** (`dbt/retail_dw/models/**/schema.yml`) — column-level `not_null`, `unique`, `relationships`, `accepted_range`.
2. **dbt custom test** (`dbt/retail_dw/tests/assert_no_future_order_dates.sql`) — a business-rule check that doesn't fit a generic schema test.
3. **`etl/data_quality_checks.py`** — pipeline-level gate run by Airflow after `dbt test`: freshness (did today's partition actually load?) and row-count sanity, which are awkward to express as a single dbt test.

## Mapping to the job requirements

| Requirement | Where |
|---|---|
| ETL/ELT pipeline design | `etl/extract_s3.py`, `etl/load_staging.py`, `dags/retail_dw_pipeline_dag.py` |
| Dimensional modeling, OLTP/OLAP, data warehousing | `sql/redshift/02_dimension_tables.sql`, `03_fact_tables.sql` |
| SQL objects: views, stored procedures | `sql/redshift/04_views_and_procedures.sql` |
| Apache Airflow orchestration | `dags/retail_dw_pipeline_dag.py` |
| DBT | `dbt/retail_dw/` (models, schema tests, custom test, `packages.yml`) |
| Cloud services: S3, Redshift, Athena | `etl/extract_s3.py`, `etl/load_staging.py`, Athena DDL reference in `04_views_and_procedures.sql` |
| REST API development | `api/main.py` |
| Data validation / QA | dbt tests + `etl/data_quality_checks.py` |
| Git-based workflows, CI/CD | `.github/workflows/ci.yml` |
| BI / visualization | `analytics.vw_*` views feed Power BI directly; same shape served by the API |

## License

MIT — see [LICENSE](LICENSE).
# retail-dataware-house-pipeline

"""
config.py

Centralised environment configuration for the ETL layer. All values are
read from the environment (populated via .env in local dev, or Airflow
Connections / Secrets Manager in production) — nothing is hard-coded.
"""
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class S3Config:
    raw_bucket: str = os.getenv("RAW_BUCKET", "retail-dw-raw")
    curated_bucket: str = os.getenv("CURATED_BUCKET", "retail-dw-curated")
    raw_prefix: str = os.getenv("RAW_PREFIX", "orders/")
    region: str = os.getenv("AWS_REGION", "us-east-1")


@dataclass(frozen=True)
class RedshiftConfig:
    host: str = os.getenv("REDSHIFT_HOST", "localhost")
    port: int = int(os.getenv("REDSHIFT_PORT", "5439"))
    database: str = os.getenv("REDSHIFT_DB", "retail_dw")
    user: str = os.getenv("REDSHIFT_USER", "etl_service")
    password: str = os.getenv("REDSHIFT_PASSWORD", "")
    iam_role_arn: str = os.getenv("REDSHIFT_COPY_ROLE_ARN", "")

    @property
    def dsn(self) -> str:
        return (f"host={self.host} port={self.port} dbname={self.database} "
                f"user={self.user} password={self.password}")


S3 = S3Config()
REDSHIFT = RedshiftConfig()

STAGING_TABLES = ["customers", "stores", "products", "orders"]

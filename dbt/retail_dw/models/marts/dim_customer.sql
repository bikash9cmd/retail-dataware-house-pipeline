-- Surrogate key is a deterministic MD5 hash rather than an IDENTITY
-- column: dbt rebuilds this table with CREATE TABLE AS on every run,
-- so a hash-based key (stable across rebuilds) is the standard dbt
-- pattern instead of a native sequence. sql/redshift/02_dimension_tables.sql
-- documents the equivalent hand-authored DDL used to bootstrap the
-- cluster and for any consumers (stored procs, Athena) that read the
-- table outside of dbt.
select
    md5(customer_id)          as customer_key,
    customer_id,
    first_name,
    last_name,
    email,
    city,
    country,
    customer_segment,
    signup_date,
    true as is_current,
    current_date as effective_date
from {{ ref('stg_customers') }}

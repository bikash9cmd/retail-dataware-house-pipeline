-- Custom singular test: fails (returns rows) if any order_date is in the
-- future relative to the run date, which usually indicates a source
-- system clock issue or a bad backfill.
select order_id, date_key
from {{ ref('fact_sales') }}
where date_key > cast(to_char(current_date, 'YYYYMMDD') as integer)

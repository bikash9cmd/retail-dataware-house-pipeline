select
    md5(store_id) as store_key,
    store_id,
    store_name,
    city,
    country,
    store_type
from {{ ref('stg_stores') }}

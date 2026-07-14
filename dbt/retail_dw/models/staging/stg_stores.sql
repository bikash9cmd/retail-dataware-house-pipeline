with source as (
    select * from {{ source('staging', 'stores') }}
)

select
    trim(store_id)          as store_id,
    trim(store_name)        as store_name,
    initcap(trim(city))     as city,
    initcap(trim(country))  as country,
    coalesce(store_type, 'Unknown') as store_type
from source
where store_id is not null

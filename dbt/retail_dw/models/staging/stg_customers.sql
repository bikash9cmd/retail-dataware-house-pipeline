-- Light cleaning only: trim, dedupe, standardise casing. Business logic
-- and surrogate keys belong in the marts layer, not here.
with source as (
    select * from {{ source('staging', 'customers') }}
),

cleaned as (
    select
        trim(customer_id)                  as customer_id,
        initcap(trim(first_name))          as first_name,
        initcap(trim(last_name))           as last_name,
        lower(trim(email))                 as email,
        initcap(trim(city))                as city,
        initcap(trim(country))             as country,
        signup_date,
        coalesce(customer_segment, 'Unknown') as customer_segment
    from source
    where customer_id is not null
),

deduped as (
    select *,
           row_number() over (partition by customer_id order by signup_date desc) as rn
    from cleaned
)

select
    customer_id, first_name, last_name, email, city, country, signup_date, customer_segment
from deduped
where rn = 1

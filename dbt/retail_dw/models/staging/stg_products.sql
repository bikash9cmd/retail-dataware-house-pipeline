with source as (
    select * from {{ source('staging', 'products') }}
),

cleaned as (
    select
        trim(product_id)            as product_id,
        trim(product_name)          as product_name,
        initcap(trim(category))     as category,
        round(unit_price, 2)        as unit_price,
        round(unit_cost, 2)         as unit_cost
    from source
    where product_id is not null
      and unit_price >= 0
)

select * from cleaned

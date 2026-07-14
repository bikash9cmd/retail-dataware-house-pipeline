with source as (
    select * from {{ source('staging', 'orders') }}
),

cleaned as (
    select
        trim(order_id)          as order_id,
        order_date,
        trim(customer_id)       as customer_id,
        trim(store_id)          as store_id,
        trim(product_id)        as product_id,
        quantity,
        round(unit_price, 2)    as unit_price,
        discount_pct,
        round(gross_amount, 2)     as gross_amount,
        round(discount_amount, 2) as discount_amount,
        round(net_amount, 2)      as net_amount,
        lower(trim(channel))        as channel,
        lower(trim(payment_method)) as payment_method
    from source
    where order_id is not null
      and quantity > 0
      and net_amount >= 0
)

select * from cleaned

-- Grain: one row per order line. date_key is derived directly from
-- order_date (YYYYMMDD int) rather than joined, so a late-arriving fact
-- never gets dropped for lack of a matching dim_date row; analytics.dim_date
-- (populated by etl/generate_dim_date.py, see the `analytics` source below)
-- is still the single source of truth for calendar attributes used in BI.
with orders as (
    select * from {{ ref('stg_orders') }}
)

select
    orders.order_id,
    cast(to_char(orders.order_date, 'YYYYMMDD') as integer) as date_key,
    cust.customer_key,
    prod.product_key,
    store.store_key,
    orders.quantity,
    orders.unit_price,
    orders.discount_pct,
    orders.gross_amount,
    orders.discount_amount,
    orders.net_amount,
    orders.channel,
    orders.payment_method
from orders
left join {{ ref('dim_customer') }} cust  on orders.customer_id = cust.customer_id
left join {{ ref('dim_product') }} prod   on orders.product_id  = prod.product_id
left join {{ ref('dim_store') }} store    on orders.store_id    = store.store_id

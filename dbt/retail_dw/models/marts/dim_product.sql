select
    md5(product_id) as product_key,
    product_id,
    product_name,
    category,
    unit_price,
    unit_cost,
    case when unit_price > 0
         then round(((unit_price - unit_cost) / unit_price) * 100, 2)
         else 0
    end as margin_pct
from {{ ref('stg_products') }}

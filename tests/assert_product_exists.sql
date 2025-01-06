select 
    o.order_id,
    o.product_id
from {{ ref('stg_orders') }} o
left join {{ ref('stg_products') }} p using (product_id)
where p.product_id is null 
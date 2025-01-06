select
    order_id,
    status
from {{ ref('stg_orders') }}
where status not in ('COMPLETED', 'CANCELLED', 'PENDING', 'SHIPPED', 'REFUNDED', 'UNKNOWN') 
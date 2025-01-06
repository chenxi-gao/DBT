-- Test if the order amount is positive
select
    order_id,
    order_amount,
    status
from {{ ref('stg_orders') }}
where order_amount <= 0
  and status != 'REFUNDED'  -- exclude refunded orders
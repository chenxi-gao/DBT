with customer_orders as (
    select
        customer_id,
        count(*) as total_orders,
        sum(order_amount) as total_amount,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date
    from {{ ref('stg_orders') }}
    group by customer_id
)

select
    customer_id,
    total_orders,
    total_amount,
    first_order_date,
    last_order_date,
    total_amount / total_orders as avg_order_amount
from customer_orders 
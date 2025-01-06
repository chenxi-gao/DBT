with source as (
    select * from {{ ref('raw_orders') }}
)

select
    order_id,
    customer_id,
    cast(order_date as date) as order_date,
    cast(order_amount as decimal(10,2)) as order_amount
from source 
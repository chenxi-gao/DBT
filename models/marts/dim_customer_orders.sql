with customer_orders as (
    select
        customer_id,
        count(*) as total_orders,
        count(distinct product_id) as unique_products_bought,
        sum(quantity) as total_items,
        sum(order_amount) as total_amount,
        sum(case when has_discount then 1 else 0 end) as orders_with_discount,
        sum(case when status = 'REFUNDED' then 1 else 0 end) as refunded_orders,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,
        array_agg(distinct payment_method) as payment_methods_used
    from {{ ref('stg_orders') }}
    group by customer_id
),

order_stats as (
    select
        customer_id,
        avg(quantity) as avg_items_per_order,
        stddev(order_amount) as order_amount_stddev,
        mode() within group (order by product_id) as most_bought_product
    from {{ ref('stg_orders') }}
    group by customer_id
)

select
    co.*,
    os.avg_items_per_order,
    os.order_amount_stddev,
    os.most_bought_product,
    total_amount / nullif(total_orders, 0) as avg_order_amount,
    orders_with_discount::float / nullif(total_orders, 0) as discount_usage_rate,
    case 
        when total_amount >= 1000 then 'VIP'
        when total_amount >= 500 then 'Premium'
        else 'Regular'
    end as customer_tier
from customer_orders co
join order_stats os using (customer_id) 
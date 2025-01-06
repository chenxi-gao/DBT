with source as (
    select * from {{ ref('raw_orders') }}
),

cleaned as (
    select
        order_id,
        customer_id,
        product_id,
        cast(order_date as date) as order_date,
        cast(order_amount as decimal(10,2)) as order_amount,
        cast(quantity as integer) as quantity,
        case 
            when status = 'completed' then 'COMPLETED'
            when status = 'cancelled' then 'CANCELLED'
            when status = 'pending' then 'PENDING'
            when status = 'shipped' then 'SHIPPED'
            when status = 'refunded' then 'REFUNDED'
            else 'UNKNOWN'
        end as status,
        case 
            when payment_method in ('credit_card', 'paypal', 'alipay', 'wechat', 'bank_transfer')
            then payment_method
            else 'other'
        end as payment_method,
        shipping_address,
        coalesce(discount_code, 'NO_DISCOUNT') as discount_code,
        -- Add new column to indicate if the order has a discount
        case when discount_code is not null then true else false end as has_discount,
        extract(month from cast(order_date as date)) as order_month,
        extract(year from cast(order_date as date)) as order_year
    from source
)

select 
    *,
    -- Add more derived fields
    case 
        when quantity >= 10 then 'bulk_order'
        when quantity >= 5 then 'medium_order'
        else 'small_order'
    end as order_size,
    case 
        when status = 'REFUNDED' then -1 * order_amount
        else order_amount
    end as adjusted_amount
from cleaned 
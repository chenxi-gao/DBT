with source as (
    select * from {{ ref('raw_products') }}
)

select
    product_id,
    product_name,
    category,
    cast(unit_price as decimal(10,2)) as unit_price,
    cast(weight_kg as decimal(5,2)) as weight_kg,
    supplier_id,
    -- Add derived fields
    case 
        when unit_price >= 100 then 'high'
        when unit_price >= 50 then 'medium'
        else 'low'
    end as price_tier
from source 
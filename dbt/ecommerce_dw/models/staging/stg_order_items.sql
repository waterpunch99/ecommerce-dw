with source as (
    {{ dedupe_latest(source('ecommerce_raw', 'order_items'), 'order_item_id') }}
)

select
    cast(order_item_id as int64) as order_item_id,
    cast(order_id as int64) as order_id,
    cast(product_id as int64) as product_id,
    cast(quantity as int64) as quantity,
    cast(unit_price as numeric) as unit_price,
    cast(discount_amount as numeric) as discount_amount,
    cast(item_amount as numeric) as item_amount,
    cast(created_at as timestamp) as created_at,
    cast(updated_at as timestamp) as updated_at,
    cast(is_deleted as bool) as is_deleted,
    cast(extract_date as date) as extract_date,
    cast(extracted_at as timestamp) as extracted_at,
    cast(batch_id as string) as batch_id,
    cast(loaded_at as timestamp) as loaded_at
from source


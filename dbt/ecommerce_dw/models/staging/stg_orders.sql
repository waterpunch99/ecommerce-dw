with source as (
    {{ dedupe_latest(source('ecommerce_raw', 'orders'), 'order_id') }}
)

select
    cast(order_id as int64) as order_id,
    cast(customer_id as int64) as customer_id,
    cast(coupon_id as int64) as coupon_id,
    cast(order_status as string) as order_status,
    cast(order_channel as string) as order_channel,
    cast(order_at as timestamp) as order_at,
    date(order_at) as order_date,
    cast(subtotal_amount as numeric) as subtotal_amount,
    cast(discount_amount as numeric) as discount_amount,
    cast(shipping_fee as numeric) as shipping_fee,
    cast(total_amount as numeric) as total_amount,
    cast(created_at as timestamp) as created_at,
    cast(updated_at as timestamp) as updated_at,
    coalesce(cast(is_deleted as bool), false) as is_deleted,
    cast(extract_date as date) as extract_date,
    cast(extracted_at as timestamp) as extracted_at,
    cast(batch_id as string) as batch_id,
    cast(loaded_at as timestamp) as loaded_at
from source

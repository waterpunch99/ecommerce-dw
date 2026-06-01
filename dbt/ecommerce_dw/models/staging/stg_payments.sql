with source as (
    {{ dedupe_latest(source('ecommerce_raw', 'payments'), 'payment_id') }}
)

select
    cast(payment_id as int64) as payment_id,
    cast(order_id as int64) as order_id,
    cast(payment_method as string) as payment_method,
    cast(payment_status as string) as payment_status,
    cast(payment_amount as numeric) as payment_amount,
    cast(payment_at as timestamp) as payment_at,
    date(payment_at) as payment_date,
    cast(transaction_id as string) as transaction_id,
    cast(created_at as timestamp) as created_at,
    cast(updated_at as timestamp) as updated_at,
    cast(is_deleted as bool) as is_deleted,
    cast(extract_date as date) as extract_date,
    cast(extracted_at as timestamp) as extracted_at,
    cast(batch_id as string) as batch_id,
    cast(loaded_at as timestamp) as loaded_at
from source


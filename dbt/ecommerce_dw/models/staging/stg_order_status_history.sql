with source as (
    {{ dedupe_latest(source('ecommerce_raw', 'order_status_history'), 'order_status_history_id') }}
)

select
    cast(order_status_history_id as int64) as order_status_history_id,
    cast(order_id as int64) as order_id,
    cast(previous_status as string) as previous_status,
    cast(new_status as string) as new_status,
    cast(changed_at as timestamp) as changed_at,
    cast(change_reason as string) as change_reason,
    cast(created_at as timestamp) as created_at,
    cast(updated_at as timestamp) as updated_at,
    cast(is_deleted as bool) as is_deleted,
    cast(extract_date as date) as extract_date,
    cast(extracted_at as timestamp) as extracted_at,
    cast(batch_id as string) as batch_id,
    cast(loaded_at as timestamp) as loaded_at
from source


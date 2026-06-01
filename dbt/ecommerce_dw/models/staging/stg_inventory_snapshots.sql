with source as (
    {{ dedupe_latest(source('ecommerce_raw', 'inventory_snapshots'), 'snapshot_date, product_id') }}
)

select
    cast(snapshot_date as date) as snapshot_date,
    cast(product_id as int64) as product_id,
    cast(category_id as int64) as category_id,
    cast(stock_quantity as int64) as stock_quantity,
    cast(reserved_quantity as int64) as reserved_quantity,
    cast(available_quantity as int64) as available_quantity,
    cast(created_at as timestamp) as created_at,
    cast(updated_at as timestamp) as updated_at,
    cast(is_deleted as bool) as is_deleted,
    cast(extract_date as date) as extract_date,
    cast(extracted_at as timestamp) as extracted_at,
    cast(batch_id as string) as batch_id,
    cast(loaded_at as timestamp) as loaded_at
from source


with source as (
    {{ dedupe_latest(source('ecommerce_raw', 'products'), 'product_id') }}
)

select
    cast(product_id as int64) as product_id,
    cast(category_id as int64) as category_id,
    cast(product_name as string) as product_name,
    cast(brand as string) as brand,
    cast(list_price as numeric) as list_price,
    cast(product_status as string) as product_status,
    cast(created_at as timestamp) as created_at,
    cast(updated_at as timestamp) as updated_at,
    cast(is_deleted as bool) as is_deleted,
    cast(extract_date as date) as extract_date,
    cast(extracted_at as timestamp) as extracted_at,
    cast(batch_id as string) as batch_id,
    cast(loaded_at as timestamp) as loaded_at
from source


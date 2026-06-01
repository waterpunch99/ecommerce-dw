with source as (
    {{ dedupe_latest(source('ecommerce_raw', 'categories'), 'category_id') }}
)

select
    cast(category_id as int64) as category_id,
    cast(parent_category_id as int64) as parent_category_id,
    cast(category_name as string) as category_name,
    cast(category_level as int64) as category_level,
    cast(is_active as bool) as is_active,
    cast(created_at as timestamp) as created_at,
    cast(updated_at as timestamp) as updated_at,
    cast(is_deleted as bool) as is_deleted,
    cast(extract_date as date) as extract_date,
    cast(extracted_at as timestamp) as extracted_at,
    cast(batch_id as string) as batch_id,
    cast(loaded_at as timestamp) as loaded_at
from source


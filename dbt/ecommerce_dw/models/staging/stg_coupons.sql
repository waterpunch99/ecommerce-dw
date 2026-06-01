with source as (
    {{ dedupe_latest(source('ecommerce_raw', 'coupons'), 'coupon_id') }}
)

select
    cast(coupon_id as int64) as coupon_id,
    cast(coupon_code as string) as coupon_code,
    cast(coupon_name as string) as coupon_name,
    cast(discount_type as string) as discount_type,
    cast(discount_value as numeric) as discount_value,
    cast(valid_from as timestamp) as valid_from,
    cast(valid_to as timestamp) as valid_to,
    cast(created_at as timestamp) as created_at,
    cast(updated_at as timestamp) as updated_at,
    cast(is_deleted as bool) as is_deleted,
    cast(extract_date as date) as extract_date,
    cast(extracted_at as timestamp) as extracted_at,
    cast(batch_id as string) as batch_id,
    cast(loaded_at as timestamp) as loaded_at
from source


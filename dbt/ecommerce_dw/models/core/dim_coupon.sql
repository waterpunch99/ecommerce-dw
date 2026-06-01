{{ config(
    materialized='incremental',
    unique_key='coupon_id',
    incremental_strategy='merge'
) }}

select
    coupon_id,
    coupon_code,
    coupon_name,
    discount_type,
    discount_value,
    valid_from,
    valid_to,
    created_at,
    updated_at,
    is_deleted,
    extract_date,
    loaded_at
from {{ ref('stg_coupons') }}

{% if is_incremental() %}
where updated_at >= (
    select coalesce(max(updated_at), timestamp '1970-01-01')
    from {{ this }}
)
{% endif %}


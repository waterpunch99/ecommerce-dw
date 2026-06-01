{{ config(
    materialized='incremental',
    unique_key='category_id',
    incremental_strategy='merge'
) }}

select
    category_id,
    parent_category_id,
    category_name,
    category_level,
    is_active,
    created_at,
    updated_at,
    is_deleted,
    extract_date,
    loaded_at
from {{ ref('stg_categories') }}

{% if is_incremental() %}
where updated_at >= (
    select coalesce(max(updated_at), timestamp '1970-01-01')
    from {{ this }}
)
{% endif %}


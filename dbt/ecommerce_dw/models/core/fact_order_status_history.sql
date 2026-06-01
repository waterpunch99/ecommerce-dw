{{ config(
    materialized='incremental',
    unique_key='order_status_history_id',
    incremental_strategy='merge'
) }}

select
    order_status_history_id,
    order_id,
    previous_status,
    new_status,
    changed_at,
    date(changed_at) as changed_date,
    change_reason,
    created_at,
    updated_at,
    is_deleted,
    extract_date,
    loaded_at
from {{ ref('stg_order_status_history') }}

{% if is_incremental() %}
where updated_at >= (
    select coalesce(max(updated_at), timestamp '1970-01-01')
    from {{ this }}
)
{% endif %}


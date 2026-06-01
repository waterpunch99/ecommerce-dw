{{ config(
    materialized='incremental',
    unique_key='shipment_id',
    incremental_strategy='merge'
) }}

select
    shipment_id,
    order_id,
    shipment_status,
    carrier,
    tracking_number,
    shipped_at,
    delivered_at,
    date(shipped_at) as shipped_date,
    date(delivered_at) as delivered_date,
    created_at,
    updated_at,
    is_deleted,
    extract_date,
    loaded_at
from {{ ref('stg_shipments') }}

{% if is_incremental() %}
where updated_at >= (
    select coalesce(max(updated_at), timestamp '1970-01-01')
    from {{ this }}
)
{% endif %}


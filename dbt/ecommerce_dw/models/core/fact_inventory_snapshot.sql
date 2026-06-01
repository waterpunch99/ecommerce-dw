{{ config(
    materialized='incremental',
    unique_key=['snapshot_date', 'product_id'],
    incremental_strategy='merge',
    partition_by={'field': 'snapshot_date', 'data_type': 'date'},
    cluster_by=['product_id', 'category_id']
) }}

select
    snapshot_date,
    product_id,
    category_id,
    stock_quantity,
    reserved_quantity,
    available_quantity,
    created_at,
    updated_at,
    is_deleted,
    extract_date,
    loaded_at
from {{ ref('stg_inventory_snapshots') }}

{% if is_incremental() %}
where updated_at >= (
    select coalesce(max(updated_at), timestamp '1970-01-01')
    from {{ this }}
)
{% endif %}


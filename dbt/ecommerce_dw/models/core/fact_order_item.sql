{{ config(
    materialized='incremental',
    unique_key='order_item_id',
    incremental_strategy='merge',
    partition_by={'field': 'order_date', 'data_type': 'date'},
    cluster_by=['product_id', 'category_id']
) }}

select
    oi.order_item_id,
    oi.order_id,
    fo.order_date,
    oi.product_id,
    dp.product_sk,
    dp.category_id,
    oi.quantity,
    oi.unit_price,
    oi.discount_amount,
    oi.item_amount,
    oi.created_at,
    oi.updated_at,
    oi.is_deleted,
    oi.extract_date,
    oi.loaded_at
from {{ ref('stg_order_items') }} as oi
left join {{ ref('fact_order') }} as fo
    on oi.order_id = fo.order_id
left join {{ ref('dim_product') }} as dp
    on oi.product_id = dp.product_id
    and fo.order_at >= dp.effective_from
    and fo.order_at < dp.effective_to

{% if is_incremental() %}
where oi.updated_at >= (
    select coalesce(max(updated_at), timestamp '1970-01-01')
    from {{ this }}
)
{% endif %}


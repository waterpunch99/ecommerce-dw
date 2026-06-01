{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge',
    partition_by={'field': 'order_date', 'data_type': 'date'},
    cluster_by=['customer_id', 'order_status']
) }}

select
    o.order_id,
    o.customer_id,
    dc.customer_sk,
    o.coupon_id,
    o.order_status,
    o.order_channel,
    o.order_at,
    o.order_date,
    o.subtotal_amount,
    o.discount_amount,
    o.shipping_fee,
    o.total_amount,
    o.created_at,
    o.updated_at,
    o.is_deleted,
    o.extract_date,
    o.loaded_at
from {{ ref('stg_orders') }} as o
left join {{ ref('dim_customer') }} as dc
    on o.customer_id = dc.customer_id
    and o.order_at >= dc.effective_from
    and o.order_at < dc.effective_to

{% if is_incremental() %}
where o.updated_at >= (
    select coalesce(max(updated_at), timestamp '1970-01-01')
    from {{ this }}
)
{% endif %}


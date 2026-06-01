{{ config(
    materialized='incremental',
    unique_key='payment_id',
    incremental_strategy='merge',
    partition_by={'field': 'payment_date', 'data_type': 'date'},
    cluster_by=['payment_status', 'payment_method']
) }}

select
    payment_id,
    order_id,
    payment_method,
    payment_status,
    payment_amount,
    payment_at,
    payment_date,
    transaction_id,
    created_at,
    updated_at,
    is_deleted,
    extract_date,
    loaded_at
from {{ ref('stg_payments') }}

{% if is_incremental() %}
where updated_at >= (
    select coalesce(max(updated_at), timestamp '1970-01-01')
    from {{ this }}
)
{% endif %}


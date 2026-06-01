{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'sales_date', 'data_type': 'date'},
    cluster_by=['category_id']
) }}

{% set mart_start_date = var('mart_start_date', none) %}
{% set mart_end_date = var('mart_end_date', none) %}

with item_base as (
    select
        oi.order_date as sales_date,
        coalesce(oi.category_id, -1) as category_id,
        coalesce(dc.category_name, 'unknown') as category_name,
        oi.order_id,
        oi.order_item_id,
        oi.product_id,
        oi.quantity,
        oi.item_amount,
        oi.discount_amount as item_discount_amount
    from {{ ref('fact_order_item') }} as oi
    left join {{ ref('dim_category') }} as dc
        on oi.category_id = dc.category_id
    where coalesce(oi.is_deleted, false) = false

    {% if mart_start_date and mart_end_date %}
      and oi.order_date between date('{{ mart_start_date }}') and date('{{ mart_end_date }}')
    {% elif is_incremental() %}
      and oi.order_date >= date_sub(current_date(), interval 7 day)
    {% endif %}
),

category_items as (
    select
        sales_date,
        category_id,
        category_name,
        order_id,
        count(distinct order_item_id) as order_item_count,
        count(distinct product_id) as product_count,
        sum(quantity) as quantity_sold,
        sum(item_amount) as category_item_amount,
        sum(item_discount_amount) as item_discount_amount
    from item_base
    group by 1, 2, 3, 4
),

order_item_totals as (
    select
        order_id,
        sum(item_amount) as order_item_amount
    from item_base
    group by 1
),

orders as (
    select
        order_id,
        order_status,
        total_amount,
        discount_amount as order_discount_amount,
        shipping_fee
    from {{ ref('fact_order') }}
    where coalesce(is_deleted, false) = false
),

payments as (
    select
        order_id,
        sum(case when payment_status = 'paid' then payment_amount else 0 end) as paid_amount,
        sum(case when payment_status = 'refunded' then payment_amount else 0 end) as refunded_amount
    from {{ ref('fact_payment') }}
    where coalesce(is_deleted, false) = false
    group by 1
)

select
    ci.sales_date,
    ci.category_id,
    ci.category_name,
    count(distinct ci.order_id) as order_count,
    sum(ci.product_count) as product_count,
    sum(ci.quantity_sold) as quantity_sold,
    sum(ci.category_item_amount) as gross_item_revenue,
    sum(ci.item_discount_amount) as item_discount_amount,
    sum(coalesce(p.paid_amount, 0) * safe_divide(ci.category_item_amount, oit.order_item_amount)) as allocated_paid_amount,
    sum(coalesce(p.refunded_amount, 0) * safe_divide(ci.category_item_amount, oit.order_item_amount)) as allocated_refunded_amount,
    sum(case when o.order_status in ('paid', 'shipped', 'delivered') then ci.category_item_amount else 0 end) as daily_revenue,
    current_timestamp() as mart_loaded_at
from category_items as ci
left join orders as o
    on ci.order_id = o.order_id
left join order_item_totals as oit
    on ci.order_id = oit.order_id
left join payments as p
    on ci.order_id = p.order_id
group by 1, 2, 3

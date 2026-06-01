{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'sales_date', 'data_type': 'date'},
    cluster_by=['product_id', 'category_id']
) }}

{% set mart_start_date = var('mart_start_date', none) %}
{% set mart_end_date = var('mart_end_date', none) %}

with order_items as (
    select
        oi.order_date as sales_date,
        oi.order_id,
        oi.order_item_id,
        oi.product_id,
        oi.product_sk,
        coalesce(oi.category_id, -1) as category_id,
        oi.quantity,
        oi.unit_price,
        oi.discount_amount,
        oi.item_amount
    from {{ ref('fact_order_item') }} as oi
    where coalesce(oi.is_deleted, false) = false

    {% if mart_start_date and mart_end_date %}
      and oi.order_date between date('{{ mart_start_date }}') and date('{{ mart_end_date }}')
    {% elif is_incremental() %}
      and oi.order_date >= date_sub(current_date(), interval 7 day)
    {% endif %}
),

orders as (
    select
        order_id,
        order_status
    from {{ ref('fact_order') }}
    where coalesce(is_deleted, false) = false
),

products as (
    select
        product_sk,
        product_id,
        product_name,
        brand,
        product_status
    from {{ ref('dim_product') }}
),

categories as (
    select
        category_id,
        category_name
    from {{ ref('dim_category') }}
)

select
    oi.sales_date,
    oi.product_id,
    oi.product_sk,
    p.product_name,
    p.brand,
    p.product_status,
    oi.category_id,
    coalesce(c.category_name, 'unknown') as category_name,
    count(distinct oi.order_id) as order_count,
    count(distinct oi.order_item_id) as order_item_count,
    sum(oi.quantity) as quantity_sold,
    sum(oi.item_amount) as gross_revenue,
    sum(oi.discount_amount) as discount_amount,
    sum(case when o.order_status in ('paid', 'shipped', 'delivered') then oi.item_amount else 0 end) as net_revenue,
    current_timestamp() as mart_loaded_at
from order_items as oi
left join orders as o
    on oi.order_id = o.order_id
left join products as p
    on oi.product_sk = p.product_sk
left join categories as c
    on oi.category_id = c.category_id
group by 1, 2, 3, 4, 5, 6, 7, 8

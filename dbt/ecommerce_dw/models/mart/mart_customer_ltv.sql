{{ config(
    materialized='incremental',
    unique_key='customer_id',
    incremental_strategy='merge',
    cluster_by=['customer_id', 'customer_segment']
) }}

{% set mart_start_date = var('mart_start_date', none) %}
{% set mart_end_date = var('mart_end_date', none) %}

with valid_orders as (
    select
        order_id,
        customer_id,
        customer_sk,
        order_at,
        order_date,
        order_status,
        total_amount
    from {{ ref('fact_order') }}
    where coalesce(is_deleted, false) = false
      and order_status in ('paid', 'shipped', 'delivered')
),

payments as (
    select
        order_id,
        sum(case when payment_status = 'paid' then payment_amount else 0 end) as paid_amount,
        sum(case when payment_status = 'refunded' then payment_amount else 0 end) as refunded_amount
    from {{ ref('fact_payment') }}
    where coalesce(is_deleted, false) = false
    group by 1
),

customer_orders as (
    select
        o.customer_id,
        any_value(o.customer_sk) as latest_customer_sk,
        min(o.order_date) as first_order_date,
        max(o.order_date) as last_order_date,
        count(distinct o.order_id) as lifetime_order_count,
        sum(coalesce(p.paid_amount, 0)) as lifetime_paid_amount,
        sum(coalesce(p.refunded_amount, 0)) as lifetime_refunded_amount,
        sum(o.total_amount) as lifetime_order_amount
    from valid_orders as o
    left join payments as p
        on o.order_id = p.order_id
    group by 1
),

current_customers as (
    select
        customer_id,
        customer_sk,
        customer_name,
        email,
        customer_grade,
        marketing_opt_in
    from {{ ref('dim_customer') }}
    where is_current
)

select
    co.customer_id,
    cc.customer_sk,
    cc.customer_name,
    cc.email,
    cc.customer_grade,
    cc.marketing_opt_in,
    co.first_order_date,
    co.last_order_date,
    date_diff(current_date(), co.last_order_date, day) as days_since_last_order,
    co.lifetime_order_count,
    co.lifetime_paid_amount,
    co.lifetime_refunded_amount,
    co.lifetime_paid_amount - co.lifetime_refunded_amount as customer_ltv,
    safe_divide(co.lifetime_paid_amount - co.lifetime_refunded_amount, co.lifetime_order_count) as avg_order_value,
    case
        when co.lifetime_paid_amount - co.lifetime_refunded_amount >= 1000000 then 'high_value'
        when co.lifetime_order_count >= 3 then 'repeat'
        else 'new_or_low'
    end as customer_segment,
    current_timestamp() as mart_loaded_at
from customer_orders as co
left join current_customers as cc
    on co.customer_id = cc.customer_id

{% if mart_start_date and mart_end_date %}
where co.last_order_date between date('{{ mart_start_date }}') and date('{{ mart_end_date }}')
{% elif is_incremental() %}
where co.last_order_date >= date_sub(current_date(), interval 30 day)
{% endif %}

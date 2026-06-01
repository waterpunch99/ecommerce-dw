{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'cohort_month', 'data_type': 'date'},
    cluster_by=['customer_segment']
) }}

{% set mart_start_date = var('mart_start_date', none) %}
{% set mart_end_date = var('mart_end_date', none) %}

with valid_orders as (
    select
        customer_id,
        order_id,
        order_date
    from {{ ref('fact_order') }}
    where not is_deleted
      and order_status in ('paid', 'shipped', 'delivered')
),

customer_months as (
    select
        customer_id,
        date_trunc(order_date, month) as order_month,
        count(distinct order_id) as monthly_order_count
    from valid_orders
    group by 1, 2
),

cohorts as (
    select
        customer_id,
        min(order_month) as cohort_month
    from customer_months
    group by 1
),

ltv_segments as (
    select
        customer_id,
        customer_segment
    from {{ ref('mart_customer_ltv') }}
),

retention_base as (
    select
        c.cohort_month,
        cm.order_month,
        date_diff(cm.order_month, c.cohort_month, month) as months_since_cohort,
        cm.customer_id,
        coalesce(ls.customer_segment, 'unknown') as customer_segment
    from customer_months as cm
    inner join cohorts as c
        on cm.customer_id = c.customer_id
    left join ltv_segments as ls
        on cm.customer_id = ls.customer_id
    where cm.order_month >= c.cohort_month

    {% if mart_start_date and mart_end_date %}
      and c.cohort_month between date_trunc(date('{{ mart_start_date }}'), month) and date_trunc(date('{{ mart_end_date }}'), month)
    {% elif is_incremental() %}
      and c.cohort_month >= date_sub(date_trunc(current_date(), month), interval 12 month)
    {% endif %}
),

cohort_sizes as (
    select
        cohort_month,
        customer_segment,
        count(distinct customer_id) as cohort_customer_count
    from retention_base
    where months_since_cohort = 0
    group by 1, 2
)

select
    rb.cohort_month,
    rb.order_month,
    rb.months_since_cohort,
    rb.customer_segment,
    cs.cohort_customer_count,
    count(distinct rb.customer_id) as retained_customer_count,
    safe_divide(count(distinct rb.customer_id), cs.cohort_customer_count) as retention_rate,
    current_timestamp() as mart_loaded_at
from retention_base as rb
left join cohort_sizes as cs
    on rb.cohort_month = cs.cohort_month
    and rb.customer_segment = cs.customer_segment
group by 1, 2, 3, 4, 5

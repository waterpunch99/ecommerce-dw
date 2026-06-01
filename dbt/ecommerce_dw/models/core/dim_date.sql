{{ config(materialized='table') }}

with date_spine as (
    select date_day
    from unnest(
        generate_date_array(date '2020-01-01', date '2030-12-31', interval 1 day)
    ) as date_day
)

select
    date_day,
    extract(year from date_day) as year,
    extract(quarter from date_day) as quarter,
    extract(month from date_day) as month,
    extract(day from date_day) as day_of_month,
    extract(dayofweek from date_day) as day_of_week,
    extract(isoweek from date_day) as iso_week,
    format_date('%A', date_day) as day_name,
    format_date('%Y-%m', date_day) as year_month,
    date_trunc(date_day, month) as month_start_date,
    date_trunc(date_day, quarter) as quarter_start_date,
    date_trunc(date_day, year) as year_start_date,
    extract(dayofweek from date_day) in (1, 7) as is_weekend
from date_spine


-- name: raw_orders_freshness
-- type: freshness
-- target_dataset: ecommerce_raw
-- target_table: orders
-- severity: ERROR
select count(*) as failed_row_count
from (
    select max(loaded_at) as max_loaded_at
    from `{project_id}.ecommerce_raw.orders`
)
where max_loaded_at is null
   or max_loaded_at < timestamp_sub(current_timestamp(), interval 1 day);

-- name: fact_order_pk_not_null
-- type: not_null
-- target_dataset: ecommerce_core
-- target_table: fact_order
-- severity: ERROR
select count(*) as failed_row_count
from `{project_id}.ecommerce_core.fact_order`
where order_id is null;

-- name: fact_order_pk_unique
-- type: unique
-- target_dataset: ecommerce_core
-- target_table: fact_order
-- severity: ERROR
select count(*) as failed_row_count
from (
    select order_id
    from `{project_id}.ecommerce_core.fact_order`
    group by order_id
    having count(*) > 1
);

-- name: fact_order_item_pk_unique
-- type: unique
-- target_dataset: ecommerce_core
-- target_table: fact_order_item
-- severity: ERROR
select count(*) as failed_row_count
from (
    select order_item_id
    from `{project_id}.ecommerce_core.fact_order_item`
    group by order_item_id
    having count(*) > 1
);

-- name: fact_order_item_order_fk
-- type: relationship
-- target_dataset: ecommerce_core
-- target_table: fact_order_item
-- severity: ERROR
select count(*) as failed_row_count
from `{project_id}.ecommerce_core.fact_order_item` as oi
left join `{project_id}.ecommerce_core.fact_order` as o
    on oi.order_id = o.order_id
where oi.order_id is not null
  and o.order_id is null;

-- name: fact_payment_order_fk
-- type: relationship
-- target_dataset: ecommerce_core
-- target_table: fact_payment
-- severity: ERROR
select count(*) as failed_row_count
from `{project_id}.ecommerce_core.fact_payment` as p
left join `{project_id}.ecommerce_core.fact_order` as o
    on p.order_id = o.order_id
where p.order_id is not null
  and o.order_id is null;

-- name: fact_order_status_accepted_values
-- type: accepted_values
-- target_dataset: ecommerce_core
-- target_table: fact_order
-- severity: ERROR
select count(*) as failed_row_count
from `{project_id}.ecommerce_core.fact_order`
where order_status not in ('created', 'paid', 'shipped', 'delivered', 'cancelled', 'refunded');

-- name: fact_payment_status_accepted_values
-- type: accepted_values
-- target_dataset: ecommerce_core
-- target_table: fact_payment
-- severity: ERROR
select count(*) as failed_row_count
from `{project_id}.ecommerce_core.fact_payment`
where payment_status not in ('authorized', 'paid', 'failed', 'cancelled', 'refunded');

-- name: payment_amount_non_negative
-- type: range
-- target_dataset: ecommerce_core
-- target_table: fact_payment
-- severity: ERROR
select count(*) as failed_row_count
from `{project_id}.ecommerce_core.fact_payment`
where payment_amount < 0;

-- name: order_total_amount_non_negative
-- type: range
-- target_dataset: ecommerce_core
-- target_table: fact_order
-- severity: ERROR
select count(*) as failed_row_count
from `{project_id}.ecommerce_core.fact_order`
where total_amount < 0;

-- name: dim_customer_one_current_row
-- type: scd_type_2
-- target_dataset: ecommerce_core
-- target_table: dim_customer
-- severity: ERROR
select count(*) as failed_row_count
from (
    select customer_id
    from `{project_id}.ecommerce_core.dim_customer`
    where is_current
    group by customer_id
    having count(*) > 1
);

-- name: dim_product_one_current_row
-- type: scd_type_2
-- target_dataset: ecommerce_core
-- target_table: dim_product
-- severity: ERROR
select count(*) as failed_row_count
from (
    select product_id
    from `{project_id}.ecommerce_core.dim_product`
    where is_current
    group by product_id
    having count(*) > 1
);

-- name: raw_to_core_orders_row_count
-- type: row_count
-- target_dataset: ecommerce_core
-- target_table: fact_order
-- severity: WARN
select if(raw_count < core_count, 1, 0) as failed_row_count
from (
    select
        (select count(distinct order_id) from `{project_id}.ecommerce_raw.orders`) as raw_count,
        (select count(distinct order_id) from `{project_id}.ecommerce_core.fact_order`) as core_count
);

-- name: mart_daily_sales_revenue_non_negative
-- type: metric_sanity
-- target_dataset: ecommerce_mart
-- target_table: mart_daily_sales
-- severity: ERROR
select count(*) as failed_row_count
from `{project_id}.ecommerce_mart.mart_daily_sales`
where daily_revenue < 0;

-- name: mart_product_sales_quantity_non_negative
-- type: metric_sanity
-- target_dataset: ecommerce_mart
-- target_table: mart_product_sales
-- severity: ERROR
select count(*) as failed_row_count
from `{project_id}.ecommerce_mart.mart_product_sales`
where quantity_sold < 0;

-- name: mart_daily_sales_aggregate_validation
-- type: mart_aggregate_validation
-- target_dataset: ecommerce_mart
-- target_table: mart_daily_sales
-- severity: WARN
select count(*) as failed_row_count
from (
    select
        m.sales_date,
        abs(m.daily_revenue - coalesce(f.fact_revenue, 0)) as revenue_diff
    from (
        select
            sales_date,
            sum(daily_revenue) as daily_revenue
        from `{project_id}.ecommerce_mart.mart_daily_sales`
        group by 1
    ) as m
    left join (
        select
            foi.order_date as sales_date,
            sum(case when fo.order_status in ('paid', 'shipped', 'delivered') then foi.item_amount else 0 end) as fact_revenue
        from `{project_id}.ecommerce_core.fact_order_item` as foi
        left join `{project_id}.ecommerce_core.fact_order` as fo
            on foi.order_id = fo.order_id
        where not foi.is_deleted
          and not fo.is_deleted
        group by 1
    ) as f
        on m.sales_date = f.sales_date
    where abs(m.daily_revenue - coalesce(f.fact_revenue, 0)) > 1
);

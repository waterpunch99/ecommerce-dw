{{ config(
    materialized='incremental',
    unique_key='product_sk',
    incremental_strategy='merge'
) }}

with source_rows as (
    select
        product_id,
        category_id,
        product_name,
        brand,
        list_price,
        product_status,
        created_at,
        updated_at,
        is_deleted,
        extract_date,
        loaded_at,
        {{ surrogate_key([
            'product_name',
            'category_id',
            'list_price',
            'brand',
            'product_status'
        ]) }} as row_hash
    from {{ ref('stg_products') }}
    where not is_deleted
),

{% if is_incremental() %}

existing_current as (
    select *
    from {{ this }}
    where is_current
),

changed_rows as (
    select s.*
    from source_rows as s
    left join existing_current as e
        on s.product_id = e.product_id
    where e.product_id is null
       or s.row_hash != e.row_hash
),

rows_to_close as (
    select
        e.product_sk,
        e.product_id,
        e.category_id,
        e.product_name,
        e.brand,
        e.list_price,
        e.product_status,
        e.effective_from,
        c.updated_at as effective_to,
        false as is_current,
        e.row_hash,
        e.created_at,
        e.updated_at,
        e.extract_date,
        e.loaded_at
    from existing_current as e
    inner join changed_rows as c
        on e.product_id = c.product_id
),

rows_to_insert as (
    select
        {{ surrogate_key(['product_id', 'updated_at', 'row_hash']) }} as product_sk,
        product_id,
        category_id,
        product_name,
        brand,
        list_price,
        product_status,
        updated_at as effective_from,
        timestamp '9999-12-31 00:00:00' as effective_to,
        true as is_current,
        row_hash,
        created_at,
        updated_at,
        extract_date,
        loaded_at
    from changed_rows
)

select *
from rows_to_close

union all

select *
from rows_to_insert

{% else %}

versioned as (
    select
        *,
        lead(updated_at) over (
            partition by product_id
            order by updated_at, loaded_at
        ) as next_updated_at
    from source_rows
),

scd_rows as (
    select
        {{ surrogate_key(['product_id', 'updated_at', 'row_hash']) }} as product_sk,
        product_id,
        category_id,
        product_name,
        brand,
        list_price,
        product_status,
        updated_at as effective_from,
        coalesce(next_updated_at, timestamp '9999-12-31 00:00:00') as effective_to,
        next_updated_at is null as is_current,
        row_hash,
        created_at,
        updated_at,
        extract_date,
        loaded_at
    from versioned
)

select *
from scd_rows

{% endif %}

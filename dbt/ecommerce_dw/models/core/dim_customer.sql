{{ config(
    materialized='incremental',
    unique_key='customer_sk',
    incremental_strategy='merge'
) }}

with source_rows as (
    select
        customer_id,
        customer_name,
        email,
        phone_number,
        address,
        customer_grade,
        marketing_opt_in,
        signup_date,
        created_at,
        updated_at,
        is_deleted,
        extract_date,
        loaded_at,
        {{ surrogate_key([
            'customer_name',
            'email',
            'phone_number',
            'address',
            'customer_grade',
            'marketing_opt_in'
        ]) }} as row_hash
    from {{ ref('stg_customers') }}
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
        on s.customer_id = e.customer_id
    where e.customer_id is null
       or s.row_hash != e.row_hash
),

rows_to_close as (
    select
        e.customer_sk,
        e.customer_id,
        e.customer_name,
        e.email,
        e.phone_number,
        e.address,
        e.customer_grade,
        e.marketing_opt_in,
        e.signup_date,
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
        on e.customer_id = c.customer_id
),

rows_to_insert as (
    select
        {{ surrogate_key(['customer_id', 'updated_at', 'row_hash']) }} as customer_sk,
        customer_id,
        customer_name,
        email,
        phone_number,
        address,
        customer_grade,
        marketing_opt_in,
        signup_date,
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
            partition by customer_id
            order by updated_at, loaded_at
        ) as next_updated_at
    from source_rows
),

scd_rows as (
    select
        {{ surrogate_key(['customer_id', 'updated_at', 'row_hash']) }} as customer_sk,
        customer_id,
        customer_name,
        email,
        phone_number,
        address,
        customer_grade,
        marketing_opt_in,
        signup_date,
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

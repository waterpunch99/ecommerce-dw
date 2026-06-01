with source as (
    {{ dedupe_latest(source('ecommerce_raw', 'customers'), 'customer_id') }}
)

select
    cast(customer_id as int64) as customer_id,
    cast(customer_name as string) as customer_name,
    lower(cast(email as string)) as email,
    cast(phone_number as string) as phone_number,
    cast(address as string) as address,
    cast(customer_grade as string) as customer_grade,
    cast(marketing_opt_in as bool) as marketing_opt_in,
    cast(signup_date as date) as signup_date,
    cast(created_at as timestamp) as created_at,
    cast(updated_at as timestamp) as updated_at,
    cast(is_deleted as bool) as is_deleted,
    cast(extract_date as date) as extract_date,
    cast(extracted_at as timestamp) as extracted_at,
    cast(batch_id as string) as batch_id,
    cast(loaded_at as timestamp) as loaded_at
from source


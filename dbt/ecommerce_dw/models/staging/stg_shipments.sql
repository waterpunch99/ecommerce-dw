with source as (
    {{ dedupe_latest(source('ecommerce_raw', 'shipments'), 'shipment_id') }}
)

select
    cast(shipment_id as int64) as shipment_id,
    cast(order_id as int64) as order_id,
    cast(shipment_status as string) as shipment_status,
    cast(carrier as string) as carrier,
    cast(tracking_number as string) as tracking_number,
    cast(shipped_at as timestamp) as shipped_at,
    cast(delivered_at as timestamp) as delivered_at,
    cast(created_at as timestamp) as created_at,
    cast(updated_at as timestamp) as updated_at,
    cast(is_deleted as bool) as is_deleted,
    cast(extract_date as date) as extract_date,
    cast(extracted_at as timestamp) as extracted_at,
    cast(batch_id as string) as batch_id,
    cast(loaded_at as timestamp) as loaded_at
from source


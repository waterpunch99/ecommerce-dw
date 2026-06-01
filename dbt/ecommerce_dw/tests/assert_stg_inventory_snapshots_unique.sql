select
    snapshot_date,
    product_id,
    count(*) as row_count
from {{ ref('stg_inventory_snapshots') }}
group by 1, 2
having count(*) > 1


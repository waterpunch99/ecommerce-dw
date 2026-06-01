select
    snapshot_date,
    product_id,
    count(*) as row_count
from {{ ref('fact_inventory_snapshot') }}
group by 1, 2
having count(*) > 1


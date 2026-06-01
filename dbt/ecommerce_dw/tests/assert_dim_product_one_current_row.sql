select
    product_id,
    count(*) as current_row_count
from {{ ref('dim_product') }}
where is_current
group by 1
having count(*) > 1


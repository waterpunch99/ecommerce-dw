select
    customer_id,
    count(*) as current_row_count
from {{ ref('dim_customer') }}
where is_current
group by 1
having count(*) > 1


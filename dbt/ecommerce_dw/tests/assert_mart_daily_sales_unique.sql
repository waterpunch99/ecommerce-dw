select
    sales_date,
    category_id,
    count(*) as row_count
from {{ ref('mart_daily_sales') }}
group by 1, 2
having count(*) > 1


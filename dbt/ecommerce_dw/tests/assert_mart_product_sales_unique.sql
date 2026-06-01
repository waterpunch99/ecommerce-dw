select
    sales_date,
    product_id,
    count(*) as row_count
from {{ ref('mart_product_sales') }}
group by 1, 2
having count(*) > 1


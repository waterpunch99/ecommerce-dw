select
    cohort_month,
    order_month,
    customer_segment,
    count(*) as row_count
from {{ ref('mart_monthly_retention') }}
group by 1, 2, 3
having count(*) > 1


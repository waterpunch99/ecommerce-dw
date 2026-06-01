# Data Quality

## Overview

데이터 품질 검증은 dbt 기본 테스트와 별도 SQL 검증을 함께 사용합니다. dbt test는 모델 스키마 수준의 일반 검증을 담당하고, 별도 SQL 검증은 운영성 검증과 Mart 지표 정합성 검증을 담당합니다.

## Quality Result Table

검증 결과는 BigQuery Audit Dataset의 다음 테이블에 저장합니다.

```text
ecommerce_audit.data_quality_results
```

관리 컬럼 예시:

- `check_id`
- `check_name`
- `check_type`
- `target_dataset`
- `target_table`
- `severity`
- `status`
- `failed_row_count`
- `checked_at`
- `batch_id`
- `execution_date`
- `error_message`

DDL:

- `sql/bigquery/002_create_data_quality_tables.sql`

## dbt Tests

dbt test 대상:

- `not_null`
- `unique`
- `relationships`
- `accepted_values`

Staging Layer에는 다음 기본 테스트를 정의합니다.

- Staging 모델별 Natural Key `not_null`
- Staging 모델별 Natural Key `unique`
- `stg_inventory_snapshots`의 `snapshot_date`, `product_id` 복합키 중복 검증
- 주요 FK 관계 `relationships`
- 주요 상태값 `accepted_values`

예시:

- `orders.order_id`는 null이면 안 됩니다.
- `fact_order_item.order_item_id`는 unique해야 합니다.
- `fact_order_item.order_id`는 `fact_order.order_id`에 존재해야 합니다.
- `order_status`는 허용된 값 안에 있어야 합니다.

## SQL Quality Checks

별도 SQL 검증 대상:

- Primary Key not null
- Primary Key uniqueness
- Foreign Key relationship check
- Accepted values check
- Amount range check
- Date range check
- Freshness check
- Row count check
- Duplicate check
- Mart metric sanity check

구현 파일:

- `sql/bigquery/quality_checks.sql`
- `scripts/quality/run_bigquery_quality_checks.py`

검증 SQL은 각 check별로 `failed_row_count`를 반환합니다. 스크립트는 결과를 `ecommerce_audit.data_quality_results`에 저장하고, `severity = ERROR` 검증이 실패하면 비정상 종료합니다.

## Required Checks

### Primary Key Not Null

주요 Fact와 Dimension의 Natural Key는 null이면 안 됩니다.

예시:

- `fact_order.order_id`
- `fact_order_item.order_item_id`
- `fact_payment.payment_id`
- `dim_customer.customer_id`
- `dim_product.product_id`

### Primary Key Uniqueness

Fact Natural Key는 중복되면 안 됩니다. SCD Type 2 Dimension은 Natural Key 단독 unique가 아니라 current row 조건을 포함해 검증합니다.

예시:

- `fact_order.order_id` 중복 금지
- `fact_order_item.order_item_id` 중복 금지
- `dim_customer`에서 `customer_id`별 current row는 1개 이하
- `dim_product`에서 `product_id`별 current row는 1개 이하

### Foreign Key Relationship

Fact가 참조하는 Dimension 또는 상위 Fact가 존재해야 합니다.

예시:

- `fact_order_item.order_id`는 `fact_order.order_id`에 존재해야 합니다.
- `fact_payment.order_id`는 `fact_order.order_id`에 존재해야 합니다.
- `fact_order_item.product_id`는 `dim_product.product_id`에 존재해야 합니다.

### Accepted Values

상태값은 허용된 값만 가져야 합니다.

예시:

- `order_status`
- `payment_status`
- `shipment_status`
- `product_status`

### Amount Range

금액성 컬럼은 기본적으로 음수가 될 수 없습니다. 환불 등 음수 표현이 필요한 경우 별도 비즈니스 규칙으로 관리합니다.

예시:

- `payment_amount >= 0`
- `item_amount >= 0`
- `discount_amount >= 0`

### Freshness

Raw Layer에 최근 1일 이내 데이터가 적재되었는지 확인합니다.

### Row Count

Source, Raw, Staging, Core 간 row count 급감 또는 급증을 확인합니다.

### Mart Metric Sanity

Mart 지표가 비정상 값을 갖지 않는지 확인합니다.

예시:

- `mart_daily_sales.daily_revenue >= 0`
- `mart_daily_sales.order_count >= 0`
- `mart_product_sales.quantity_sold >= 0`

## Failure Handling

품질 검증 실패 시 Airflow Task를 실패 처리합니다. 실패한 실행은 watermark를 갱신하지 않습니다.

CLI 스크립트는 실패 종료 코드를 반환하며, Airflow DAG는 이 종료 코드를 기준으로 파이프라인 실패와 watermark 갱신 차단을 처리합니다.

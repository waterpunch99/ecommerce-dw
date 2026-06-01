# Operations

## Overview

운영 전략은 실패 처리, 재실행, 백필, 멱등성, watermark 관리에 집중합니다. 목표는 같은 배치나 같은 실행일을 다시 실행해도 데이터 중복이나 잘못된 watermark 갱신이 발생하지 않는 것입니다.

운영 관점의 핵심 원칙:

- 실패한 실행은 `last_success_watermark`를 갱신하지 않습니다.
- 같은 `batch_id`는 같은 GCS 경로와 Raw 적재 범위를 의미합니다.
- Raw는 동일 `batch_id` 삭제 후 재적재합니다.
- Core는 MERGE 기준 Key로 Upsert합니다.
- Mart는 날짜 파티션 범위 기준으로 재처리합니다.

로컬 개발 환경은 `.env.example`을 복사한 `.env`를 기준으로 실행합니다. 실제 GCP 서비스 계정 JSON 파일은 프로젝트에서 제공하지 않으며, 로컬 사용자 환경에서 별도로 준비합니다.

기본 실행 명령:

```bash
cp .env.example .env
docker compose up airflow-init
docker compose up -d
```

종료 명령:

```bash
docker compose down
```

볼륨까지 삭제해 완전히 초기화할 때만 다음 명령을 사용합니다.

```bash
docker compose down -v
```

## Failure Handling

파이프라인은 다음 단계 중 하나라도 실패하면 전체 실행을 실패로 간주합니다.

- Postgres extract
- GCS upload
- BigQuery Raw load
- dbt staging/core/mart model run
- dbt test
- SQL quality checks

실패 시 처리:

- `last_success_watermark`를 갱신하지 않습니다.
- 실패 상태와 오류 메시지를 metadata 또는 audit 테이블에 기록합니다.
- 같은 기간을 재실행할 수 있도록 입력 경계값을 보존합니다.

## Watermark Update Rule

watermark는 전체 파이프라인이 성공한 뒤 마지막 단계에서만 갱신합니다.

성공 조건:

1. GCS Raw 파일 저장 성공
2. BigQuery Raw 적재 성공
3. dbt Staging 모델 성공
4. dbt Core 모델 성공
5. dbt Mart 모델 성공
6. 데이터 품질 검증 성공

위 조건을 모두 만족한 경우:

- `last_success_watermark = current_run_watermark`
- `last_run_at` 갱신
- `status = success`
- `row_count` 기록
- `error_message = null`

Postgres 추출 스크립트는 `current_run_watermark`, batch 실행 상태, row count, GCS URI만 기록합니다. `last_success_watermark`는 변경하지 않으므로 추출 성공 후 후속 단계가 실패해도 다음 실행에서 동일 범위를 다시 처리할 수 있습니다.

Airflow DAG에서는 `metadata_update` TaskGroup이 마지막에 위치합니다. 이 TaskGroup은 `scripts/metadata/update_watermarks.py`를 실행하며, 모든 선행 TaskGroup이 성공한 경우에만 `last_success_watermark`를 갱신합니다.

## Retry Strategy

Airflow Task에는 제한적인 retry를 설정합니다. 일시적 네트워크 오류나 BigQuery Load Job 지연에는 retry가 유효하지만, 품질 검증 실패나 SQL 로직 오류는 retry만으로 해결되지 않을 가능성이 높습니다.

기본 원칙:

- 외부 시스템 일시 오류는 retry 대상입니다.
- 데이터 품질 실패는 원인 분석 후 재실행합니다.
- 실패한 실행은 watermark를 유지합니다.

## Reprocessing Strategy

### Same Batch Re-run

같은 `batch_id`를 재실행할 경우 GCS 경로를 덮어쓰거나 해당 `batch_id` 경로를 정리한 뒤 다시 적재합니다.

```text
gs://{bucket}/ecommerce/raw/{table_name}/extract_date={YYYY-MM-DD}/batch_id={batch_id}/{table_name}.parquet
```

같은 `batch_id` 재실행 시 다음 제어가 적용됩니다.

- GCS 객체명은 동일하게 생성되어 같은 위치에 다시 업로드됩니다.
- BigQuery Raw 적재 전 대상 Raw 테이블에서 동일 `batch_id` rows를 삭제합니다.
- Core Layer는 dbt incremental `merge` 기준 Key로 Upsert합니다.
- Mart Layer는 dbt vars로 받은 날짜 범위를 기준으로 파티션을 재처리합니다.

### Same Execution Date Re-run

같은 `execution_date`를 재실행할 경우:

- Raw 중복 적재를 제어합니다.
- BigQuery Raw는 동일 `batch_id` rows를 삭제한 뒤 GCS Parquet 파일을 다시 Load Job으로 적재합니다.
- Core는 `MERGE` 기준 Key로 Upsert합니다.
- Mart는 대상 파티션을 Delete 후 Insert합니다.
- watermark는 성공 시에만 갱신합니다.

### Backfill

백필은 날짜 범위를 명시해 실행합니다.

백필 시 고려사항:

- Source 추출 범위
- GCS Raw 경로의 `extract_date`
- BigQuery Raw 중복 제어
- Core MERGE 기준 Key
- Mart 대상 파티션 삭제 범위
- 품질 검증 범위

Airflow DAG run config 예시:

```json
{
  "batch_id": "backfill_20250101_20250107",
  "start_watermark": "2025-01-01T00:00:00",
  "end_watermark": "2025-01-08T00:00:00",
  "mart_start_date": "2025-01-01",
  "mart_end_date": "2025-01-07",
  "watermark_mode": "preserve"
}
```

`watermark_mode=preserve`는 백필 성공 후에도 운영 daily pipeline의 `last_success_watermark`를 전진시키지 않습니다. 운영 daily 실행에서는 기본값인 `watermark_mode=advance`를 사용합니다.

## Idempotency Checklist

- GCS 객체 경로가 `batch_id`를 포함하는가
- BigQuery Raw 적재 시 동일 배치 중복이 제어되는가
- Raw 적재 이력이 `ecommerce_audit.raw_load_runs`에 남는가
- Fact MERGE 기준 Natural Key가 명확한가
- SCD Type 2 Dimension의 변경 감지 기준이 `row_hash`로 명확한가
- Mart 재처리가 Delete & Insert 방식으로 구현되는가
- 실패한 실행이 watermark를 갱신하지 않는가
- 품질 검증 실패 시 파이프라인이 실패하는가

## Operational Tables

Audit Dataset에는 다음 목적의 테이블을 둡니다.

- 적재 실행 이력
- source table별 watermark
- data quality check 결과
- row count 비교 결과

구체적인 DDL은 이후 BigQuery Raw 적재와 품질 검증 구현 단계에서 작성합니다.

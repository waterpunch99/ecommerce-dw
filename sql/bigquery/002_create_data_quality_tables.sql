create schema if not exists `{project_id}.ecommerce_audit`;

create table if not exists `{project_id}.ecommerce_audit.data_quality_results` (
    check_id string not null,
    check_name string not null,
    check_type string not null,
    target_dataset string not null,
    target_table string not null,
    severity string not null,
    status string not null,
    failed_row_count int64 not null,
    checked_at timestamp not null,
    batch_id string,
    execution_date date,
    error_message string
)
partition by date(checked_at)
cluster by target_table, check_type, status;


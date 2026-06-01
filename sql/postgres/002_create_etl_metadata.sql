create table if not exists etl_watermarks (
    pipeline_name varchar(100) not null,
    source_table varchar(100) not null,
    last_success_watermark timestamp not null default timestamp '1970-01-01 00:00:00',
    current_run_watermark timestamp,
    last_run_at timestamp,
    status varchar(30) not null default 'not_started',
    row_count bigint not null default 0,
    error_message text,
    created_at timestamp not null default current_timestamp,
    updated_at timestamp not null default current_timestamp,
    primary key (pipeline_name, source_table)
);

create table if not exists etl_batch_runs (
    batch_id varchar(100) not null,
    pipeline_name varchar(100) not null,
    source_table varchar(100) not null,
    extract_date date not null,
    last_success_watermark timestamp not null,
    current_run_watermark timestamp not null,
    gcs_uri text,
    row_count bigint not null default 0,
    status varchar(30) not null,
    error_message text,
    started_at timestamp not null default current_timestamp,
    finished_at timestamp,
    primary key (batch_id, pipeline_name, source_table)
);

create index if not exists idx_etl_batch_runs_pipeline_table
    on etl_batch_runs(pipeline_name, source_table, started_at);


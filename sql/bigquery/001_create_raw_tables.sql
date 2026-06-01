create schema if not exists `{project_id}.ecommerce_raw`;
create schema if not exists `{project_id}.ecommerce_audit`;

create table if not exists `{project_id}.ecommerce_audit.raw_load_runs` (
    batch_id string not null,
    source_table string not null,
    gcs_uri string not null,
    target_table string not null,
    row_count int64,
    status string not null,
    error_message string,
    started_at timestamp not null,
    finished_at timestamp
);

create table if not exists `{project_id}.ecommerce_raw.categories` (
    category_id int64,
    parent_category_id int64,
    category_name string,
    category_level int64,
    is_active bool,
    created_at timestamp,
    updated_at timestamp,
    is_deleted bool,
    extract_date date,
    extracted_at timestamp,
    source_table string,
    batch_id string,
    loaded_at timestamp
)
partition by extract_date
cluster by category_id, batch_id;

create table if not exists `{project_id}.ecommerce_raw.customers` (
    customer_id int64,
    customer_name string,
    email string,
    phone_number string,
    address string,
    customer_grade string,
    marketing_opt_in bool,
    signup_date date,
    created_at timestamp,
    updated_at timestamp,
    is_deleted bool,
    extract_date date,
    extracted_at timestamp,
    source_table string,
    batch_id string,
    loaded_at timestamp
)
partition by extract_date
cluster by customer_id, batch_id;

create table if not exists `{project_id}.ecommerce_raw.products` (
    product_id int64,
    category_id int64,
    product_name string,
    brand string,
    list_price numeric,
    product_status string,
    created_at timestamp,
    updated_at timestamp,
    is_deleted bool,
    extract_date date,
    extracted_at timestamp,
    source_table string,
    batch_id string,
    loaded_at timestamp
)
partition by extract_date
cluster by product_id, category_id;

create table if not exists `{project_id}.ecommerce_raw.coupons` (
    coupon_id int64,
    coupon_code string,
    coupon_name string,
    discount_type string,
    discount_value numeric,
    valid_from timestamp,
    valid_to timestamp,
    created_at timestamp,
    updated_at timestamp,
    is_deleted bool,
    extract_date date,
    extracted_at timestamp,
    source_table string,
    batch_id string,
    loaded_at timestamp
)
partition by extract_date
cluster by coupon_id, batch_id;

create table if not exists `{project_id}.ecommerce_raw.orders` (
    order_id int64,
    customer_id int64,
    coupon_id int64,
    order_status string,
    order_channel string,
    order_at timestamp,
    subtotal_amount numeric,
    discount_amount numeric,
    shipping_fee numeric,
    total_amount numeric,
    created_at timestamp,
    updated_at timestamp,
    is_deleted bool,
    extract_date date,
    extracted_at timestamp,
    source_table string,
    batch_id string,
    loaded_at timestamp
)
partition by extract_date
cluster by order_id, customer_id;

create table if not exists `{project_id}.ecommerce_raw.order_items` (
    order_item_id int64,
    order_id int64,
    product_id int64,
    quantity int64,
    unit_price numeric,
    discount_amount numeric,
    item_amount numeric,
    created_at timestamp,
    updated_at timestamp,
    is_deleted bool,
    extract_date date,
    extracted_at timestamp,
    source_table string,
    batch_id string,
    loaded_at timestamp
)
partition by extract_date
cluster by order_id, product_id;

create table if not exists `{project_id}.ecommerce_raw.payments` (
    payment_id int64,
    order_id int64,
    payment_method string,
    payment_status string,
    payment_amount numeric,
    payment_at timestamp,
    transaction_id string,
    created_at timestamp,
    updated_at timestamp,
    is_deleted bool,
    extract_date date,
    extracted_at timestamp,
    source_table string,
    batch_id string,
    loaded_at timestamp
)
partition by extract_date
cluster by order_id, payment_status;

create table if not exists `{project_id}.ecommerce_raw.shipments` (
    shipment_id int64,
    order_id int64,
    shipment_status string,
    carrier string,
    tracking_number string,
    shipped_at timestamp,
    delivered_at timestamp,
    created_at timestamp,
    updated_at timestamp,
    is_deleted bool,
    extract_date date,
    extracted_at timestamp,
    source_table string,
    batch_id string,
    loaded_at timestamp
)
partition by extract_date
cluster by order_id, shipment_status;

create table if not exists `{project_id}.ecommerce_raw.inventory_snapshots` (
    snapshot_date date,
    product_id int64,
    category_id int64,
    stock_quantity int64,
    reserved_quantity int64,
    available_quantity int64,
    created_at timestamp,
    updated_at timestamp,
    is_deleted bool,
    extract_date date,
    extracted_at timestamp,
    source_table string,
    batch_id string,
    loaded_at timestamp
)
partition by extract_date
cluster by product_id, category_id;

create table if not exists `{project_id}.ecommerce_raw.order_status_history` (
    order_status_history_id int64,
    order_id int64,
    previous_status string,
    new_status string,
    changed_at timestamp,
    change_reason string,
    created_at timestamp,
    updated_at timestamp,
    is_deleted bool,
    extract_date date,
    extracted_at timestamp,
    source_table string,
    batch_id string,
    loaded_at timestamp
)
partition by extract_date
cluster by order_id, new_status;


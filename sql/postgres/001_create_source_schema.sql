create table if not exists categories (
    category_id bigserial primary key,
    parent_category_id bigint references categories(category_id),
    category_name varchar(100) not null,
    category_level integer not null default 1,
    is_active boolean not null default true,
    created_at timestamp not null,
    updated_at timestamp not null,
    is_deleted boolean not null default false
);

create table if not exists customers (
    customer_id bigserial primary key,
    customer_name varchar(100) not null,
    email varchar(255) not null unique,
    phone_number varchar(50),
    address text,
    customer_grade varchar(30) not null,
    marketing_opt_in boolean not null default false,
    signup_date date not null,
    created_at timestamp not null,
    updated_at timestamp not null,
    is_deleted boolean not null default false
);

create table if not exists products (
    product_id bigserial primary key,
    category_id bigint not null references categories(category_id),
    product_name varchar(200) not null,
    brand varchar(100) not null,
    list_price numeric(12, 2) not null check (list_price >= 0),
    product_status varchar(30) not null,
    created_at timestamp not null,
    updated_at timestamp not null,
    is_deleted boolean not null default false
);

create table if not exists coupons (
    coupon_id bigserial primary key,
    coupon_code varchar(50) not null unique,
    coupon_name varchar(100) not null,
    discount_type varchar(30) not null,
    discount_value numeric(12, 2) not null check (discount_value >= 0),
    valid_from timestamp not null,
    valid_to timestamp not null,
    created_at timestamp not null,
    updated_at timestamp not null,
    is_deleted boolean not null default false,
    constraint coupons_valid_period_check check (valid_from <= valid_to)
);

create table if not exists orders (
    order_id bigserial primary key,
    customer_id bigint not null references customers(customer_id),
    coupon_id bigint references coupons(coupon_id),
    order_status varchar(30) not null,
    order_channel varchar(30) not null,
    order_at timestamp not null,
    subtotal_amount numeric(12, 2) not null check (subtotal_amount >= 0),
    discount_amount numeric(12, 2) not null default 0 check (discount_amount >= 0),
    shipping_fee numeric(12, 2) not null default 0 check (shipping_fee >= 0),
    total_amount numeric(12, 2) not null check (total_amount >= 0),
    created_at timestamp not null,
    updated_at timestamp not null,
    is_deleted boolean not null default false
);

create table if not exists order_items (
    order_item_id bigserial primary key,
    order_id bigint not null references orders(order_id),
    product_id bigint not null references products(product_id),
    quantity integer not null check (quantity > 0),
    unit_price numeric(12, 2) not null check (unit_price >= 0),
    discount_amount numeric(12, 2) not null default 0 check (discount_amount >= 0),
    item_amount numeric(12, 2) not null check (item_amount >= 0),
    created_at timestamp not null,
    updated_at timestamp not null,
    is_deleted boolean not null default false
);

create table if not exists payments (
    payment_id bigserial primary key,
    order_id bigint not null references orders(order_id),
    payment_method varchar(30) not null,
    payment_status varchar(30) not null,
    payment_amount numeric(12, 2) not null check (payment_amount >= 0),
    payment_at timestamp,
    transaction_id varchar(100) not null unique,
    created_at timestamp not null,
    updated_at timestamp not null,
    is_deleted boolean not null default false
);

create table if not exists shipments (
    shipment_id bigserial primary key,
    order_id bigint not null references orders(order_id),
    shipment_status varchar(30) not null,
    carrier varchar(50) not null,
    tracking_number varchar(100) not null unique,
    shipped_at timestamp,
    delivered_at timestamp,
    created_at timestamp not null,
    updated_at timestamp not null,
    is_deleted boolean not null default false,
    constraint shipments_delivery_period_check check (
        delivered_at is null or shipped_at is null or shipped_at <= delivered_at
    )
);

create table if not exists inventory_snapshots (
    snapshot_date date not null,
    product_id bigint not null references products(product_id),
    category_id bigint not null references categories(category_id),
    stock_quantity integer not null check (stock_quantity >= 0),
    reserved_quantity integer not null default 0 check (reserved_quantity >= 0),
    available_quantity integer not null check (available_quantity >= 0),
    created_at timestamp not null,
    updated_at timestamp not null,
    is_deleted boolean not null default false,
    primary key (snapshot_date, product_id)
);

create table if not exists order_status_history (
    order_status_history_id bigserial primary key,
    order_id bigint not null references orders(order_id),
    previous_status varchar(30),
    new_status varchar(30) not null,
    changed_at timestamp not null,
    change_reason varchar(200),
    created_at timestamp not null,
    updated_at timestamp not null,
    is_deleted boolean not null default false
);

create index if not exists idx_customers_updated_at on customers(updated_at);
create index if not exists idx_products_updated_at on products(updated_at);
create index if not exists idx_categories_updated_at on categories(updated_at);
create index if not exists idx_orders_updated_at on orders(updated_at);
create index if not exists idx_order_items_updated_at on order_items(updated_at);
create index if not exists idx_payments_updated_at on payments(updated_at);
create index if not exists idx_shipments_updated_at on shipments(updated_at);
create index if not exists idx_inventory_snapshots_updated_at on inventory_snapshots(updated_at);
create index if not exists idx_coupons_updated_at on coupons(updated_at);
create index if not exists idx_order_status_history_updated_at on order_status_history(updated_at);


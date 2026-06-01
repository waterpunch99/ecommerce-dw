from __future__ import annotations

import argparse
import os
import random
from datetime import date, datetime, time, timedelta
from decimal import Decimal, ROUND_HALF_UP


CUSTOMER_GRADES = ["bronze", "silver", "gold", "vip"]
PRODUCT_STATUSES = ["active", "inactive", "discontinued"]
ORDER_STATUSES = ["created", "paid", "shipped", "delivered", "cancelled", "refunded"]
PAYMENT_METHODS = ["card", "bank_transfer", "kakao_pay", "naver_pay", "point"]
PAYMENT_STATUSES = ["authorized", "paid", "failed", "cancelled", "refunded"]
SHIPMENT_STATUSES = ["ready", "shipped", "delivered", "returned"]
ORDER_CHANNELS = ["web", "mobile_app", "mobile_web"]
DISCOUNT_TYPES = ["fixed_amount", "percentage"]
CARRIERS = ["CJ Logistics", "Lotte Global Logistics", "Hanjin", "Korea Post"]
BRANDS = ["Northwind", "Urbanic", "Dailylab", "Freshday", "Monomart", "Bluebasket"]
ROOT_CATEGORIES = ["Fashion", "Beauty", "Digital", "Home", "Food", "Sports"]

Faker = None
psycopg2 = None
execute_batch = None


def load_dependencies():
    global Faker, psycopg2, execute_batch

    try:
        from faker import Faker as faker_cls
        import psycopg2 as psycopg2_module
        from psycopg2.extras import execute_batch as execute_batch_fn
    except ModuleNotFoundError as exc:
        raise SystemExit(
            f"Missing Python dependency: {exc.name}. "
            "Install dependencies with `pip install -r requirements.txt` "
            "or run the script inside the Airflow container."
        ) from exc

    Faker = faker_cls
    psycopg2 = psycopg2_module
    execute_batch = execute_batch_fn


def money(value: float) -> Decimal:
    return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def cap_datetime(value: datetime, max_value: datetime) -> datetime:
    return min(value, max_value)


def connect():
    return psycopg2.connect(
        host=os.getenv("SOURCE_POSTGRES_HOST", "localhost"),
        port=int(os.getenv("SOURCE_POSTGRES_PORT", "5432")),
        dbname=os.getenv("SOURCE_POSTGRES_DB", "ecommerce"),
        user=os.getenv("SOURCE_POSTGRES_USER", "ecommerce_user"),
        password=os.getenv("SOURCE_POSTGRES_PASSWORD", "ecommerce_password"),
    )


def fetch_ids(cur, table_name: str, id_column: str):
    cur.execute(f"select {id_column} from {table_name} order by {id_column}")
    return [row[0] for row in cur.fetchall()]


def reset_tables(cur):
    cur.execute(
        """
        truncate table
            order_status_history,
            inventory_snapshots,
            shipments,
            payments,
            order_items,
            orders,
            coupons,
            products,
            customers,
            categories
        restart identity cascade
        """
    )


def seed_categories(cur, fake: Faker, base_ts: datetime):
    rows = []
    for name in ROOT_CATEGORIES:
        ts = base_ts + timedelta(minutes=random.randint(0, 60))
        rows.append((None, name, 1, True, ts, ts, False))

    execute_batch(
        cur,
        """
        insert into categories (
            parent_category_id, category_name, category_level, is_active,
            created_at, updated_at, is_deleted
        )
        values (%s, %s, %s, %s, %s, %s, %s)
        """,
        rows,
    )

    root_ids = fetch_ids(cur, "categories", "category_id")
    child_rows = []
    for root_id in root_ids:
        for _ in range(3):
            ts = base_ts + timedelta(minutes=random.randint(60, 180))
            child_rows.append(
                (
                    root_id,
                    fake.unique.word().title(),
                    2,
                    True,
                    ts,
                    ts,
                    False,
                )
            )

    execute_batch(
        cur,
        """
        insert into categories (
            parent_category_id, category_name, category_level, is_active,
            created_at, updated_at, is_deleted
        )
        values (%s, %s, %s, %s, %s, %s, %s)
        """,
        child_rows,
    )


def seed_customers(cur, fake: Faker, count: int, start_date: date, end_date: date, max_updated_at: datetime):
    rows = []
    for _ in range(count):
        signup_date = fake.date_between(start_date=start_date, end_date=end_date)
        created_at = datetime.combine(signup_date, fake.time_object())
        updated_at = cap_datetime(created_at + timedelta(days=random.randint(0, 20)), max_updated_at)
        rows.append(
            (
                fake.name(),
                fake.unique.email(),
                fake.phone_number()[:50],
                fake.address().replace("\n", " "),
                random.choices(CUSTOMER_GRADES, weights=[50, 30, 15, 5], k=1)[0],
                random.random() < 0.55,
                signup_date,
                created_at,
                updated_at,
                False,
            )
        )

    execute_batch(
        cur,
        """
        insert into customers (
            customer_name, email, phone_number, address, customer_grade,
            marketing_opt_in, signup_date, created_at, updated_at, is_deleted
        )
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        rows,
        page_size=500,
    )


def seed_products(cur, fake: Faker, count: int, base_ts: datetime):
    category_ids = fetch_ids(cur, "categories", "category_id")
    rows = []
    for _ in range(count):
        created_at = base_ts + timedelta(days=random.randint(0, 30), minutes=random.randint(0, 1440))
        updated_at = created_at + timedelta(days=random.randint(0, 15))
        rows.append(
            (
                random.choice(category_ids),
                f"{fake.word().title()} {fake.word().title()}",
                random.choice(BRANDS),
                money(random.uniform(8, 500) * 1000),
                random.choices(PRODUCT_STATUSES, weights=[85, 10, 5], k=1)[0],
                created_at,
                updated_at,
                False,
            )
        )

    execute_batch(
        cur,
        """
        insert into products (
            category_id, product_name, brand, list_price, product_status,
            created_at, updated_at, is_deleted
        )
        values (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        rows,
        page_size=500,
    )


def seed_coupons(cur, fake: Faker, count: int, base_ts: datetime):
    rows = []
    for index in range(count):
        valid_from = base_ts + timedelta(days=random.randint(0, 20))
        valid_to = valid_from + timedelta(days=random.randint(30, 120))
        discount_type = random.choice(DISCOUNT_TYPES)
        discount_value = money(random.randint(5, 30) if discount_type == "percentage" else random.randint(3, 20) * 1000)
        created_at = valid_from - timedelta(days=random.randint(1, 10))
        rows.append(
            (
                f"CPN{index + 1:04d}",
                f"{fake.word().title()} Coupon",
                discount_type,
                discount_value,
                valid_from,
                valid_to,
                created_at,
                created_at,
                False,
            )
        )

    execute_batch(
        cur,
        """
        insert into coupons (
            coupon_code, coupon_name, discount_type, discount_value,
            valid_from, valid_to, created_at, updated_at, is_deleted
        )
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        rows,
    )


def get_product_lookup(cur):
    cur.execute("select product_id, category_id, list_price from products where product_status = 'active'")
    return cur.fetchall()


def calculate_coupon_discount(subtotal: Decimal, coupon):
    if coupon is None:
        return money(0)

    _, discount_type, discount_value = coupon
    if discount_type == "percentage":
        return min(money(float(subtotal) * float(discount_value) / 100), money(float(subtotal) * 0.4))
    return min(discount_value, money(float(subtotal) * 0.5))


def seed_orders(cur, fake: Faker, count: int, start_date: date, end_date: date, max_updated_at: datetime):
    customer_ids = fetch_ids(cur, "customers", "customer_id")
    products = get_product_lookup(cur)
    cur.execute("select coupon_id, discount_type, discount_value from coupons")
    coupons = cur.fetchall()

    order_rows = []
    order_payloads = []
    for _ in range(count):
        order_day = fake.date_between(start_date=start_date, end_date=end_date)
        order_at = datetime.combine(order_day, fake.time_object())
        selected_products = random.sample(products, k=random.randint(1, min(4, len(products))))
        item_drafts = []
        subtotal = money(0)
        for product_id, category_id, list_price in selected_products:
            quantity = random.randint(1, 3)
            item_discount = money(float(list_price) * quantity * random.choice([0, 0, 0.03, 0.05, 0.1]))
            item_amount = max(money(0), money(float(list_price) * quantity) - item_discount)
            subtotal += item_amount
            item_drafts.append((product_id, category_id, quantity, list_price, item_discount, item_amount))

        coupon = random.choice(coupons) if coupons and random.random() < 0.25 else None
        discount_amount = calculate_coupon_discount(subtotal, coupon)
        shipping_fee = money(0 if subtotal >= 50000 else random.choice([2500, 3000, 3500]))
        total_amount = max(money(0), subtotal - discount_amount + shipping_fee)
        status = random.choices(ORDER_STATUSES, weights=[8, 20, 18, 42, 8, 4], k=1)[0]
        updated_at = cap_datetime(order_at + timedelta(hours=random.randint(0, 96)), max_updated_at)
        order_rows.append(
            (
                random.choice(customer_ids),
                coupon[0] if coupon else None,
                status,
                random.choice(ORDER_CHANNELS),
                order_at,
                subtotal,
                discount_amount,
                shipping_fee,
                total_amount,
                order_at,
                updated_at,
                False,
            )
        )
        order_payloads.append((item_drafts, status, order_at, total_amount))

    execute_batch(
        cur,
        """
        insert into orders (
            customer_id, coupon_id, order_status, order_channel, order_at,
            subtotal_amount, discount_amount, shipping_fee, total_amount,
            created_at, updated_at, is_deleted
        )
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        order_rows,
        page_size=500,
    )

    cur.execute("select order_id from orders order by order_id")
    order_ids = [row[0] for row in cur.fetchall()]
    seed_order_children(cur, fake, order_ids, order_payloads, max_updated_at)


def seed_order_children(cur, fake: Faker, order_ids, order_payloads, max_updated_at: datetime):
    item_rows = []
    payment_rows = []
    shipment_rows = []
    history_rows = []

    for order_id, payload in zip(order_ids, order_payloads):
        item_drafts, order_status, order_at, total_amount = payload
        for product_id, _category_id, quantity, unit_price, discount_amount, item_amount in item_drafts:
            item_rows.append(
                (
                    order_id,
                    product_id,
                    quantity,
                    unit_price,
                    discount_amount,
                    item_amount,
                    order_at,
                    cap_datetime(order_at + timedelta(hours=random.randint(0, 12)), max_updated_at),
                    False,
                )
            )

        payment_status = payment_status_for_order(order_status)
        payment_at = order_at + timedelta(minutes=random.randint(1, 30)) if payment_status in ["paid", "refunded"] else None
        payment_rows.append(
            (
                order_id,
                random.choice(PAYMENT_METHODS),
                payment_status,
                total_amount,
                payment_at,
                fake.unique.bothify(text="txn-########-????"),
                order_at,
                cap_datetime(order_at + timedelta(hours=random.randint(0, 24)), max_updated_at),
                False,
            )
        )

        if order_status in ["shipped", "delivered"]:
            shipped_at = order_at + timedelta(days=random.randint(1, 3))
            delivered_at = shipped_at + timedelta(days=random.randint(1, 4)) if order_status == "delivered" else None
            shipment_rows.append(
                (
                    order_id,
                    "delivered" if delivered_at else "shipped",
                    random.choice(CARRIERS),
                    fake.unique.bothify(text="TRK##########"),
                    shipped_at,
                    delivered_at,
                    order_at,
                    cap_datetime(delivered_at or shipped_at, max_updated_at),
                    False,
                )
            )

        history_rows.extend(build_status_history(order_id, order_status, order_at, max_updated_at))

    execute_batch(
        cur,
        """
        insert into order_items (
            order_id, product_id, quantity, unit_price, discount_amount,
            item_amount, created_at, updated_at, is_deleted
        )
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        item_rows,
        page_size=500,
    )

    execute_batch(
        cur,
        """
        insert into payments (
            order_id, payment_method, payment_status, payment_amount, payment_at,
            transaction_id, created_at, updated_at, is_deleted
        )
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        payment_rows,
        page_size=500,
    )

    if shipment_rows:
        execute_batch(
            cur,
            """
            insert into shipments (
                order_id, shipment_status, carrier, tracking_number, shipped_at,
                delivered_at, created_at, updated_at, is_deleted
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            shipment_rows,
            page_size=500,
        )

    execute_batch(
        cur,
        """
        insert into order_status_history (
            order_id, previous_status, new_status, changed_at, change_reason,
            created_at, updated_at, is_deleted
        )
        values (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        history_rows,
        page_size=500,
    )


def payment_status_for_order(order_status: str) -> str:
    if order_status in ["paid", "shipped", "delivered"]:
        return "paid"
    if order_status == "refunded":
        return "refunded"
    if order_status == "cancelled":
        return random.choice(["cancelled", "failed"])
    return random.choice(["authorized", "failed"])


def build_status_history(order_id: int, final_status: str, order_at: datetime, max_updated_at: datetime):
    paths = {
        "created": ["created"],
        "paid": ["created", "paid"],
        "shipped": ["created", "paid", "shipped"],
        "delivered": ["created", "paid", "shipped", "delivered"],
        "cancelled": ["created", "cancelled"],
        "refunded": ["created", "paid", "refunded"],
    }
    rows = []
    previous = None
    for offset, status in enumerate(paths[final_status]):
        changed_at = cap_datetime(order_at + timedelta(hours=offset * random.randint(1, 12)), max_updated_at)
        rows.append((order_id, previous, status, changed_at, f"status changed to {status}", changed_at, changed_at, False))
        previous = status
    return rows


def seed_inventory_snapshots(cur, snapshot_days: int, end_date: date):
    cur.execute(
        """
        select p.product_id, p.category_id
        from products p
        where p.product_status <> 'discontinued'
        order by p.product_id
        """
    )
    products = cur.fetchall()
    rows = []
    for day_offset in range(snapshot_days):
        snapshot_date = end_date - timedelta(days=snapshot_days - day_offset - 1)
        for product_id, category_id in products:
            stock_quantity = random.randint(0, 500)
            reserved_quantity = random.randint(0, min(stock_quantity, 30))
            available_quantity = stock_quantity - reserved_quantity
            ts = datetime.combine(snapshot_date, time(hour=23, minute=55))
            rows.append(
                (
                    snapshot_date,
                    product_id,
                    category_id,
                    stock_quantity,
                    reserved_quantity,
                    available_quantity,
                    ts,
                    ts,
                    False,
                )
            )

    execute_batch(
        cur,
        """
        insert into inventory_snapshots (
            snapshot_date, product_id, category_id, stock_quantity,
            reserved_quantity, available_quantity, created_at, updated_at, is_deleted
        )
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        on conflict (snapshot_date, product_id) do update set
            category_id = excluded.category_id,
            stock_quantity = excluded.stock_quantity,
            reserved_quantity = excluded.reserved_quantity,
            available_quantity = excluded.available_quantity,
            updated_at = excluded.updated_at,
            is_deleted = excluded.is_deleted
        """,
        rows,
        page_size=1000,
    )


def apply_incremental_changes(cur, fake: Faker, changed_at: datetime):
    cur.execute("select customer_id from customers order by random() limit 20")
    customer_ids = [row[0] for row in cur.fetchall()]
    for customer_id in customer_ids:
        cur.execute(
            """
            update customers
            set address = %s,
                customer_grade = %s,
                marketing_opt_in = %s,
                updated_at = %s
            where customer_id = %s
            """,
            (
                fake.address().replace("\n", " "),
                random.choice(CUSTOMER_GRADES),
                random.random() < 0.7,
                changed_at,
                customer_id,
            ),
        )

    cur.execute("select product_id from products order by random() limit 15")
    product_ids = [row[0] for row in cur.fetchall()]
    for product_id in product_ids:
        cur.execute(
            """
            update products
            set list_price = round((list_price * %s)::numeric, 2),
                product_status = %s,
                updated_at = %s
            where product_id = %s
            """,
            (
                Decimal(str(random.uniform(0.9, 1.15))).quantize(Decimal("0.0001")),
                random.choices(PRODUCT_STATUSES, weights=[90, 8, 2], k=1)[0],
                changed_at,
                product_id,
            ),
        )

    cur.execute(
        """
        update orders
        set order_status = 'delivered',
            updated_at = %s
        where order_id in (
            select order_id
            from orders
            where order_status = 'shipped'
            order by random()
            limit 10
        )
        """,
        (changed_at,),
    )


def print_counts(cur):
    tables = [
        "categories",
        "customers",
        "products",
        "coupons",
        "orders",
        "order_items",
        "payments",
        "shipments",
        "inventory_snapshots",
        "order_status_history",
    ]
    for table in tables:
        cur.execute(f"select count(*) from {table}")
        print(f"{table}: {cur.fetchone()[0]}")


def parse_args():
    parser = argparse.ArgumentParser(description="Seed ecommerce source data into Postgres.")
    parser.add_argument("--customers", type=int, default=500)
    parser.add_argument("--products", type=int, default=120)
    parser.add_argument("--orders", type=int, default=1500)
    parser.add_argument("--coupons", type=int, default=30)
    parser.add_argument("--snapshot-days", type=int, default=30)
    parser.add_argument("--start-date", type=str, default="2025-01-01")
    parser.add_argument("--end-date", type=str, default=None)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--reset", action="store_true", help="Truncate source tables before seeding.")
    parser.add_argument(
        "--with-incremental-changes",
        action="store_true",
        help="Apply additional updates with a recent updated_at value for incremental load tests.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    load_dependencies()

    random.seed(args.seed)
    fake = Faker("ko_KR")
    Faker.seed(args.seed)

    start_date = date.fromisoformat(args.start_date)
    end_date = date.fromisoformat(args.end_date) if args.end_date else date.today()
    if start_date > end_date:
        raise ValueError("start_date must be earlier than or equal to end_date")

    base_ts = datetime.combine(start_date, time(hour=9))
    changed_at = datetime.combine(end_date, time(hour=23, minute=30))
    max_updated_at = datetime.combine(end_date, time(hour=23, minute=59, second=59))

    with connect() as conn:
        with conn.cursor() as cur:
            if args.reset:
                reset_tables(cur)
            seed_categories(cur, fake, base_ts)
            seed_customers(cur, fake, args.customers, start_date, end_date, max_updated_at)
            seed_products(cur, fake, args.products, base_ts)
            seed_coupons(cur, fake, args.coupons, base_ts)
            seed_orders(cur, fake, args.orders, start_date, end_date, max_updated_at)
            seed_inventory_snapshots(cur, args.snapshot_days, end_date)
            if args.with_incremental_changes:
                apply_incremental_changes(cur, fake, changed_at)
            print_counts(cur)


if __name__ == "__main__":
    main()

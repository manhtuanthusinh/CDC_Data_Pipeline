-- ClickHouse
USE inventory_dwh;

CREATE TABLE IF NOT EXISTS orders_warehouse (
    id Int32,
    customer_id Int32,
    product_name String,
    price Decimal(10, 2),
    status String,
    created_at DateTime,
    inserted_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (id, created_at);
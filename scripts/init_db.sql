-- MySQL
CREATE TABLE IF NOT EXISTS inventory.orders (
  id INT AUTO_INCREMENT PRIMARY KEY,
  customer_id INT,
  product_name VARCHAR(255),
  price DECIMAL(10,2),
  status VARCHAR(50),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ClickHouse
CREATE TABLE IF NOT EXISTS inventory_dwh.orders_warehouse (
    id Int32,
    customer_id Int32,
    product_name String,
    price Decimal(10, 2),
    status String,
    created_at DateTime,
    inserted_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (id, created_at);
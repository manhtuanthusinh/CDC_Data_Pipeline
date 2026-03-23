-- MySQL
CREATE TABLE IF NOT EXISTS inventory.orders (
  id INT AUTO_INCREMENT PRIMARY KEY,
  customer_id INT,
  product_name VARCHAR(255),
  price DECIMAL(10,2),
  status VARCHAR(50),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
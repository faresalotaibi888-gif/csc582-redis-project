-- ============================================================
-- CSC582: Data Warehouse and Mining Systems
-- E-Commerce Relational Database Schema
-- ============================================================

-- Drop tables if they exist (for clean recreation)
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS customers;

-- ============================================================
-- Table 1: CUSTOMERS
-- Stores customer information
-- ============================================================
CREATE TABLE customers (
    customer_id     INT PRIMARY KEY,
    first_name      VARCHAR(50) NOT NULL,
    last_name       VARCHAR(50) NOT NULL,
    email           VARCHAR(100) UNIQUE NOT NULL,
    phone           VARCHAR(20),
    city            VARCHAR(50),
    country         VARCHAR(50),
    created_at      DATE
);

-- ============================================================
-- Table 2: PRODUCTS
-- Stores product catalog information
-- ============================================================
CREATE TABLE products (
    product_id      INT PRIMARY KEY,
    product_name    VARCHAR(100) NOT NULL,
    category        VARCHAR(50),
    price           DECIMAL(10, 2) NOT NULL,
    stock_quantity  INT DEFAULT 0,
    description     TEXT
);

-- ============================================================
-- Table 3: ORDERS
-- Stores order header information with foreign key to customers
-- ============================================================
CREATE TABLE orders (
    order_id        INT PRIMARY KEY,
    customer_id     INT NOT NULL,
    order_date      DATE NOT NULL,
    status          VARCHAR(20) DEFAULT 'pending',
    total_amount    DECIMAL(10, 2),
    shipping_address VARCHAR(200),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- ============================================================
-- Sample Data: CUSTOMERS
-- ============================================================
INSERT INTO customers (customer_id, first_name, last_name, email, phone, city, country, created_at) VALUES
(1, 'Ahmed', 'Al-Rashid', 'ahmed.rashid@email.com', '+966501234567', 'Riyadh', 'Saudi Arabia', '2024-01-15'),
(2, 'Fatima', 'Hassan', 'fatima.hassan@email.com', '+966502345678', 'Jeddah', 'Saudi Arabia', '2024-02-20'),
(3, 'Mohammed', 'Al-Saud', 'mohammed.saud@email.com', '+966503456789', 'Dammam', 'Saudi Arabia', '2024-03-10'),
(4, 'Sara', 'Abdullah', 'sara.abdullah@email.com', '+966504567890', 'Riyadh', 'Saudi Arabia', '2024-04-05'),
(5, 'Khalid', 'Omar', 'khalid.omar@email.com', '+966505678901', 'Mecca', 'Saudi Arabia', '2024-05-12');

-- ============================================================
-- Sample Data: PRODUCTS
-- ============================================================
INSERT INTO products (product_id, product_name, category, price, stock_quantity, description) VALUES
(101, 'Laptop Pro 15', 'Electronics', 4500.00, 25, 'High-performance laptop with 16GB RAM'),
(102, 'Wireless Mouse', 'Electronics', 150.00, 100, 'Ergonomic wireless mouse'),
(103, 'Office Chair', 'Furniture', 850.00, 30, 'Ergonomic office chair with lumbar support'),
(104, 'Standing Desk', 'Furniture', 2200.00, 15, 'Adjustable height standing desk'),
(105, 'Noise-Canceling Headphones', 'Electronics', 1200.00, 50, 'Premium wireless headphones'),
(106, 'USB-C Hub', 'Electronics', 280.00, 75, '7-in-1 USB-C hub with HDMI');

-- ============================================================
-- Sample Data: ORDERS
-- ============================================================
INSERT INTO orders (order_id, customer_id, order_date, status, total_amount, shipping_address) VALUES
(1001, 1, '2024-06-01', 'delivered', 4650.00, '123 King Fahd Road, Riyadh'),
(1002, 2, '2024-06-05', 'delivered', 3050.00, '456 Tahlia Street, Jeddah'),
(1003, 1, '2024-06-10', 'shipped', 1200.00, '123 King Fahd Road, Riyadh'),
(1004, 3, '2024-06-15', 'processing', 2480.00, '789 Corniche Road, Dammam'),
(1005, 4, '2024-06-20', 'pending', 850.00, '321 Olaya Street, Riyadh'),
(1006, 5, '2024-06-25', 'delivered', 4780.00, '654 Haram Road, Mecca'),
(1007, 2, '2024-07-01', 'shipped', 430.00, '456 Tahlia Street, Jeddah');

-- ============================================================
-- Verification Queries
-- ============================================================
SELECT 'Customers Table:' AS info;
SELECT * FROM customers;

SELECT 'Products Table:' AS info;
SELECT * FROM products;

SELECT 'Orders Table:' AS info;
SELECT * FROM orders;

-- Join query to show orders with customer names
SELECT 'Orders with Customer Details:' AS info;
SELECT 
    o.order_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    o.order_date,
    o.status,
    o.total_amount
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id;

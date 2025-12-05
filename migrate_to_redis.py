#!/usr/bin/env python3
"""
============================================================
CSC582: Data Warehouse and Mining Systems
Relational to Key-Value Database Migration Script
============================================================

This script demonstrates:
1. Creating a relational database (SQLite)
2. Mapping relational schema to Redis key-value model
3. Migrating data to Redis Cluster
4. Performing key-value operations

Key-Value Mapping Strategy:
---------------------------
Table: customers
  Key Pattern: customer:{id}
  Value Type: HASH
  Example: HSET customer:1 first_name "Ahmed" last_name "Al-Rashid" ...

Table: products  
  Key Pattern: product:{id}
  Value Type: HASH
  Example: HSET product:101 product_name "Laptop Pro 15" price "4500.00" ...

Table: orders
  Key Pattern: order:{id}
  Value Type: HASH
  Example: HSET order:1001 customer_id "1" order_date "2024-06-01" ...

Secondary Indexes (for queries):
  - customer:email:{email} -> customer_id (for email lookups)
  - product:category:{category} -> SET of product_ids
  - customer:{id}:orders -> SET of order_ids
  - orders:status:{status} -> SET of order_ids
"""

import sqlite3
import json
from datetime import datetime
from typing import Dict, List, Any

# ============================================================
# PART 1: RELATIONAL DATABASE SETUP (SQLite)
# ============================================================

def create_relational_database(db_path: str = "ecommerce.db") -> sqlite3.Connection:
    """Create and populate the relational database."""
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create tables
    cursor.executescript("""
        DROP TABLE IF EXISTS orders;
        DROP TABLE IF EXISTS products;
        DROP TABLE IF EXISTS customers;
        
        CREATE TABLE customers (
            customer_id     INTEGER PRIMARY KEY,
            first_name      TEXT NOT NULL,
            last_name       TEXT NOT NULL,
            email           TEXT UNIQUE NOT NULL,
            phone           TEXT,
            city            TEXT,
            country         TEXT,
            created_at      TEXT
        );
        
        CREATE TABLE products (
            product_id      INTEGER PRIMARY KEY,
            product_name    TEXT NOT NULL,
            category        TEXT,
            price           REAL NOT NULL,
            stock_quantity  INTEGER DEFAULT 0,
            description     TEXT
        );
        
        CREATE TABLE orders (
            order_id        INTEGER PRIMARY KEY,
            customer_id     INTEGER NOT NULL,
            order_date      TEXT NOT NULL,
            status          TEXT DEFAULT 'pending',
            total_amount    REAL,
            shipping_address TEXT,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        );
    """)
    
    # Insert sample data
    customers_data = [
        (1, 'Ahmed', 'Al-Rashid', 'ahmed.rashid@email.com', '+966501234567', 'Riyadh', 'Saudi Arabia', '2024-01-15'),
        (2, 'Fatima', 'Hassan', 'fatima.hassan@email.com', '+966502345678', 'Jeddah', 'Saudi Arabia', '2024-02-20'),
        (3, 'Mohammed', 'Al-Saud', 'mohammed.saud@email.com', '+966503456789', 'Dammam', 'Saudi Arabia', '2024-03-10'),
        (4, 'Sara', 'Abdullah', 'sara.abdullah@email.com', '+966504567890', 'Riyadh', 'Saudi Arabia', '2024-04-05'),
        (5, 'Khalid', 'Omar', 'khalid.omar@email.com', '+966505678901', 'Mecca', 'Saudi Arabia', '2024-05-12'),
    ]
    cursor.executemany("INSERT INTO customers VALUES (?,?,?,?,?,?,?,?)", customers_data)
    
    products_data = [
        (101, 'Laptop Pro 15', 'Electronics', 4500.00, 25, 'High-performance laptop with 16GB RAM'),
        (102, 'Wireless Mouse', 'Electronics', 150.00, 100, 'Ergonomic wireless mouse'),
        (103, 'Office Chair', 'Furniture', 850.00, 30, 'Ergonomic office chair with lumbar support'),
        (104, 'Standing Desk', 'Furniture', 2200.00, 15, 'Adjustable height standing desk'),
        (105, 'Noise-Canceling Headphones', 'Electronics', 1200.00, 50, 'Premium wireless headphones'),
        (106, 'USB-C Hub', 'Electronics', 280.00, 75, '7-in-1 USB-C hub with HDMI'),
    ]
    cursor.executemany("INSERT INTO products VALUES (?,?,?,?,?,?)", products_data)
    
    orders_data = [
        (1001, 1, '2024-06-01', 'delivered', 4650.00, '123 King Fahd Road, Riyadh'),
        (1002, 2, '2024-06-05', 'delivered', 3050.00, '456 Tahlia Street, Jeddah'),
        (1003, 1, '2024-06-10', 'shipped', 1200.00, '123 King Fahd Road, Riyadh'),
        (1004, 3, '2024-06-15', 'processing', 2480.00, '789 Corniche Road, Dammam'),
        (1005, 4, '2024-06-20', 'pending', 850.00, '321 Olaya Street, Riyadh'),
        (1006, 5, '2024-06-25', 'delivered', 4780.00, '654 Haram Road, Mecca'),
        (1007, 2, '2024-07-01', 'shipped', 430.00, '456 Tahlia Street, Jeddah'),
    ]
    cursor.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?)", orders_data)
    
    conn.commit()
    return conn


def display_relational_data(conn: sqlite3.Connection):
    """Display data from relational database."""
    cursor = conn.cursor()
    
    print("\n" + "="*60)
    print("RELATIONAL DATABASE CONTENT")
    print("="*60)
    
    print("\n--- CUSTOMERS TABLE ---")
    cursor.execute("SELECT * FROM customers")
    for row in cursor.fetchall():
        print(f"  ID:{row[0]} | {row[1]} {row[2]} | {row[3]} | {row[5]}")
    
    print("\n--- PRODUCTS TABLE ---")
    cursor.execute("SELECT * FROM products")
    for row in cursor.fetchall():
        print(f"  ID:{row[0]} | {row[1]} | {row[2]} | ${row[3]}")
    
    print("\n--- ORDERS TABLE ---")
    cursor.execute("SELECT * FROM orders")
    for row in cursor.fetchall():
        print(f"  Order:{row[0]} | Customer:{row[1]} | {row[2]} | {row[3]} | ${row[4]}")


# ============================================================
# PART 2: KEY-VALUE MAPPING STRATEGY
# ============================================================

class KeyValueMapper:
    """
    Maps relational data to Redis key-value structure.
    
    Mapping Strategy:
    -----------------
    1. Each table row becomes a Redis HASH
    2. Primary key becomes part of the Redis key
    3. Secondary indexes are created for common queries
    4. Foreign key relationships are maintained via SETs
    
    Key Patterns:
    -------------
    - customer:{id} -> HASH (all customer fields)
    - product:{id} -> HASH (all product fields)
    - order:{id} -> HASH (all order fields)
    
    Secondary Indexes:
    ------------------
    - idx:customer:email:{email} -> customer_id
    - idx:product:category:{category} -> SET of product_ids
    - idx:customer:{id}:orders -> SET of order_ids
    - idx:orders:status:{status} -> SET of order_ids
    """
    
    @staticmethod
    def map_customer(row: tuple) -> Dict[str, Any]:
        """Map customer row to Redis key-value structure."""
        customer_id = row[0]
        key = f"customer:{customer_id}"
        value = {
            "customer_id": str(row[0]),
            "first_name": row[1],
            "last_name": row[2],
            "email": row[3],
            "phone": row[4] or "",
            "city": row[5] or "",
            "country": row[6] or "",
            "created_at": row[7] or ""
        }
        
        # Secondary index for email lookup
        email_index_key = f"idx:customer:email:{row[3]}"
        
        return {
            "main_key": key,
            "main_value": value,
            "indexes": [
                {"key": email_index_key, "value": str(customer_id)}
            ]
        }
    
    @staticmethod
    def map_product(row: tuple) -> Dict[str, Any]:
        """Map product row to Redis key-value structure."""
        product_id = row[0]
        category = row[2] or "uncategorized"
        key = f"product:{product_id}"
        value = {
            "product_id": str(row[0]),
            "product_name": row[1],
            "category": category,
            "price": str(row[3]),
            "stock_quantity": str(row[4]),
            "description": row[5] or ""
        }
        
        # Secondary index for category lookup
        category_index_key = f"idx:product:category:{category.lower()}"
        
        return {
            "main_key": key,
            "main_value": value,
            "indexes": [
                {"key": category_index_key, "value": str(product_id), "type": "SET"}
            ]
        }
    
    @staticmethod
    def map_order(row: tuple) -> Dict[str, Any]:
        """Map order row to Redis key-value structure."""
        order_id = row[0]
        customer_id = row[1]
        status = row[3] or "pending"
        key = f"order:{order_id}"
        value = {
            "order_id": str(row[0]),
            "customer_id": str(row[1]),
            "order_date": row[2],
            "status": status,
            "total_amount": str(row[4]),
            "shipping_address": row[5] or ""
        }
        
        # Secondary indexes
        customer_orders_key = f"idx:customer:{customer_id}:orders"
        status_index_key = f"idx:orders:status:{status}"
        
        return {
            "main_key": key,
            "main_value": value,
            "indexes": [
                {"key": customer_orders_key, "value": str(order_id), "type": "SET"},
                {"key": status_index_key, "value": str(order_id), "type": "SET"}
            ]
        }


# ============================================================
# PART 3: REDIS OPERATIONS (Simulated for demonstration)
# ============================================================

class RedisClusterSimulator:
    """
    Simulates Redis Cluster operations for demonstration.
    In production, replace with redis-py-cluster.
    
    Hash-Based Partitioning:
    ------------------------
    Redis Cluster uses CRC16 hash function to determine slot:
    slot = CRC16(key) mod 16384
    
    With 4 master nodes:
    - Master 0 (port 7000): slots 0-4095
    - Master 1 (port 7001): slots 4096-8191
    - Master 2 (port 7002): slots 8192-12287
    - Master 3 (port 7003): slots 12288-16383
    """
    
    def __init__(self):
        self.data = {}  # Simulated storage
        self.sets = {}  # Simulated SET storage
        self.slots = {}  # Track which slot each key belongs to
        self.node_assignment = {}  # Track which node handles each key
        
    def _crc16(self, key: str) -> int:
        """Calculate CRC16 hash (simplified simulation)."""
        # Simplified hash for demonstration
        hash_val = 0
        for char in key:
            hash_val = ((hash_val << 5) + hash_val) + ord(char)
        return hash_val & 0xFFFF
    
    def _get_slot(self, key: str) -> int:
        """Get the hash slot for a key."""
        # Handle hash tags: {tag}key -> use only 'tag' for hashing
        if '{' in key and '}' in key:
            start = key.index('{') + 1
            end = key.index('}')
            key = key[start:end]
        return self._crc16(key) % 16384
    
    def _get_node(self, slot: int) -> str:
        """Determine which node handles a given slot."""
        if slot < 4096:
            return "Master-0 (port 7000)"
        elif slot < 8192:
            return "Master-1 (port 7001)"
        elif slot < 12288:
            return "Master-2 (port 7002)"
        else:
            return "Master-3 (port 7003)"
    
    def hset(self, key: str, mapping: Dict[str, str]) -> bool:
        """HSET - Set hash fields."""
        slot = self._get_slot(key)
        node = self._get_node(slot)
        
        self.data[key] = mapping
        self.slots[key] = slot
        self.node_assignment[key] = node
        
        print(f"  HSET {key}")
        print(f"    -> Slot: {slot}")
        print(f"    -> Node: {node}")
        return True
    
    def hget(self, key: str, field: str) -> str:
        """HGET - Get a hash field."""
        if key in self.data and field in self.data[key]:
            return self.data[key][field]
        return None
    
    def hgetall(self, key: str) -> Dict[str, str]:
        """HGETALL - Get all fields of a hash."""
        return self.data.get(key, {})
    
    def set(self, key: str, value: str) -> bool:
        """SET - Set a string value."""
        slot = self._get_slot(key)
        node = self._get_node(slot)
        
        self.data[key] = value
        self.slots[key] = slot
        self.node_assignment[key] = node
        return True
    
    def get(self, key: str) -> str:
        """GET - Get a string value."""
        return self.data.get(key)
    
    def sadd(self, key: str, *members) -> int:
        """SADD - Add members to a set."""
        slot = self._get_slot(key)
        node = self._get_node(slot)
        
        if key not in self.sets:
            self.sets[key] = set()
            self.slots[key] = slot
            self.node_assignment[key] = node
        
        added = 0
        for member in members:
            if member not in self.sets[key]:
                self.sets[key].add(member)
                added += 1
        return added
    
    def smembers(self, key: str) -> set:
        """SMEMBERS - Get all members of a set."""
        return self.sets.get(key, set())
    
    def keys(self, pattern: str = "*") -> List[str]:
        """KEYS - Find keys matching pattern (simplified)."""
        if pattern == "*":
            return list(self.data.keys()) + list(self.sets.keys())
        # Simple prefix matching
        prefix = pattern.replace("*", "")
        result = []
        for key in list(self.data.keys()) + list(self.sets.keys()):
            if key.startswith(prefix):
                result.append(key)
        return result
    
    def cluster_info(self):
        """Display cluster information."""
        print("\n" + "="*60)
        print("REDIS CLUSTER INFORMATION")
        print("="*60)
        print("\nCluster Configuration:")
        print("  Total Hash Slots: 16384")
        print("  Number of Masters: 4")
        print("  Number of Replicas: 4")
        print("\nSlot Distribution:")
        print("  Master-0 (7000): slots 0-4095     | Replica: 7004")
        print("  Master-1 (7001): slots 4096-8191  | Replica: 7005")
        print("  Master-2 (7002): slots 8192-12287 | Replica: 7006")
        print("  Master-3 (7003): slots 12288-16383| Replica: 7007")
        
        # Count keys per node
        node_counts = {}
        for key, node in self.node_assignment.items():
            node_counts[node] = node_counts.get(node, 0) + 1
        
        print("\nKeys per Node:")
        for node, count in sorted(node_counts.items()):
            print(f"  {node}: {count} keys")


# ============================================================
# PART 4: MIGRATION PROCESS
# ============================================================

def migrate_to_redis(conn: sqlite3.Connection, redis: RedisClusterSimulator):
    """Migrate all data from SQLite to Redis."""
    cursor = conn.cursor()
    mapper = KeyValueMapper()
    
    print("\n" + "="*60)
    print("MIGRATING DATA TO REDIS")
    print("="*60)
    
    # Migrate Customers
    print("\n--- Migrating CUSTOMERS ---")
    cursor.execute("SELECT * FROM customers")
    for row in cursor.fetchall():
        mapped = mapper.map_customer(row)
        redis.hset(mapped["main_key"], mapped["main_value"])
        for idx in mapped["indexes"]:
            redis.set(idx["key"], idx["value"])
    
    # Migrate Products
    print("\n--- Migrating PRODUCTS ---")
    cursor.execute("SELECT * FROM products")
    for row in cursor.fetchall():
        mapped = mapper.map_product(row)
        redis.hset(mapped["main_key"], mapped["main_value"])
        for idx in mapped["indexes"]:
            if idx.get("type") == "SET":
                redis.sadd(idx["key"], idx["value"])
            else:
                redis.set(idx["key"], idx["value"])
    
    # Migrate Orders
    print("\n--- Migrating ORDERS ---")
    cursor.execute("SELECT * FROM orders")
    for row in cursor.fetchall():
        mapped = mapper.map_order(row)
        redis.hset(mapped["main_key"], mapped["main_value"])
        for idx in mapped["indexes"]:
            if idx.get("type") == "SET":
                redis.sadd(idx["key"], idx["value"])
            else:
                redis.set(idx["key"], idx["value"])


# ============================================================
# PART 5: KEY-VALUE QUERIES AND OPERATIONS
# ============================================================

def demonstrate_operations(redis: RedisClusterSimulator):
    """Demonstrate various Redis operations."""
    
    print("\n" + "="*60)
    print("KEY-VALUE OPERATIONS DEMONSTRATION")
    print("="*60)
    
    # Operation 1: Get customer by ID
    print("\n--- Operation 1: Get Customer by ID ---")
    print("Command: HGETALL customer:1")
    customer = redis.hgetall("customer:1")
    print(f"Result: {json.dumps(customer, indent=2)}")
    
    # Operation 2: Get specific field
    print("\n--- Operation 2: Get Specific Field ---")
    print("Command: HGET customer:2 email")
    email = redis.hget("customer:2", "email")
    print(f"Result: {email}")
    
    # Operation 3: Get product by ID
    print("\n--- Operation 3: Get Product Details ---")
    print("Command: HGETALL product:101")
    product = redis.hgetall("product:101")
    print(f"Result: {json.dumps(product, indent=2)}")
    
    # Operation 4: Get products by category
    print("\n--- Operation 4: Get Products by Category ---")
    print("Command: SMEMBERS idx:product:category:electronics")
    product_ids = redis.smembers("idx:product:category:electronics")
    print(f"Electronics Product IDs: {product_ids}")
    
    # Operation 5: Get customer's orders
    print("\n--- Operation 5: Get Customer's Orders ---")
    print("Command: SMEMBERS idx:customer:1:orders")
    order_ids = redis.smembers("idx:customer:1:orders")
    print(f"Customer 1's Order IDs: {order_ids}")
    
    # Operation 6: Get orders by status
    print("\n--- Operation 6: Get Orders by Status ---")
    print("Command: SMEMBERS idx:orders:status:delivered")
    delivered_orders = redis.smembers("idx:orders:status:delivered")
    print(f"Delivered Order IDs: {delivered_orders}")
    
    # Operation 7: SET and GET simple values
    print("\n--- Operation 7: SET/GET Operations ---")
    print("Command: SET session:user1 'active'")
    redis.set("session:user1", "active")
    print("Command: GET session:user1")
    session = redis.get("session:user1")
    print(f"Result: {session}")
    
    # Operation 8: List all customer keys
    print("\n--- Operation 8: Find Keys by Pattern ---")
    print("Command: KEYS customer:*")
    customer_keys = redis.keys("customer:")
    print(f"Customer Keys: {customer_keys}")


# ============================================================
# MAIN EXECUTION
# ============================================================

def main():
    print("="*60)
    print("CSC582: Relational to Key-Value Migration")
    print("E-Commerce Database -> Redis Cluster")
    print("="*60)
    
    # Step 1: Create relational database
    print("\n[Step 1] Creating Relational Database...")
    conn = create_relational_database()
    display_relational_data(conn)
    
    # Step 2: Initialize Redis Cluster (simulated)
    print("\n[Step 2] Initializing Redis Cluster...")
    redis = RedisClusterSimulator()
    
    # Step 3: Migrate data
    print("\n[Step 3] Starting Migration...")
    migrate_to_redis(conn, redis)
    
    # Step 4: Display cluster info
    redis.cluster_info()
    
    # Step 5: Demonstrate operations
    demonstrate_operations(redis)
    
    # Cleanup
    conn.close()
    
    print("\n" + "="*60)
    print("MIGRATION COMPLETE!")
    print("="*60)


if __name__ == "__main__":
    main()

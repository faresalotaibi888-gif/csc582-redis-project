#!/usr/bin/env python3
"""
============================================================
CSC582: Data Warehouse and Mining Systems
Relational to Key-Value Database Migration Script
============================================================

Key-Value Mapping Strategy:
---------------------------
Format: TableName:TupleID:Attribute → Value

Examples:
  Customer:1:first_name → "Ahmed"
  Customer:1:email → "ahmed.rashid@email.com"
  Product:101:product_name → "Laptop Pro 15"
  Product:101:price → "4500.0"
  Order:1001:status → "delivered"
"""

import sqlite3
from typing import Dict, List, Any

# ============================================================
# PART 1: RELATIONAL DATABASE SETUP (SQLite)
# ============================================================

def create_relational_database(db_path: str = ":memory:") -> sqlite3.Connection:
    """Create and populate the relational database."""
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create tables
    cursor.executescript("""
        DROP TABLE IF EXISTS Order_;
        DROP TABLE IF EXISTS Product;
        DROP TABLE IF EXISTS Customer;
        
        CREATE TABLE Customer (
            customer_id     INTEGER PRIMARY KEY,
            first_name      TEXT NOT NULL,
            last_name       TEXT NOT NULL,
            email           TEXT UNIQUE NOT NULL,
            phone           TEXT,
            city            TEXT,
            country         TEXT
        );
        
        CREATE TABLE Product (
            product_id      INTEGER PRIMARY KEY,
            product_name    TEXT NOT NULL,
            category        TEXT,
            price           REAL NOT NULL,
            stock_quantity  INTEGER DEFAULT 0
        );
        
        CREATE TABLE Order_ (
            order_id        INTEGER PRIMARY KEY,
            customer_id     INTEGER NOT NULL,
            order_date      TEXT NOT NULL,
            status          TEXT DEFAULT 'pending',
            total_amount    REAL,
            FOREIGN KEY (customer_id) REFERENCES Customer(customer_id)
        );
    """)
    
    # Insert Customer data
    customers_data = [
        (1, 'Ahmed', 'Al-Rashid', 'ahmed.rashid@email.com', '+966501234567', 'Riyadh', 'Saudi Arabia'),
        (2, 'Fatima', 'Hassan', 'fatima.hassan@email.com', '+966502345678', 'Jeddah', 'Saudi Arabia'),
        (3, 'Mohammed', 'Al-Saud', 'mohammed.saud@email.com', '+966503456789', 'Dammam', 'Saudi Arabia'),
        (4, 'Sara', 'Abdullah', 'sara.abdullah@email.com', '+966504567890', 'Riyadh', 'Saudi Arabia'),
        (5, 'Khalid', 'Omar', 'khalid.omar@email.com', '+966505678901', 'Mecca', 'Saudi Arabia'),
    ]
    cursor.executemany("INSERT INTO Customer VALUES (?,?,?,?,?,?,?)", customers_data)
    
    # Insert Product data
    products_data = [
        (101, 'Laptop Pro 15', 'Electronics', 4500.00, 25),
        (102, 'Wireless Mouse', 'Electronics', 150.00, 100),
        (103, 'Office Chair', 'Furniture', 850.00, 30),
        (104, 'Standing Desk', 'Furniture', 2200.00, 15),
        (105, 'Noise-Canceling Headphones', 'Electronics', 1200.00, 50),
        (106, 'USB-C Hub', 'Electronics', 280.00, 75),
    ]
    cursor.executemany("INSERT INTO Product VALUES (?,?,?,?,?)", products_data)
    
    # Insert Order data
    orders_data = [
        (1001, 1, '2024-06-01', 'delivered', 4650.00),
        (1002, 2, '2024-06-05', 'delivered', 3050.00),
        (1003, 1, '2024-06-10', 'shipped', 1200.00),
        (1004, 3, '2024-06-15', 'processing', 2480.00),
        (1005, 4, '2024-06-20', 'pending', 850.00),
        (1006, 5, '2024-06-25', 'delivered', 4780.00),
        (1007, 2, '2024-07-01', 'shipped', 430.00),
    ]
    cursor.executemany("INSERT INTO Order_ VALUES (?,?,?,?,?)", orders_data)
    
    conn.commit()
    return conn


def display_relational_data(conn: sqlite3.Connection):
    """Display data from relational database."""
    cursor = conn.cursor()
    
    print("\n" + "="*70)
    print("RELATIONAL DATABASE CONTENT")
    print("="*70)
    
    print("\n--- CUSTOMER TABLE ---")
    print(f"{'ID':<4} {'First Name':<12} {'Last Name':<12} {'Email':<28} {'City':<10}")
    print("-"*70)
    cursor.execute("SELECT * FROM Customer")
    for row in cursor.fetchall():
        print(f"{row[0]:<4} {row[1]:<12} {row[2]:<12} {row[3]:<28} {row[5]:<10}")
    
    print("\n--- PRODUCT TABLE ---")
    print(f"{'ID':<6} {'Product Name':<30} {'Category':<12} {'Price':<10} {'Stock':<6}")
    print("-"*70)
    cursor.execute("SELECT * FROM Product")
    for row in cursor.fetchall():
        print(f"{row[0]:<6} {row[1]:<30} {row[2]:<12} ${row[3]:<9} {row[4]:<6}")
    
    print("\n--- ORDER TABLE ---")
    print(f"{'Order ID':<10} {'Customer':<10} {'Date':<12} {'Status':<12} {'Total':<10}")
    print("-"*70)
    cursor.execute("SELECT * FROM Order_")
    for row in cursor.fetchall():
        print(f"{row[0]:<10} {row[1]:<10} {row[2]:<12} {row[3]:<12} ${row[4]:<10}")


# ============================================================
# PART 2: REDIS CLUSTER SIMULATOR
# ============================================================

class RedisClusterSimulator:
    """
    Simulates Redis Cluster operations for demonstration.
    In production, replace with redis-py.
    
    Key Format: TableName:TupleID:Attribute → Value
    
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
        self.data = {}  # Key-Value storage
        self.slots = {}  # Track which slot each key belongs to
        self.node_assignment = {}  # Track which node handles each key
        
    def _crc16(self, key: str) -> int:
        """Calculate CRC16 hash."""
        crc = 0
        for char in key.encode():
            crc = ((crc << 5) + crc + char) & 0xFFFF
        return crc
    
    def _get_slot(self, key: str) -> int:
        """Get the hash slot for a key."""
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
    
    def set(self, key: str, value: str) -> bool:
        """SET - Store a key-value pair."""
        slot = self._get_slot(key)
        node = self._get_node(slot)
        
        self.data[key] = value
        self.slots[key] = slot
        self.node_assignment[key] = node
        
        return True
    
    def get(self, key: str) -> str:
        """GET - Retrieve value by key."""
        return self.data.get(key)
    
    def keys(self, pattern: str = "*") -> List[str]:
        """KEYS - Find keys matching pattern."""
        if pattern == "*":
            return list(self.data.keys())
        
        # Simple pattern matching for prefix:* or *:suffix patterns
        if pattern.endswith("*"):
            prefix = pattern[:-1]
            return [k for k in self.data.keys() if k.startswith(prefix)]
        elif pattern.startswith("*"):
            suffix = pattern[1:]
            return [k for k in self.data.keys() if k.endswith(suffix)]
        else:
            return [k for k in self.data.keys() if pattern in k]
    
    def dbsize(self) -> int:
        """DBSIZE - Return number of keys."""
        return len(self.data)
    
    def cluster_info(self):
        """Display cluster information."""
        print("\n" + "="*70)
        print("REDIS CLUSTER INFORMATION")
        print("="*70)
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
        
        print(f"\nTotal Keys: {self.dbsize()}")
        print("\nKeys per Node:")
        for node, count in sorted(node_counts.items()):
            print(f"  {node}: {count} keys")


# ============================================================
# PART 3: DATA MIGRATION
# ============================================================

def migrate_to_redis(conn: sqlite3.Connection, redis: RedisClusterSimulator):
    """
    Migrate all data from SQLite to Redis.
    
    Key Format: TableName:TupleID:Attribute → Value
    """
    cursor = conn.cursor()
    
    print("\n" + "="*70)
    print("MIGRATING DATA TO REDIS")
    print("Key Format: TableName:TupleID:Attribute → Value")
    print("="*70)
    
    # Migrate Customers
    print("\n--- Migrating CUSTOMER Table ---")
    cursor.execute("SELECT * FROM Customer")
    for row in cursor.fetchall():
        cid = row[0]
        
        redis.set(f"Customer:{cid}:first_name", row[1])
        print(f"   SET Customer:{cid}:first_name → \"{row[1]}\"")
        
        redis.set(f"Customer:{cid}:last_name", row[2])
        print(f"   SET Customer:{cid}:last_name → \"{row[2]}\"")
        
        redis.set(f"Customer:{cid}:email", row[3])
        print(f"   SET Customer:{cid}:email → \"{row[3]}\"")
        
        redis.set(f"Customer:{cid}:phone", row[4])
        print(f"   SET Customer:{cid}:phone → \"{row[4]}\"")
        
        redis.set(f"Customer:{cid}:city", row[5])
        print(f"   SET Customer:{cid}:city → \"{row[5]}\"")
        
        redis.set(f"Customer:{cid}:country", row[6])
        print(f"   SET Customer:{cid}:country → \"{row[6]}\"")
        print()
    
    # Migrate Products
    print("\n--- Migrating PRODUCT Table ---")
    cursor.execute("SELECT * FROM Product")
    for row in cursor.fetchall():
        pid = row[0]
        
        redis.set(f"Product:{pid}:product_name", row[1])
        print(f"   SET Product:{pid}:product_name → \"{row[1]}\"")
        
        redis.set(f"Product:{pid}:category", row[2])
        print(f"   SET Product:{pid}:category → \"{row[2]}\"")
        
        redis.set(f"Product:{pid}:price", str(row[3]))
        print(f"   SET Product:{pid}:price → \"{row[3]}\"")
        
        redis.set(f"Product:{pid}:stock_quantity", str(row[4]))
        print(f"   SET Product:{pid}:stock_quantity → \"{row[4]}\"")
        print()
    
    # Migrate Orders
    print("\n--- Migrating ORDER Table ---")
    cursor.execute("SELECT * FROM Order_")
    for row in cursor.fetchall():
        oid = row[0]
        
        redis.set(f"Order:{oid}:customer_id", str(row[1]))
        print(f"   SET Order:{oid}:customer_id → \"{row[1]}\"")
        
        redis.set(f"Order:{oid}:order_date", row[2])
        print(f"   SET Order:{oid}:order_date → \"{row[2]}\"")
        
        redis.set(f"Order:{oid}:status", row[3])
        print(f"   SET Order:{oid}:status → \"{row[3]}\"")
        
        redis.set(f"Order:{oid}:total_amount", str(row[4]))
        print(f"   SET Order:{oid}:total_amount → \"{row[4]}\"")
        print()


# ============================================================
# PART 4: KEY-VALUE OPERATIONS DEMO
# ============================================================

def demonstrate_operations(redis: RedisClusterSimulator):
    """Demonstrate various Redis operations."""
    
    print("\n" + "="*70)
    print("KEY-VALUE OPERATIONS DEMONSTRATION")
    print("="*70)
    
    # GET by Key - Customer
    print("\n--- GET by Key: Customer 1 ---")
    print(f"GET Customer:1:first_name → {redis.get('Customer:1:first_name')}")
    print(f"GET Customer:1:last_name  → {redis.get('Customer:1:last_name')}")
    print(f"GET Customer:1:email      → {redis.get('Customer:1:email')}")
    print(f"GET Customer:1:phone      → {redis.get('Customer:1:phone')}")
    print(f"GET Customer:1:city       → {redis.get('Customer:1:city')}")
    print(f"GET Customer:1:country    → {redis.get('Customer:1:country')}")
    
    # GET by Key - Product
    print("\n--- GET by Key: Product 101 ---")
    print(f"GET Product:101:product_name   → {redis.get('Product:101:product_name')}")
    print(f"GET Product:101:category       → {redis.get('Product:101:category')}")
    print(f"GET Product:101:price          → {redis.get('Product:101:price')}")
    print(f"GET Product:101:stock_quantity → {redis.get('Product:101:stock_quantity')}")
    
    # GET by Key - Order
    print("\n--- GET by Key: Order 1001 ---")
    print(f"GET Order:1001:customer_id  → {redis.get('Order:1001:customer_id')}")
    print(f"GET Order:1001:order_date   → {redis.get('Order:1001:order_date')}")
    print(f"GET Order:1001:status       → {redis.get('Order:1001:status')}")
    print(f"GET Order:1001:total_amount → {redis.get('Order:1001:total_amount')}")
    
    # GET by Value (using KEYS pattern)
    print("\n--- GET by Value: Find all Customer emails ---")
    print("KEYS Customer:*:email")
    email_keys = redis.keys("Customer:*:email")
    for key in sorted(email_keys):
        print(f"   {key} → {redis.get(key)}")
    
    print("\n--- GET by Value: Find all Product prices ---")
    print("KEYS Product:*:price")
    price_keys = redis.keys("Product:*:price")
    for key in sorted(price_keys):
        name_key = key.replace(":price", ":product_name")
        print(f"   {redis.get(name_key)}: ${redis.get(key)}")
    
    print("\n--- GET by Value: Find all Order statuses ---")
    print("KEYS Order:*:status")
    status_keys = redis.keys("Order:*:status")
    for key in sorted(status_keys):
        order_id = key.split(":")[1]
        print(f"   Order {order_id}: {redis.get(key)}")
    
    # Simulating JOIN
    print("\n--- Simulating JOIN: Order 1001 with Customer Name ---")
    customer_id = redis.get("Order:1001:customer_id")
    print(f"Step 1: GET Order:1001:customer_id → {customer_id}")
    
    first_name = redis.get(f"Customer:{customer_id}:first_name")
    last_name = redis.get(f"Customer:{customer_id}:last_name")
    print(f"Step 2: GET Customer:{customer_id}:first_name → {first_name}")
    print(f"Step 3: GET Customer:{customer_id}:last_name → {last_name}")
    
    order_date = redis.get("Order:1001:order_date")
    total = redis.get("Order:1001:total_amount")
    status = redis.get("Order:1001:status")
    
    print(f"\n✅ RESULT: Order 1001")
    print(f"   Customer: {first_name} {last_name}")
    print(f"   Date: {order_date}")
    print(f"   Amount: ${total}")
    print(f"   Status: {status}")


# ============================================================
# PART 5: SHOW COMPLETE MAPPING
# ============================================================

def show_complete_mapping(redis: RedisClusterSimulator):
    """Display complete key-value mapping for all tables."""
    
    print("\n" + "="*70)
    print("COMPLETE KEY-VALUE MAPPING")
    print("Format: TableName:TupleID:Attribute → Value")
    print("="*70)
    
    # Customer mappings
    print("\n👥 CUSTOMER TABLE MAPPING")
    print("-"*70)
    for cid in range(1, 6):
        print(f"\nCustomer {cid}:")
        for attr in ['first_name', 'last_name', 'email', 'phone', 'city', 'country']:
            key = f"Customer:{cid}:{attr}"
            val = redis.get(key)
            print(f"   {key:<30} → \"{val}\"")
    
    # Product mappings
    print("\n📦 PRODUCT TABLE MAPPING")
    print("-"*70)
    for pid in [101, 102, 103, 104, 105, 106]:
        print(f"\nProduct {pid}:")
        for attr in ['product_name', 'category', 'price', 'stock_quantity']:
            key = f"Product:{pid}:{attr}"
            val = redis.get(key)
            print(f"   {key:<35} → \"{val}\"")
    
    # Order mappings
    print("\n🛒 ORDER TABLE MAPPING")
    print("-"*70)
    for oid in [1001, 1002, 1003, 1004, 1005, 1006, 1007]:
        print(f"\nOrder {oid}:")
        for attr in ['customer_id', 'order_date', 'status', 'total_amount']:
            key = f"Order:{oid}:{attr}"
            val = redis.get(key)
            print(f"   {key:<30} → \"{val}\"")


# ============================================================
# MAIN EXECUTION
# ============================================================

def main():
    print("="*70)
    print("CSC582: Relational to Key-Value Database Migration")
    print("E-Commerce Database → Redis Cluster")
    print("Mapping Format: TableName:TupleID:Attribute → Value")
    print("="*70)
    
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
    
    # Step 6: Show complete mapping
    show_complete_mapping(redis)
    
    # Cleanup
    conn.close()
    
    print("\n" + "="*70)
    print("MIGRATION COMPLETE!")
    print("="*70)


if __name__ == "__main__":
    main()

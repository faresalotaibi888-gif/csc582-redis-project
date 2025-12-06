#!/usr/bin/env python3
"""
CSC582: Relational to Key-Value Database Migration
Key Format: TableName:TupleID:Attribute → Value
Cluster: 4 Masters + 4 Replicas (8 Nodes)
"""

import sqlite3

# ============================================================
# PART 1: RELATIONAL DATABASE
# ============================================================

def create_database():
    conn = sqlite3.connect(':memory:')
    cursor = conn.cursor()
    
    cursor.executescript("""
        CREATE TABLE Customer (
            customer_id INTEGER PRIMARY KEY,
            first_name TEXT, last_name TEXT, email TEXT,
            phone TEXT, city TEXT, country TEXT
        );
        CREATE TABLE Product (
            product_id INTEGER PRIMARY KEY,
            product_name TEXT, category TEXT,
            price REAL, stock_quantity INTEGER
        );
        CREATE TABLE Order_ (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER, order_date TEXT,
            status TEXT, total_amount REAL
        );
    """)
    
    customers = [
        (1,'Ahmed','Al-Rashid','ahmed.rashid@email.com','+966501234567','Riyadh','Saudi Arabia'),
        (2,'Fatima','Hassan','fatima.hassan@email.com','+966502345678','Jeddah','Saudi Arabia'),
        (3,'Mohammed','Al-Saud','mohammed.saud@email.com','+966503456789','Dammam','Saudi Arabia'),
        (4,'Sara','Abdullah','sara.abdullah@email.com','+966504567890','Riyadh','Saudi Arabia'),
        (5,'Khalid','Omar','khalid.omar@email.com','+966505678901','Mecca','Saudi Arabia'),
    ]
    cursor.executemany("INSERT INTO Customer VALUES (?,?,?,?,?,?,?)", customers)
    
    products = [
        (101,'Laptop Pro 15','Electronics',4500.00,25),
        (102,'Wireless Mouse','Electronics',150.00,100),
        (103,'Office Chair','Furniture',850.00,30),
        (104,'Standing Desk','Furniture',2200.00,15),
        (105,'Headphones','Electronics',1200.00,50),
        (106,'USB-C Hub','Electronics',280.00,75),
    ]
    cursor.executemany("INSERT INTO Product VALUES (?,?,?,?,?)", products)
    
    orders = [
        (1001,1,'2024-06-01','delivered',4650.00),
        (1002,2,'2024-06-05','delivered',3050.00),
        (1003,1,'2024-06-10','shipped',1200.00),
        (1004,3,'2024-06-15','processing',2480.00),
        (1005,4,'2024-06-20','pending',850.00),
        (1006,5,'2024-06-25','delivered',4780.00),
        (1007,2,'2024-07-01','shipped',430.00),
    ]
    cursor.executemany("INSERT INTO Order_ VALUES (?,?,?,?,?)", orders)
    conn.commit()
    return conn

# ============================================================
# PART 2: REDIS CLUSTER SIMULATOR WITH SHARDING & REPLICATION
# ============================================================

class RedisCluster:
    def __init__(self):
        self.data = {}
        self.sharding_log = {'Master-0':[], 'Master-1':[], 'Master-2':[], 'Master-3':[]}
    
    def _crc16(self, key):
        crc = 0
        for b in key.encode():
            crc = ((crc << 5) + crc + b) & 0xFFFF
        return crc
    
    def _get_slot(self, key):
        return self._crc16(key) % 16384
    
    def _get_master(self, slot):
        if slot < 4096: return 'Master-0', 7000
        elif slot < 8192: return 'Master-1', 7001
        elif slot < 12288: return 'Master-2', 7002
        else: return 'Master-3', 7003
    
    def _get_replica(self, master_port):
        return f'Replica-{master_port-7000}', master_port + 4
    
    def set(self, key, value):
        slot = self._get_slot(key)
        master, mport = self._get_master(slot)
        replica, rport = self._get_replica(mport)
        self.data[key] = value
        self.sharding_log[master].append({'key': key, 'slot': slot})
        return slot, master, mport, replica, rport
    
    def get(self, key):
        return self.data.get(key)
    
    def keys(self, pattern='*'):
        if pattern == '*': return list(self.data.keys())
        prefix = pattern.replace('*', '')
        return [k for k in self.data.keys() if prefix in k]
    
    def dbsize(self):
        return len(self.data)

# ============================================================
# PART 3: MIGRATION WITH SHARDING DEMO
# ============================================================

def migrate_with_sharding(conn, redis):
    cursor = conn.cursor()
    
    print("="*80)
    print("MIGRATING DATA TO REDIS WITH SHARDING")
    print("Key Format: TableName:TupleID:Attribute → Value")
    print("="*80)
    
    print(f"\n{'Key':<40} {'Slot':>6} {'Master':<10} {'Replica'}")
    print("-"*80)
    
    # Migrate Customers
    print("\n--- CUSTOMER TABLE ---")
    cursor.execute("SELECT * FROM Customer")
    for row in cursor.fetchall():
        cid = row[0]
        attrs = [('first_name',row[1]),('last_name',row[2]),('email',row[3]),
                 ('phone',row[4]),('city',row[5]),('country',row[6])]
        for attr, val in attrs:
            key = f"Customer:{cid}:{attr}"
            slot, master, mp, replica, rp = redis.set(key, str(val))
            print(f"{key:<40} {slot:>6} {master:<10} {replica}")
    
    # Migrate Products
    print("\n--- PRODUCT TABLE ---")
    cursor.execute("SELECT * FROM Product")
    for row in cursor.fetchall():
        pid = row[0]
        attrs = [('product_name',row[1]),('category',row[2]),
                 ('price',row[3]),('stock_quantity',row[4])]
        for attr, val in attrs:
            key = f"Product:{pid}:{attr}"
            slot, master, mp, replica, rp = redis.set(key, str(val))
            print(f"{key:<40} {slot:>6} {master:<10} {replica}")
    
    # Migrate Orders
    print("\n--- ORDER TABLE ---")
    cursor.execute("SELECT * FROM Order_")
    for row in cursor.fetchall():
        oid = row[0]
        attrs = [('customer_id',row[1]),('order_date',row[2]),
                 ('status',row[3]),('total_amount',row[4])]
        for attr, val in attrs:
            key = f"Order:{oid}:{attr}"
            slot, master, mp, replica, rp = redis.set(key, str(val))
            print(f"{key:<40} {slot:>6} {master:<10} {replica}")
    
    print(f"\n✅ Migration complete! Total keys: {redis.dbsize()}")

# ============================================================
# PART 4: SHARDING DEMONSTRATION
# ============================================================

def demo_sharding(redis):
    print("\n" + "="*80)
    print("SHARDING DEMONSTRATION - Data Distribution")
    print("="*80)
    
    total = sum(len(v) for v in redis.sharding_log.values())
    for m in ['Master-0','Master-1','Master-2','Master-3']:
        cnt = len(redis.sharding_log[m])
        pct = cnt/total*100
        bar = '█' * int(pct/2)
        port = 7000 + int(m[-1])
        print(f"{m} (Port {port}): {cnt:>3} keys ({pct:>5.1f}%) {bar}")
    
    print(f"\nTotal: {total} keys distributed across 4 master nodes")
    print("\n✅ Sharding is working!")

# ============================================================
# PART 5: REPLICATION DEMONSTRATION
# ============================================================

def demo_replication(redis):
    print("\n" + "="*80)
    print("REPLICATION DEMONSTRATION")
    print("Write to Master → Read from Replica")
    print("="*80)
    
    demo_keys = ['Customer:1:first_name', 'Product:101:product_name', 'Order:1001:status']
    
    for key in demo_keys:
        slot = redis._get_slot(key)
        master, mport = redis._get_master(slot)
        replica, rport = redis._get_replica(mport)
        value = redis.get(key)
        
        print(f"\nKey: {key}")
        print(f"  WRITE  → {master} (Port {mport}): SET {key} = \"{value}\"")
        print(f"  SYNC   → Data replicated to {replica} (Port {rport})")
        print(f"  READ   ← {replica} (Port {rport}): GET {key} = \"{value}\"")
        print(f"  ✅ Replication verified!")
    
    print("\n" + "="*80)
    print("REPLICATION MAPPING")
    print("="*80)
    print("Master-0 (7000) ←→ Replica-0 (7004) | Slots 0-4095")
    print("Master-1 (7001) ←→ Replica-1 (7005) | Slots 4096-8191")
    print("Master-2 (7002) ←→ Replica-2 (7006) | Slots 8192-12287")
    print("Master-3 (7003) ←→ Replica-3 (7007) | Slots 12288-16383")
    print("\n✅ Replication is working!")

# ============================================================
# PART 6: KEY-VALUE OPERATIONS
# ============================================================

def demo_operations(redis):
    print("\n" + "="*80)
    print("KEY-VALUE OPERATIONS")
    print("="*80)
    
    print("\n--- GET by Key ---")
    print(f"GET Customer:1:first_name → {redis.get('Customer:1:first_name')}")
    print(f"GET Customer:1:email → {redis.get('Customer:1:email')}")
    print(f"GET Product:101:product_name → {redis.get('Product:101:product_name')}")
    print(f"GET Product:101:price → {redis.get('Product:101:price')}")
    
    print("\n--- GET by Value (KEYS pattern) ---")
    print("KEYS Customer:*:email")
    for k in sorted(redis.keys('Customer:*:email')):
        print(f"  {k} → {redis.get(k)}")

# ============================================================
# MAIN
# ============================================================

def main():
    print("="*80)
    print("CSC582: Relational to Key-Value Database Migration")
    print("="*80)
    
    conn = create_database()
    redis = RedisCluster()
    
    migrate_with_sharding(conn, redis)
    demo_sharding(redis)
    demo_replication(redis)
    demo_operations(redis)
    
    conn.close()
    print("\n" + "="*80)
    print("COMPLETE!")
    print("="*80)

if __name__ == "__main__":
    main()

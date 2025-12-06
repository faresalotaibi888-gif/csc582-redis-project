# CSC582: Relational to Key-Value Database Migration

## 📋 Project Overview

This project demonstrates the migration of a relational database to a key-value NoSQL database using **Redis Cluster**. We implement an E-Commerce database with three tables (Customer, Product, Order) and migrate it to Redis.

**Mapping Format:** `TableName:TupleID:Attribute` → Value

---

## 🗂️ Project Structure

```
csc582-redis-project/
├── CSC582_Redis_Demo_Complete.ipynb   # Interactive Colab notebook (run this for demo)
├── migrate_to_redis.py                 # Python source code
├── CSC582_Project_Report_Final.pdf     # Complete PDF report
└── README.md                           # This file
```

---

## 🔄 Mapping Strategy

Each column in a relational row becomes a **separate key-value pair** in Redis:

| Relational | Redis Key-Value |
|------------|-----------------|
| Customer row 1, column first_name | `Customer:1:first_name` → "Ahmed" |
| Product row 101, column price | `Product:101:price` → "4500.0" |
| Order row 1001, column status | `Order:1001:status` → "delivered" |

### Example Migration

```
Relational Row:
┌─────────────┬────────────┬───────────┬────────────────────────┐
│ customer_id │ first_name │ last_name │ email                  │
│      1      │   Ahmed    │ Al-Rashid │ ahmed.rashid@email.com │
└─────────────┴────────────┴───────────┴────────────────────────┘

                              ↓ Maps To ↓

Redis Key-Value Pairs:
  SET Customer:1:first_name "Ahmed"
  SET Customer:1:last_name "Al-Rashid"
  SET Customer:1:email "ahmed.rashid@email.com"
```

---

## 🔀 Sharding (Hash-Based Partitioning)

Keys are distributed across 4 master nodes using CRC16 hash:

**Formula:** `slot = CRC16(key) mod 16384`

| Key | Slot | Master Node |
|-----|------|-------------|
| Customer:1:first_name | 5855 | Master 1 (7001) |
| Customer:2:email | 11008 | Master 2 (7002) |
| Product:101:product_name | 9352 | Master 2 (7002) |
| Order:1001:status | 16118 | Master 3 (7003) |

---

## 🔄 Replication (Master-Replica)

Each master has a replica for high availability:

```
Write to MASTER (6379):
  SET Customer:99:first_name "Test"
  SET Customer:99:email "test@email.com"

Read from REPLICA (6380):
  GET Customer:99:first_name → "Test"
  GET Customer:99:email → "test@email.com"

✅ Data automatically replicated!
```

---

## 🏗️ Cluster Architecture

```
┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│  MASTER 0   │  │  MASTER 1   │  │  MASTER 2   │  │  MASTER 3   │
│  Port 7000  │  │  Port 7001  │  │  Port 7002  │  │  Port 7003  │
│Slots 0-4095 │  │Slots 4096-  │  │Slots 8192-  │  │Slots 12288- │
│             │  │   8191      │  │  12287      │  │   16383     │
└──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘
       │                │                │                │
       ▼                ▼                ▼                ▼
┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│  REPLICA 0  │  │  REPLICA 1  │  │  REPLICA 2  │  │  REPLICA 3  │
│  Port 7004  │  │  Port 7005  │  │  Port 7006  │  │  Port 7007  │
└─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘
```

---

## 🔍 Key-Value Operations

### GET by Key (Direct Lookup)
```
GET Customer:1:first_name → "Ahmed"
GET Product:101:price → "4500.0"
GET Order:1001:status → "delivered"
```

### GET by Value (Pattern Matching)
```
KEYS Customer:1:*       → All attributes for Customer 1
KEYS Customer:*:email   → All customer emails
KEYS Product:*:price    → All product prices
KEYS Order:*:status     → All order statuses
```

---

## 🚀 How to Run

### Option 1: Google Colab (Recommended)
1. Open `CSC582_Redis_Demo_Complete.ipynb` in Google Colab
2. Run cells sequentially (Shift+Enter)
3. Watch the live demonstration

### Option 2: Local Python
```bash
# Install Redis
sudo apt-get install redis-server

# Install Python dependencies
pip install redis

# Run the script
python migrate_to_redis.py
```

---

## 🎯 Presentation Flow

| Part | What to Show | Time |
|------|--------------|------|
| 1 | Relational tables (Customer, Product, Order) | 1 min |
| 2 | Mapping strategy explanation | 1 min |
| 3 | Migration to Redis (SET commands) | 1 min |
| 4 | **Sharding** - keys distributed to different masters | 1 min |
| 5 | **Replication** - write to master, read from replica | 1 min |
| 6 | GET by Key examples | 1 min |
| 7 | GET by Value (KEYS pattern) | 1 min |

---

## 📊 Grading Criteria Covered

| Criteria | Implementation |
|----------|----------------|
| ✅ Mapping | Complete relational-to-KV mapping strategy |
| ✅ Sharding | Hash-based partitioning (16384 slots, 4 masters) |
| ✅ Replication | 4 replica nodes for high availability |
| ✅ Queries | Primary & secondary index lookups demonstrated |
| ✅ Presentation | PDF report with diagrams |
| ✅ Demo | Interactive Colab notebook |

---

## 🛠️ Technologies Used

- **Relational DB:** SQLite
- **Key-Value Store:** Redis
- **Language:** Python 3
- **Demo Platform:** Google Colab
- **Report:** ReportLab (PDF generation)

---

## 📈 Project Statistics

| Metric | Value |
|--------|-------|
| Relational Tables | 3 (Customer, Product, Order) |
| Total Records | 18 (5 + 6 + 7) |
| Total Redis Keys | 58 |
| Master Nodes | 4 |
| Replica Nodes | 4 |
| Hash Slots | 16384 |

---

**Course:** CSC582 - Data Warehouse and Mining Systems

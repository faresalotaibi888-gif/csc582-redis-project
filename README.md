# 🎓 CSC582: Relational to Key-Value Database Migration

## E-Commerce Database → Redis Cluster

**Course:** CSC582 - Data Warehouse and Mining Systems  
**Project:** Relational to Key-Value Database Mapping  

---

## 🚀 Quick Start - Run Online Demo

### Option 1: Google Colab (Recommended)
[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/YOUR_USERNAME/csc582-redis-project/blob/main/CSC582_Redis_Demo.ipynb)

1. Click the badge above (after uploading to GitHub)
2. Or upload `CSC582_Redis_Demo.ipynb` directly to [Google Colab](https://colab.research.google.com)
3. Run all cells in order

### Option 2: Run Locally
```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/csc582-redis-project.git
cd csc582-redis-project

# Run the migration script
python3 src/migrate_to_redis.py
```

---

## 📁 Project Structure

```
csc582-redis-project/
├── CSC582_Redis_Demo.ipynb    # 🔥 Interactive Colab Notebook (DEMO)
├── README.md                   # This file
├── .gitignore
│
├── sql/
│   └── ecommerce_schema.sql   # Relational database schema
│
├── src/
│   └── migrate_to_redis.py    # Main migration script
│
├── redis/
│   ├── setup_cluster.sh       # Cluster setup script
│   └── redis_commands.txt     # CLI commands reference
│
└── report/
    ├── generate_report.py     # PDF generator
    └── CSC582_Project_Report.pdf  # Final report
```

---

## 📊 Project Components

### 1. Relational Database Schema
| Table | Columns | Description |
|-------|---------|-------------|
| customers | customer_id, first_name, last_name, email, phone, city, country | Customer information |
| products | product_id, product_name, category, price, stock_quantity | Product catalog |
| orders | order_id, customer_id, order_date, status, total_amount | Customer orders |

### 2. Redis Key-Value Mapping

| Relational Table | Redis Key Pattern | Type |
|-----------------|-------------------|------|
| customers | `customer:{id}` | HASH |
| products | `product:{id}` | HASH |
| orders | `order:{id}` | HASH |

### 3. Secondary Indexes
| Index Pattern | Type | Purpose |
|--------------|------|---------|
| `idx:product:category:{cat}` | SET | Find products by category |
| `idx:customer:{id}:orders` | SET | Find customer's orders |
| `idx:orders:status:{status}` | SET | Find orders by status |

### 4. Redis Cluster Architecture (8 Nodes)

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

## 🔍 Key-Value Operations Examples

### HSET/HGETALL (Hash Operations)
```redis
HSET customer:1 first_name "Ahmed" last_name "Al-Rashid" email "ahmed@email.com"
HGETALL customer:1
HGET customer:1 email
```

### SET/GET (String Operations)
```redis
SET greeting "Hello, World!"
GET greeting
```

### SADD/SMEMBERS (Set Operations)
```redis
SADD idx:product:category:electronics "101" "102" "105"
SMEMBERS idx:product:category:electronics
```

---

## 📝 Grading Criteria Covered

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

## 📄 License

This project is for educational purposes - CSC582 Data Warehouse and Mining Systems.

---

**December 2024**

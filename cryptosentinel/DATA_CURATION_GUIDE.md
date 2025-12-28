# Data Curation Guide

## Overview

This guide explains how the unified data collection system works, where data is stored, and how the timestamp-based strategy is implemented.

---

## 📋 What is an Avro Schema File?

**Avro Schema** = "Table Definition" for Kafka messages

### Purpose:
- Defines the structure of data sent to Kafka topics
- Validates data before sending (ensures data quality)
- Enables Schema Registry versioning (handles schema evolution)
- Ensures producers and consumers agree on data format

### Example:
The file `schemas/unified_market_data.avsc` defines that each Kafka message contains:
- `timestamp_ms` (long integer)
- `coin_id` (string)
- `price_usd` (double or null)
- `fear_greed_value` (integer or null)
- etc.

**Think of it like:** A database table schema, but for streaming data in Kafka.

---

## 📊 Where Data is Written

When you run the unified collector, data is written to **3 places**:

### 1. **CSV File (Primary Storage)**
**Location:** `data/canonical/unified_market_data.csv`

**Format:** Single unified table with all coins
```
timestamp | coin_id | price_usd | volume_24h | return_pct | fear_greed_value | news_count | ...
2025-12-28 10:00:00 | bitcoin | 87729 | 15818125578 | 0.30 | 24 | 5 | ...
2025-12-28 10:00:00 | ethereum | 2941 | 9911846305 | 0.70 | 24 | 3 | ...
2025-12-28 10:01:00 | bitcoin | 87750 | 15820000000 | 0.35 | 24 | 5 | ...
2025-12-28 10:01:00 | ethereum | 2945 | 9920000000 | 0.75 | 24 | 3 | ...
```

**Key Features:**
- ✅ **One row per (timestamp, coin_id)** - exactly what you requested!
- ✅ **All API data in one entry** - price, sentiment, news, fear & greed
- ✅ **Appends new data** - doesn't overwrite, just adds new rows
- ✅ **Deduplicates** - if same timestamp+coin exists, keeps latest

### 2. **Kafka Topic (Streaming)**
**Topic Name:** `analytics.unified`

**Purpose:** Real-time streaming for downstream consumers
- Data flows through Confluent Cloud
- Can be consumed by other services
- Uses Avro schema for validation

**Note:** Only works if Kafka is configured in `.env`

### 3. **In-Memory Buffer**
**Location:** `collector.history` (Python list)

**Purpose:** Fast access for analysis
- Keeps last 1000 records in memory
- Used for quick DataFrame generation
- Cleared when collector stops

---

## ✅ Timestamp-Based Unified Table Strategy

**YES, we ARE implementing your strategy!** Here's how it works:

### At Each Timestamp (e.g., every 60 seconds):

```
1. Record current timestamp: 2025-12-28 10:00:00
   ↓
2. Call ALL APIs in parallel:
   - CoinGecko API → price data
   - Fear & Greed API → sentiment index
   - NewsData API → news sentiment
   ↓
3. Merge results into unified records:
   - One record per coin
   - All API data combined
   - Same timestamp for all
   ↓
4. Write to unified table:
   timestamp | coin_id | price_usd | fear_greed_value | news_count | ...
   10:00:00 | bitcoin | 87729 | 24 | 5 | ...
   10:00:00 | ethereum | 2941 | 24 | 3 | ...
```

### Example Data Flow:

```
Timestamp: 2025-12-28 10:00:00
├── CoinGecko API → {bitcoin: {price: 87729, volume: ...}, ethereum: {...}}
├── Fear & Greed API → {bitcoin: {value: 24}, ethereum: {value: 24}}
└── NewsData API → {bitcoin: {count: 5, sentiment: 0.3}, ethereum: {...}}
         ↓
    MERGE INTO:
    ┌─────────────┬──────────┬──────────┬──────────────┬─────────────┐
    │ timestamp   │ coin_id  │ price_usd│ fear_greed_val│ news_count │
    ├─────────────┼──────────┼──────────┼──────────────┼─────────────┤
    │ 10:00:00    │ bitcoin  │ 87729    │ 24           │ 5           │
    │ 10:00:00    │ ethereum │ 2941     │ 24           │ 3           │
    └─────────────┴──────────┴──────────┴──────────────┴─────────────┘
```

---

## 🚀 Usage

### Run Collector (Unified Table Mode - Default)
```bash
cd cryptosentinel
./venv/bin/python -m producers.unified_collector --interval 60 --coins bitcoin,ethereum
```

This will:
- Collect from all APIs every 60 seconds
- Save to `data/canonical/unified_market_data.csv` (single table)
- Append new rows (doesn't overwrite)

### Run Once (Test)
```bash
./venv/bin/python -m producers.unified_collector --once --coins bitcoin,ethereum
```

### Load Data for Analysis
```python
from producers.unified_collector import UnifiedDataCollector

collector = UnifiedDataCollector(unified_table=True)

# Load all coins
df = collector.load_from_csv()

# Load specific coin
df_btc = collector.load_from_csv(coin_id="bitcoin")
```

---

## 📁 File Structure

```
cryptosentinel/
├── data/
│   └── canonical/
│       └── unified_market_data.csv  ← SINGLE UNIFIED TABLE (all coins)
│
├── producers/
│   ├── unified_collector.py         ← Main orchestrator
│   ├── fear_greed_collector.py      ← Fear & Greed API
│   ├── news_collector.py            ← NewsData.io API
│   └── price_producer.py            ← CoinGecko API (existing)
│
└── schemas/
    └── unified_market_data.avsc    ← Kafka schema definition
```

---

## 🔍 Data Schema

The unified table has these columns:

| Column | Source | Type | Description |
|--------|--------|------|-------------|
| `timestamp` | System | datetime | When data was collected |
| `coin_id` | System | string | Cryptocurrency ID |
| `price_usd` | CoinGecko | float | Current price |
| `volume_24h` | CoinGecko | float | 24h trading volume |
| `return_pct` | CoinGecko | float | 24h price change % |
| `market_cap` | CoinGecko | float | Market cap |
| `fear_greed_value` | Alternative.me | int | 0-100 scale |
| `fear_greed_classification` | Alternative.me | string | Text label |
| `news_count` | NewsData.io | int | Article count |
| `news_sentiment_avg` | NewsData.io | float | -1.0 to 1.0 |
| `news_positive_count` | NewsData.io | int | Positive articles |
| `news_negative_count` | NewsData.io | int | Negative articles |

---

## ✅ Summary

1. **Avro Schema**: Defines structure for Kafka messages (like a table schema)
2. **Data Storage**: Single CSV file `unified_market_data.csv` with all coins
3. **Timestamp Strategy**: ✅ YES - At each timestamp, collect from ALL APIs and merge into one entry per coin
4. **Unified Table**: ✅ YES - All coins in one table, one row per (timestamp, coin_id)

---

## 🎯 Next Steps

1. Run the collector to start gathering data
2. Data accumulates in `unified_market_data.csv`
3. Use this table for causal analysis (Granger causality, etc.)
4. All APIs contribute to each timestamp entry!


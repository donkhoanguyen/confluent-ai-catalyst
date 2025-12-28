# CryptoSentinel Development Process

## Decision: Parallel Development Tracks

**Date**: December 28, 2024  
**Status**: Active

We have decided to separate development into **two parallel workstreams** that can progress independently, with a clear integration point.

---

## Why Two Tracks?

| Factor | Benefit |
|--------|---------|
| **Loose coupling** | Teams can work without blocking each other |
| **Independent testing** | Data track uses mock causal results; Causal track uses offline CSV |
| **Skill specialization** | Data engineering vs. statistical/ML expertise |
| **Faster iteration** | Parallel progress, earlier demo readiness |

---

## Track 1: Data Curation & Integration

**Focus**: Getting data flowing through the pipeline  
**Owner**: TBD  
**Test Strategy**: Mock causal results until Track 2 is ready

### Scope

- External API integration (CoinGecko, Fear & Greed Index, NewsData.io)
- Unified timestamp-based data collection
- Kafka producers and consumers
- Gemini sentiment enrichment (for news articles)
- Schema management (Avro schemas + Schema Registry)
- FastAPI backend (REST + WebSocket)
- Real-time data availability
- Unified CSV storage for analysis

### Files Owned by Track 1

```
cryptosentinel/
├── producers/                          ← DATA INGESTION
│   ├── __init__.py
│   ├── base_producer.py                → Abstract producer class
│   ├── reddit_producer.py              → Streams Reddit posts to Kafka (legacy)
│   ├── price_producer.py               → Streams crypto prices to Kafka (legacy)
│   ├── factory.py                      → Producer factory pattern
│   ├── unified_collector.py            → ⭐ NEW: Unified timestamp-based collector
│   ├── fear_greed_collector.py         → ⭐ NEW: Fear & Greed Index API
│   └── news_collector.py               → ⭐ NEW: NewsData.io + Gemini sentiment
│
├── consumers/                          ← DATA ENRICHMENT
│   ├── __init__.py
│   └── enrichment_consumer.py          → Gemini sentiment + produces enriched data
│
├── schemas/                            ← DATA CONTRACTS
│   ├── reddit_post.avsc                → Raw Reddit schema
│   ├── reddit_post_enriched.avsc       → Enriched with sentiment
│   ├── crypto_price.avsc               → Price data schema
│   ├── unified_market_data.avsc        → ⭐ NEW: Unified data schema
│   └── causal_result.avsc              → Causal output schema
│
├── api/                                ← DATA ACCESS LAYER
│   ├── __init__.py
│   ├── main.py                         → FastAPI backend (REST + WebSocket)
│   └── agent.py                        → Agent API endpoints
│
├── config/                             ← CONFIGURATION
│   ├── __init__.py
│   └── settings.py                     → Credentials, Kafka config, API keys
│
├── agent/                              ← AGENT DATA PLUMBING (shared with Track 2)
│   ├── confluent_client.py             → Topic creation, schema registration
│   ├── asset_registry.py               → Registry of available data sources
│   ├── data_discovery.py               → Maps variables → Kafka topics
│   └── dataframe_builder.py            → Builds canonical DataFrames
│
└── data/                               ← SAMPLE/OFFLINE DATA
    └── canonical/
        ├── bitcoin_offline_demo.csv    → Offline test data
        └── unified_market_data.csv     → ⭐ NEW: Unified timestamp-based table
```

### Key Deliverables

1. [x] Reddit producer streaming to Kafka ✅
2. [x] Price producer streaming to Kafka ✅
3. [x] Enrichment consumer with Gemini sentiment ✅
4. [x] FastAPI endpoints serving real-time data ✅
5. [x] WebSocket for live updates ✅
6. [x] Schema registry integration ✅
7. [x] Unified data collector (timestamp-based) ✅ **NEW**
8. [x] News API integration with Gemini sentiment ✅ **NEW**
9. [x] Fear & Greed Index integration ✅ **NEW**
10. [x] Unified CSV storage (single table format) ✅ **NEW**

---

## Track 2: Causal Engine

**Focus**: Statistical analysis and insight generation  
**Owner**: TBD  
**Test Strategy**: Use `bitcoin_offline_demo.csv` until Track 1's live data is ready

### Scope

- Causal inference algorithms (Granger, Transfer Entropy, PSM, IV, PC)
- Hypothesis generation with Gemini
- Confounder discovery
- LangGraph orchestration
- Alert generation
- Dashboard visualization

### Files Owned by Track 2

```
cryptosentinel/
├── causal/                             ← CORE CAUSAL ALGORITHMS
│   ├── __init__.py
│   ├── base.py                         → Abstract CausalEngine interface
│   ├── granger.py                      → Granger causality (bidirectional)
│   ├── granger_engine.py               → Granger wrapped as engine
│   ├── transfer_entropy.py             → Information-theoretic causality
│   ├── psm.py                          → Propensity Score Matching
│   ├── iv.py                           → Instrumental Variables
│   ├── pc_algorithm.py                 → PC Algorithm for DAG discovery
│   ├── confounder_test.py              → Confounder detection tests
│   └── registry.py                     → Engine registry & auto-selection
│
├── agent/                              ← AI-DRIVEN ORCHESTRATION
│   ├── __init__.py
│   ├── orchestrator.py                 → LangGraph-based discovery agent
│   ├── hypothesis_generator.py         → Gemini generates causal hypotheses
│   ├── confounder_discovery.py         → Gemini discovers confounders
│   ├── models.py                       → Domain models (Hypothesis, CausalResult)
│   ├── domain.py                       → Domain/variable registries
│   └── langgraph_entry.py              → LangGraph configuration
│
├── dashboard/                          ← VISUALIZATION
│   ├── __init__.py
│   ├── app.py                          → Main Streamlit dashboard
│   └── agent_view.py                   → Agent-specific dashboard view
│
└── Supporting files:
    ├── example_agent_usage.py          → How to use the agent
    ├── run_demo.py                     → Demo script
    ├── langgraph.json                  → LangGraph config
    └── agent_graph_visualization.ipynb → Visualize agent workflow
```

### Key Deliverables

1. [ ] Granger causality with bidirectional testing
2. [ ] Transfer entropy for non-linear relationships
3. [ ] Hypothesis generation via Gemini
4. [ ] Confounder discovery and testing
5. [ ] LangGraph orchestration loop
6. [ ] Rolling window causal analysis
7. [ ] Alert generation on causal shifts
8. [ ] Dashboard with causal direction visualization

---

## Integration Point

The two tracks meet at the **DataFrame Builder**:

```
┌─────────────────────────────────────────────────────────────────┐
│                        TRACK 1                                   │
│   Kafka Topics → Enriched Data → API Endpoints                  │
└─────────────────────────────────┬───────────────────────────────┘
                                  │
                                  ▼
                    ┌─────────────────────────────┐
                    │   agent/dataframe_builder.py │
                    │   Canonical DataFrame Format │
                    └─────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                        TRACK 2                                   │
│   Causal Engines → Hypothesis Testing → Dashboard               │
└─────────────────────────────────────────────────────────────────┘
```

### Canonical DataFrame Schema

Both tracks must agree on this schema for integration:

**Current Unified Schema** (implemented):

| Column | Type | Source | Notes |
|--------|------|--------|-------|
| `timestamp` | datetime | System | Collection timestamp |
| `coin_id` | string | System | Cryptocurrency identifier |
| `price_usd` | float | CoinGecko | Current price in USD |
| `return_pct` | float | CoinGecko | 24h price change % |
| `volume_24h` | float | CoinGecko | 24h trading volume |
| `market_cap` | float | CoinGecko | Market capitalization |
| `fear_greed_value` | int | Alternative.me | 0-100 scale (market-wide) |
| `fear_greed_classification` | string | Alternative.me | Text label (Extreme Fear, etc.) |
| `news_count` | int | NewsData.io | Number of news articles |
| `news_sentiment_avg` | float | Gemini AI | -1.0 to 1.0 (from news articles) |
| `news_positive_count` | int | Gemini AI | Positive news count |
| `news_negative_count` | int | Gemini AI | Negative news count |

**Legacy Schema** (for Reddit-based data, if needed):

| Column | Type | Source |
|--------|------|--------|
| `timestamp` | datetime | Track 1 (Kafka) |
| `coin_id` | string | Track 1 (API) |
| `price_usd` | float | Track 1 (CoinGecko) |
| `return_pct` | float | Track 1 (calculated) |
| `volume_24h` | float | Track 1 (CoinGecko) |
| `avg_sentiment` | float | Track 1 (Gemini from Reddit) |
| `post_count` | int | Track 1 (Reddit) |
| `positive_count` | int | Track 1 (Reddit) |
| `negative_count` | int | Track 1 (Reddit) |

**Note**: The unified collector uses News API instead of Reddit (due to Reddit API limitations). Reddit producer still exists but is separate.

---

## Development Workflow

### Track 1 Standalone Testing

**Recommended: Unified Collector (New Approach)**

```bash
# Unified collector - collects from all APIs at once
cd cryptosentinel
source venv/bin/activate

# Run test script (8 cycles, 10 seconds apart)
python test_unified_collector.py

# Or run continuously
python -m producers.unified_collector --interval 60 --coins bitcoin,ethereum

# Start API
python -m api.main
```

**Legacy: Separate Producers (Still Available)**

```bash
# Use mock causal results
cd cryptosentinel
source venv/bin/activate

# Start producers (requires API keys)
python -m producers.reddit_producer
python -m producers.price_producer

# Start enrichment
python -m consumers.enrichment_consumer

# Start API with demo mode
python -m api.main
```

### Track 2 Standalone Testing

```bash
# Use offline data
cd cryptosentinel
source venv/bin/activate

# Set offline mode
export OFFLINE_MODE=true

# Run agent with sample data
python example_agent_usage.py

# Or run dashboard with demo data
streamlit run dashboard/app.py
```

### Integration Testing

```bash
# Full pipeline (both tracks)
./run_pipeline.sh
```

---

## Communication Protocol

### When Track 1 Changes Schema

1. Update `schemas/*.avsc`
2. Update `data/canonical/*.csv` sample files
3. Notify Track 2 of column changes
4. Update this document's "Canonical DataFrame Schema"

### When Track 2 Needs New Data

1. File an issue describing required variable
2. Track 1 evaluates data source availability
3. Update `agent/asset_registry.py` with new source
4. Track 1 implements producer/enrichment

---

## Shared Resources (Both Tracks)

| Resource | Path | Notes |
|----------|------|-------|
| Settings | `config/settings.py` | Coordinate credential changes |
| Models | `agent/models.py` | Core dataclasses |
| Asset Registry | `agent/asset_registry.py` | Available data sources |
| DataFrame Builder | `agent/dataframe_builder.py` | Integration point |
| Requirements | `requirements.txt` | Coordinate dependency changes |

---

## Current Status

| Track | Status | Blockers |
|-------|--------|----------|
| Track 1: Data | ✅ **COMPLETE** | All deliverables done. Unified collector operational. |
| Track 2: Causal | 🟡 In Progress | Needs more sample data |

### Track 1 Completion Details

**✅ All Core Deliverables Complete:**
- ✅ Reddit producer (legacy, separate)
- ✅ Price producer (legacy, separate)
- ✅ Unified collector (NEW - recommended approach)
- ✅ Enrichment consumer with Gemini
- ✅ FastAPI with REST + WebSocket
- ✅ Schema registry integration
- ✅ Kafka topic: `agent.analytics_joined_kafka.raw`

**✅ Additional Features Implemented:**
- ✅ Fear & Greed Index API integration (no auth required)
- ✅ NewsData.io API with Gemini sentiment analysis
- ✅ Unified CSV storage (`unified_market_data.csv`)
- ✅ Timestamp-based data collection strategy
- ✅ Parallel API collection with error handling

**📊 Data Sources Currently Active:**
1. **CoinGecko** - Price, volume, market cap, 24h change
2. **Fear & Greed Index** - Market sentiment (0-100 scale)
3. **NewsData.io + Gemini** - News articles with AI sentiment analysis

**🔄 Data Flow:**
```
APIs (CoinGecko, Fear & Greed, News) 
  → Unified Collector (parallel collection)
  → Unified Record (one per coin per timestamp)
  → CSV Storage (unified_market_data.csv)
  → Kafka Topic (agent.analytics_joined_kafka.raw)
  → FastAPI (REST + WebSocket)
```

---

## Refactoring Notes

Minor refactoring identified (non-blocking):

1. **Gemini client duplication** - 4 files instantiate `genai.Client()`. Consider creating `config/clients.py` with shared factory.

2. **CausalDirection enum** - Currently in `causal/granger.py`, should move to `agent/models.py` with other shared enums.

3. **fetch_data placeholder** - `causal/base.py` has incomplete implementation. Wire to `dataframe_builder.py`.

These are **nice-to-have** and should not block feature development.

---

## Revision History

| Date | Change | Author |
|------|--------|--------|
| 2024-12-28 | Initial decision to separate tracks | Team |
| 2024-12-28 | Track 1 marked complete - unified collector implemented | Team |
| 2024-12-28 | Updated canonical schema to reflect unified approach | Team |
| 2024-12-28 | Added Fear & Greed Index and News API to data sources | Team |

## Architecture Changes from Original Plan

### What Changed:

1. **Unified Collector Approach** (NEW)
   - Original: Separate producers for each API
   - Current: Single unified collector that collects from all APIs at each timestamp
   - Benefit: Ensures data alignment, easier to maintain, single source of truth

2. **News API Instead of Reddit** (Pragmatic Change)
   - Original: Reddit API for social sentiment
   - Current: NewsData.io API + Gemini for news sentiment
   - Reason: Reddit API limitations/availability
   - Benefit: More reliable, includes built-in article metadata

3. **Fear & Greed Index Added** (Enhancement)
   - Original: Not in plan
   - Current: Integrated as market-wide sentiment indicator
   - Benefit: Provides contrarian market signal, no auth required

4. **Unified CSV Storage** (Enhancement)
   - Original: Separate CSV files per data source
   - Current: Single `unified_market_data.csv` with all data
   - Benefit: Easier analysis, timestamp alignment guaranteed

### What Stayed the Same:

- ✅ Kafka integration
- ✅ Schema Registry
- ✅ Gemini sentiment analysis
- ✅ FastAPI + WebSocket
- ✅ DataFrame builder as integration point


# CryptoSentinel: Real-Time Causal Intelligence for Crypto Markets

## A Confluent + Google AI Hackathon Project

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Problem Statement](#problem-statement)
3. [Why Causality is Our Competitive Advantage](#why-causality-is-our-competitive-advantage)
4. [Solution Overview](#solution-overview)
5. [System Architecture](#system-architecture)
6. [Data Sources & APIs](#data-sources--apis)
7. [Confluent Pipeline Design](#confluent-pipeline-design)
8. [Google AI Integration](#google-ai-integration)
9. [Causal Inference Engine](#causal-inference-engine)
10. [Frontend Dashboard](#frontend-dashboard)
11. [Technical Implementation](#technical-implementation)
12. [Demo Script](#demo-script)
13. [Success Metrics](#success-metrics)
14. [Project Timeline](#project-timeline)
15. [Risk Mitigation](#risk-mitigation)
16. [Future Enhancements](#future-enhancements)

---

## Executive Summary

**CryptoSentinel** is a real-time causal intelligence platform that answers a fundamental question in crypto markets:

> **"Does social sentiment CAUSE price movements, or do price movements CAUSE social sentiment?"**

Unlike traditional correlation-based analytics, CryptoSentinel uses **streaming causal inference** to determine the *direction* of influence between social media activity and market prices—in real-time.

### Key Differentiators

| Traditional Analytics | CryptoSentinel |
|-----------------------|----------------|
| Correlation ("X and Y move together") | Causation ("X causes Y to move") |
| Batch processing (hourly/daily) | Real-time streaming (<1 second) |
| Single data source | Multi-source fusion |
| Static dashboards | Dynamic causal graphs |
| Backward-looking | Forward-predicting |

### Technology Stack

- **Data Streaming**: Confluent Cloud (Kafka, ksqlDB, Schema Registry)
- **AI/ML**: Google Vertex AI, Gemini API
- **Causal Inference**: DoWhy, EconML, Granger Causality
- **Frontend**: Streamlit
- **Infrastructure**: Google Cloud Platform

---

## Problem Statement

### The Challenge

Crypto markets are uniquely influenced by social sentiment. A single tweet, Reddit post, or news article can trigger massive price swings. However, traders and analysts face critical questions:

1. **Causality vs. Correlation**: Does Reddit buzz *cause* price pumps, or do price pumps *cause* Reddit buzz?
2. **Lead-Lag Dynamics**: How many minutes/hours does sentiment lead (or lag) price?
3. **Pump-and-Dump Detection**: Can we identify coordinated manipulation in real-time?
4. **Signal vs. Noise**: Which social signals actually matter?

### Why Current Solutions Fail

| Current Approach | Limitation |
|------------------|------------|
| Sentiment scores | Only correlation, not causation |
| Batch analytics | Too slow for crypto volatility |
| Single-source data | Misses cross-platform dynamics |
| Retrospective analysis | Can't predict, only explain |

### Business Impact

- **Traders**: Miss alpha because they can't distinguish leading from lagging indicators
- **Exchanges**: Can't detect manipulation before it causes harm
- **Regulators**: Lack real-time surveillance tools
- **Researchers**: Don't have streaming causal infrastructure

---

## Why Causality is Our Competitive Advantage

### The Hackathon Landscape

Most AI + streaming projects at hackathons fall into predictable categories:

| Common Project Types | What They Do | Limitation |
|---------------------|--------------|------------|
| Sentiment Dashboards | Show positive/negative scores | Correlation only |
| Price Predictors | ML models on historical data | Black box, no "why" |
| Alerting Systems | Threshold-based notifications | Reactive, not predictive |
| RAG Chatbots | Answer questions about data | No analytical insight |
| Anomaly Detectors | Flag unusual patterns | No causal explanation |

**Our differentiator**: We don't just show THAT sentiment and price move together—we show WHICH ONE CAUSES THE OTHER, and by how much time.

### Why Causality is Essential (Not Optional) for This Use Case

#### 1. The Core Question is Inherently Causal

| Question Type | Example | Method Needed |
|---------------|---------|---------------|
| Descriptive | "What is the current sentiment?" | Simple aggregation |
| Correlational | "Do sentiment and price move together?" | Correlation coefficient |
| **Causal** | "Does sentiment CAUSE price to move?" | **Granger causality, etc.** |

The question we're answering—"Does Reddit hype cause pumps, or do pumps cause Reddit hype?"—**cannot be answered with correlation alone**. This is a fundamentally causal question that requires causal methods.

#### 2. Actionable Insights Require Causality

| Insight Type | Correlation-Based | Causality-Based |
|--------------|-------------------|-----------------|
| Trading Signal | "Sentiment is high" (so what?) | "Sentiment is LEADING price—buy now before the move" |
| Risk Warning | "Price dropped and sentiment dropped" | "Price drop CAUSED panic—sentiment will recover" |
| Manipulation Detection | "Unusual activity detected" | "Coordinated posts are CAUSING artificial pump" |

**Without causality**: You see patterns but don't know if acting on them makes sense.
**With causality**: You know whether sentiment is a leading indicator you can act on, or a lagging reaction you should ignore.

#### 3. The "12 Minutes" Insight Changes Everything

Consider these two statements:

> ❌ **Correlation**: "Reddit sentiment and Bitcoin price have a 0.73 correlation."

> ✅ **Causation**: "Reddit sentiment CAUSES Bitcoin price movements with a 12-minute lead time."

The first statement is interesting. The second statement is **actionable**:
- Traders know they have a 12-minute window to act
- Algorithms can be tuned to the optimal lag
- The insight has direct monetary value

#### 4. Causal Direction Flips Are High-Value Signals

In volatile markets, the causal relationship between sentiment and price **changes over time**:

| Market Condition | Typical Causal Direction |
|------------------|-------------------------|
| Bull market | Sentiment → Price (retail leads) |
| Bear market | Price → Sentiment (price shocks create fear) |
| News events | External shock → Both simultaneously |
| Manipulation | Coordinated posts → Artificial pump |

**Detecting when the causal direction flips is a trading signal in itself.** This is impossible with correlation-only analysis.

### Why Judges Will Care

| Judge Perspective | Why Causality Impresses |
|-------------------|------------------------|
| **Technical judges** | Demonstrates statistical sophistication beyond basic ML |
| **Business judges** | Shows understanding of decision-making needs |
| **Confluent engineers** | Novel use of streaming for real-time causal updates |
| **Google AI team** | Gemini used for causal interpretation, not just chat |

### Competitive Moat

| If Others Copy Our Idea | Our Advantage |
|-------------------------|---------------|
| Build sentiment dashboard | We have causal direction |
| Add price prediction | We explain WHY, not just WHAT |
| Use same APIs | Our statistical methodology is the product |
| Copy our UI | The insight quality can't be copied without the stats |

### The "Aha Moment" in Our Demo

**Setup**: Show live dashboard with Reddit posts streaming in, prices updating.

**Reveal**: 
> "See this spike in bullish posts 10 minutes ago? Watch the price chart... [wait 2 minutes]... there it is. Our system detected the causal relationship and could have signaled this move BEFORE it happened. That's not correlation. That's causation."

This moment is **only possible because we're doing causal inference**, not correlation.

### Simplified Implementation Path

If full Granger causality is too complex, we can achieve 80% of the impact with simpler methods:

| Approach | Complexity | Impact | Recommendation |
|----------|------------|--------|----------------|
| Full Granger Test | High | 100% | If time permits |
| Cross-correlation with lag | Medium | 85% | **Recommended fallback** |
| Lead-lag visualization | Low | 70% | Minimum viable |

**Cross-correlation approach** (simpler but still causal-ish):
```python
# Find optimal lag where sentiment predicts price
correlations = [df['sentiment'].shift(lag).corr(df['price_return']) 
                for lag in range(1, 30)]
optimal_lag = correlations.index(max(correlations)) + 1
# "Sentiment leads price by {optimal_lag} minutes"
```

This gives us the "X leads Y by N minutes" insight without full statistical machinery.

### Summary: Why This Angle Wins

| Factor | Our Advantage |
|--------|---------------|
| **Novel** | 95% of projects show correlation; we show causation |
| **Defensible** | Statistical methodology is hard to replicate quickly |
| **Actionable** | "12-minute lead" is more useful than "0.7 correlation" |
| **Narrative** | "Cause vs. effect" is a story everyone understands |
| **Technical depth** | Shows we understand the problem beyond surface level |
| **Business value** | Direct application to trading, risk, compliance |

---

## Solution Overview

### CryptoSentinel: How It Works

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DATA INGESTION                                     │
│                                                                             │
│   Reddit API ───┐                                                           │
│                 │                                                           │
│   CoinGecko ────┼───→ Confluent Kafka ───→ Stream Processing               │
│                 │                                                           │
│   NewsAPI ──────┘                                                           │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           STREAM PROCESSING                                  │
│                                                                             │
│   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                    │
│   │ ksqlDB      │    │ Gemini API  │    │ Time-Window │                    │
│   │ (Join/Agg)  │───→│ (Sentiment) │───→│ (Align)     │                    │
│   └─────────────┘    └─────────────┘    └─────────────┘                    │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           CAUSAL INFERENCE                                   │
│                                                                             │
│   ┌─────────────────────────────────────────────────────────────────────┐  │
│   │ Vertex AI + DoWhy                                                    │  │
│   │                                                                      │  │
│   │ • Granger Causality (temporal precedence)                           │  │
│   │ • Transfer Entropy (information flow)                                │  │
│   │ • Propensity Score Matching (confound control)                      │  │
│   │ • Instrumental Variables (exogenous shocks)                         │  │
│   └─────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           OUTPUT & INSIGHTS                                  │
│                                                                             │
│   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                    │
│   │ Real-Time   │    │ Causal      │    │ Gemini      │                    │
│   │ Dashboard   │    │ Alerts      │    │ Narration   │                    │
│   └─────────────┘    └─────────────┘    └─────────────┘                    │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Core Capabilities

1. **Real-Time Sentiment Extraction**: Gemini processes Reddit posts/news in <500ms
2. **Stream Joins**: Confluent aligns price ticks with sentiment at minute granularity
3. **Rolling Causal Analysis**: Granger causality computed on 1-hour rolling windows
4. **Causal Alerts**: "Sentiment is now LEADING price by 12 minutes" notifications
5. **Natural Language Insights**: Gemini explains causal findings in plain English

---

## System Architecture

### High-Level Architecture

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                                 DATA SOURCES                                      │
├────────────────────┬────────────────────┬────────────────────────────────────────┤
│     Reddit API     │    CoinGecko API   │              NewsAPI                   │
│   (Social Posts)   │   (Crypto Prices)  │           (News Articles)              │
└─────────┬──────────┴─────────┬──────────┴──────────────────┬─────────────────────┘
          │                    │                             │
          ▼                    ▼                             ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                            INGESTION LAYER                                        │
│  ┌──────────────────────────────────────────────────────────────────────────┐    │
│  │                     Python Producers (Kafka Clients)                      │    │
│  │                                                                           │    │
│  │  reddit_producer.py    price_producer.py    news_producer.py             │    │
│  └──────────────────────────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                          CONFLUENT CLOUD                                          │
│                                                                                   │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐                   │
│  │ Schema Registry │  │  Kafka Topics   │  │     ksqlDB      │                   │
│  │                 │  │                 │  │                 │                   │
│  │ • reddit.post   │  │ • crypto.prices │  │ • Stream joins  │                   │
│  │ • crypto.price  │  │ • reddit.posts  │  │ • Windowing     │                   │
│  │ • news.article  │  │ • news.articles │  │ • Aggregations  │                   │
│  │ • enriched.msg  │  │ • enriched.data │  │                 │                   │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘                   │
│                                                                                   │
│  ┌─────────────────────────────────────────────────────────────────────────┐     │
│  │                          Kafka Connect                                   │     │
│  │                                                                          │     │
│  │  BigQuery Sink Connector ───────────────────────→ Google BigQuery       │     │
│  └─────────────────────────────────────────────────────────────────────────┘     │
└──────────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                           GOOGLE CLOUD PLATFORM                                   │
│                                                                                   │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐                   │
│  │    BigQuery     │  │   Vertex AI     │  │   Gemini API    │                   │
│  │                 │  │                 │  │                 │                   │
│  │ • Time-series   │  │ • Causal models │  │ • Sentiment     │                   │
│  │ • Historical    │  │ • Prediction    │  │ • Narration     │                   │
│  │ • Analytics     │  │ • AutoML        │  │ • Explanation   │                   │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘                   │
│                                                                                   │
│  ┌─────────────────────────────────────────────────────────────────────────┐     │
│  │                      Cloud Functions / Cloud Run                         │     │
│  │                                                                          │     │
│  │  • causal_inference_service.py (Granger, Transfer Entropy)              │     │
│  │  • alert_service.py (Threshold monitoring)                              │     │
│  │  • gemini_enrichment.py (Real-time sentiment)                           │     │
│  └─────────────────────────────────────────────────────────────────────────┘     │
└──────────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                           APPLICATION LAYER                                       │
│                                                                                   │
│  ┌─────────────────────────────────────────────────────────────────────────┐     │
│  │                         Streamlit Dashboard                              │     │
│  │                                                                          │     │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │     │
│  │  │ Real-Time   │  │ Causal      │  │ Historical  │  │ AI          │    │     │
│  │  │ Prices      │  │ Direction   │  │ Analysis    │  │ Insights    │    │     │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘    │     │
│  │                                                                          │     │
│  │  ┌─────────────────────────────────────────────────────────────────┐    │     │
│  │  │ Causal Graph Visualization (Sentiment ←→ Price)                 │    │     │
│  │  └─────────────────────────────────────────────────────────────────┘    │     │
│  └─────────────────────────────────────────────────────────────────────────┘     │
└──────────────────────────────────────────────────────────────────────────────────┘
```

### Component Details

| Layer | Component | Purpose |
|-------|-----------|---------|
| **Ingestion** | Python Producers | Poll APIs, publish to Kafka |
| **Streaming** | Confluent Cloud | Managed Kafka, ksqlDB, Schema Registry |
| **Processing** | ksqlDB | Stream joins, windowing, aggregations |
| **Enrichment** | Gemini API | Real-time sentiment extraction |
| **Storage** | BigQuery | Time-series storage, historical analysis |
| **Analytics** | Vertex AI | Causal model training and inference |
| **Presentation** | Streamlit | Interactive dashboard |

---

## Data Sources & APIs

### 1. Reddit API

**Purpose**: Social sentiment from crypto communities

| Parameter | Value |
|-----------|-------|
| Endpoint | `https://oauth.reddit.com/r/{subreddit}/new` |
| Auth | OAuth2 (free, requires app registration) |
| Rate Limit | 60 requests/minute |
| Subreddits | `cryptocurrency`, `bitcoin`, `ethereum`, `solana`, `wallstreetbets` |

**Sample Payload**:
```json
{
  "id": "1abc2de",
  "subreddit": "cryptocurrency",
  "title": "Bitcoin breaking $100k is inevitable",
  "selftext": "With institutional adoption...",
  "score": 1542,
  "num_comments": 234,
  "created_utc": 1735315200,
  "author": "crypto_whale_42"
}
```

**Extracted Fields**:
- `post_id`: Unique identifier
- `subreddit`: Source community
- `title`: Post title (primary sentiment source)
- `body`: Post body (secondary sentiment source)
- `score`: Upvotes - downvotes (engagement signal)
- `num_comments`: Discussion intensity
- `created_utc`: Timestamp
- `mentioned_coins`: Extracted via NER (BTC, ETH, SOL, etc.)

---

### 2. CoinGecko API

**Purpose**: Real-time cryptocurrency prices

| Parameter | Value |
|-----------|-------|
| Endpoint | `https://api.coingecko.com/api/v3/simple/price` |
| Auth | None (free tier) / API key (paid) |
| Rate Limit | 10-50 calls/minute (free) |
| Coins | BTC, ETH, SOL, DOGE, XRP, ADA, etc. |

**Sample Payload**:
```json
{
  "bitcoin": {
    "usd": 98542.12,
    "usd_24h_vol": 45234567890,
    "usd_24h_change": 2.34,
    "last_updated_at": 1735315200
  },
  "ethereum": {
    "usd": 3421.56,
    "usd_24h_vol": 12345678901,
    "usd_24h_change": 1.87,
    "last_updated_at": 1735315200
  }
}
```

**Extracted Fields**:
- `coin_id`: Coin identifier (bitcoin, ethereum, etc.)
- `price_usd`: Current price in USD
- `volume_24h`: 24-hour trading volume
- `price_change_24h`: Percentage change
- `timestamp`: Price timestamp

---

### 3. NewsAPI

**Purpose**: Crypto news articles from major outlets

| Parameter | Value |
|-----------|-------|
| Endpoint | `https://newsapi.org/v2/everything` |
| Auth | API Key (free tier: 100 requests/day) |
| Rate Limit | 100 requests/day (free) |
| Query | `bitcoin OR ethereum OR crypto OR cryptocurrency` |

**Sample Payload**:
```json
{
  "status": "ok",
  "totalResults": 1234,
  "articles": [
    {
      "source": {"id": "bloomberg", "name": "Bloomberg"},
      "author": "John Doe",
      "title": "Bitcoin ETF Sees Record Inflows",
      "description": "Institutional investors pour...",
      "url": "https://...",
      "publishedAt": "2024-12-27T10:30:00Z"
    }
  ]
}
```

**Extracted Fields**:
- `article_id`: Hash of URL
- `source`: News outlet
- `title`: Article headline
- `description`: Article summary
- `published_at`: Publication timestamp
- `mentioned_coins`: Extracted via NER

---

### 4. Alternative/Backup APIs

| API | Purpose | Free Tier | Notes |
|-----|---------|-----------|-------|
| **Finnhub** | Stock + crypto news | 60/min | Backup for NewsAPI |
| **Alpha Vantage** | Crypto prices | 25/day | Backup for CoinGecko |
| **Polygon.io** | Market data | EOD free | Historical backup |
| **GDELT** | Global events | Unlimited | Macro events |

---

## Confluent Pipeline Design

### Kafka Topics

| Topic | Schema | Partitions | Retention | Purpose |
|-------|--------|------------|-----------|---------|
| `crypto.prices.raw` | Avro | 3 | 7 days | Raw price data |
| `social.reddit.raw` | Avro | 6 | 7 days | Raw Reddit posts |
| `news.articles.raw` | Avro | 3 | 7 days | Raw news articles |
| `social.reddit.enriched` | Avro | 6 | 14 days | Posts + sentiment |
| `news.articles.enriched` | Avro | 3 | 14 days | Articles + sentiment |
| `analytics.joined` | Avro | 6 | 30 days | Price + sentiment joined |
| `causal.results` | Avro | 1 | 90 days | Causal inference outputs |
| `alerts.causal` | Avro | 1 | 7 days | Causal shift alerts |

### Avro Schemas

**crypto.prices.raw**:
```json
{
  "type": "record",
  "name": "CryptoPrice",
  "namespace": "com.cryptosentinel",
  "fields": [
    {"name": "coin_id", "type": "string"},
    {"name": "price_usd", "type": "double"},
    {"name": "volume_24h", "type": "double"},
    {"name": "price_change_pct", "type": "double"},
    {"name": "timestamp", "type": "long", "logicalType": "timestamp-millis"}
  ]
}
```

**social.reddit.enriched**:
```json
{
  "type": "record",
  "name": "RedditPostEnriched",
  "namespace": "com.cryptosentinel",
  "fields": [
    {"name": "post_id", "type": "string"},
    {"name": "subreddit", "type": "string"},
    {"name": "title", "type": "string"},
    {"name": "body", "type": ["null", "string"]},
    {"name": "score", "type": "int"},
    {"name": "num_comments", "type": "int"},
    {"name": "created_utc", "type": "long"},
    {"name": "mentioned_coins", "type": {"type": "array", "items": "string"}},
    {"name": "sentiment_score", "type": "double"},
    {"name": "sentiment_label", "type": "string"},
    {"name": "sentiment_confidence", "type": "double"},
    {"name": "processed_at", "type": "long", "logicalType": "timestamp-millis"}
  ]
}
```

### ksqlDB Queries

**1. Create Streams**:
```sql
-- Price stream
CREATE STREAM prices_stream (
  coin_id VARCHAR KEY,
  price_usd DOUBLE,
  volume_24h DOUBLE,
  price_change_pct DOUBLE,
  timestamp BIGINT
) WITH (
  KAFKA_TOPIC = 'crypto.prices.raw',
  VALUE_FORMAT = 'AVRO'
);

-- Enriched Reddit stream
CREATE STREAM reddit_enriched_stream (
  post_id VARCHAR KEY,
  subreddit VARCHAR,
  title VARCHAR,
  mentioned_coins ARRAY<VARCHAR>,
  sentiment_score DOUBLE,
  sentiment_label VARCHAR,
  created_utc BIGINT
) WITH (
  KAFKA_TOPIC = 'social.reddit.enriched',
  VALUE_FORMAT = 'AVRO'
);
```

**2. Aggregate Sentiment by Coin (1-minute windows)**:
```sql
CREATE TABLE sentiment_by_coin_1min AS
SELECT
  EXPLODE(mentioned_coins) AS coin_id,
  WINDOWSTART AS window_start,
  WINDOWEND AS window_end,
  COUNT(*) AS post_count,
  AVG(sentiment_score) AS avg_sentiment,
  SUM(CASE WHEN sentiment_label = 'POSITIVE' THEN 1 ELSE 0 END) AS positive_count,
  SUM(CASE WHEN sentiment_label = 'NEGATIVE' THEN 1 ELSE 0 END) AS negative_count,
  SUM(CASE WHEN sentiment_label = 'NEUTRAL' THEN 1 ELSE 0 END) AS neutral_count
FROM reddit_enriched_stream
WINDOW TUMBLING (SIZE 1 MINUTE)
GROUP BY EXPLODE(mentioned_coins)
EMIT CHANGES;
```

**3. Join Prices with Sentiment**:
```sql
CREATE STREAM price_sentiment_joined AS
SELECT
  p.coin_id,
  p.price_usd,
  p.price_change_pct,
  p.volume_24h,
  s.avg_sentiment,
  s.post_count,
  s.positive_count,
  s.negative_count,
  p.timestamp AS price_timestamp,
  s.window_start AS sentiment_window_start
FROM prices_stream p
INNER JOIN sentiment_by_coin_1min s
  WITHIN 5 MINUTES
  ON p.coin_id = s.coin_id
EMIT CHANGES;
```

**4. Calculate Price Returns (for Granger Causality)**:
```sql
CREATE TABLE price_returns_1min AS
SELECT
  coin_id,
  WINDOWSTART AS window_start,
  LATEST_BY_OFFSET(price_usd) AS close_price,
  EARLIEST_BY_OFFSET(price_usd) AS open_price,
  (LATEST_BY_OFFSET(price_usd) - EARLIEST_BY_OFFSET(price_usd)) 
    / EARLIEST_BY_OFFSET(price_usd) * 100 AS return_pct
FROM prices_stream
WINDOW TUMBLING (SIZE 1 MINUTE)
GROUP BY coin_id
EMIT CHANGES;
```

### Kafka Connect Configuration

**BigQuery Sink Connector**:
```json
{
  "name": "bigquery-sink-price-sentiment",
  "config": {
    "connector.class": "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector",
    "tasks.max": "1",
    "topics": "analytics.joined,causal.results",
    "project": "your-gcp-project-id",
    "datasets": "cryptosentinel",
    "keyfile": "/path/to/service-account.json",
    "autoCreateTables": "true",
    "autoUpdateSchemas": "true",
    "bufferSize": "100000",
    "maxWriteSize": "10000",
    "tableWriteWait": "1000"
  }
}
```

---

## Google AI Integration

### 1. Gemini API - Sentiment Extraction

**Purpose**: Extract sentiment from Reddit posts and news articles in real-time

**Model**: `gemini-2.5-flash` (optimized for speed)

**Prompt Template**:
```python
SENTIMENT_PROMPT = """
Analyze the following cryptocurrency-related social media post and extract:
1. Overall sentiment (POSITIVE, NEGATIVE, NEUTRAL)
2. Sentiment score (-1.0 to 1.0)
3. Confidence level (0.0 to 1.0)
4. Mentioned cryptocurrencies (list)
5. Key topics (list)

Post Title: {title}
Post Body: {body}
Subreddit: {subreddit}

Respond in JSON format:
{{
  "sentiment_label": "POSITIVE|NEGATIVE|NEUTRAL",
  "sentiment_score": <float>,
  "confidence": <float>,
  "mentioned_coins": ["BTC", "ETH", ...],
  "topics": ["price_prediction", "adoption", "regulation", ...]
}}
"""
```

**Implementation**:
```python
import google.genai as genai
from confluent_kafka import Consumer, Producer
import json

genai.configure(api_key=os.environ["GEMINI_API_KEY"])
model = genai.GenerativeModel("gemini-2.5-flash")

def enrich_with_sentiment(post: dict) -> dict:
    """Add sentiment analysis to a Reddit post."""
    prompt = SENTIMENT_PROMPT.format(
        title=post["title"],
        body=post.get("body", ""),
        subreddit=post["subreddit"]
    )
    
    response = model.generate_content(prompt)
    sentiment_data = json.loads(response.text)
    
    return {
        **post,
        "sentiment_score": sentiment_data["sentiment_score"],
        "sentiment_label": sentiment_data["sentiment_label"],
        "sentiment_confidence": sentiment_data["confidence"],
        "mentioned_coins": sentiment_data["mentioned_coins"],
        "topics": sentiment_data["topics"]
    }
```

### 2. Gemini API - Causal Interpretation

**Purpose**: Generate natural language explanations of causal findings

**Prompt Template**:
```python
CAUSAL_INTERPRETATION_PROMPT = """
You are a quantitative analyst explaining causal relationships in crypto markets.

Given the following causal analysis results:

Coin: {coin_id}
Time Window: {window_start} to {window_end}
Granger Causality Test:
  - Sentiment → Price: p-value = {sentiment_to_price_pval}, F-stat = {sentiment_to_price_f}
  - Price → Sentiment: p-value = {price_to_sentiment_pval}, F-stat = {price_to_sentiment_f}
Optimal Lag: {optimal_lag} minutes
Current Sentiment: {current_sentiment}
Recent Price Change: {price_change_pct}%

Provide a concise interpretation:
1. Which direction is the causal relationship (if any)?
2. How strong is the evidence?
3. What does this mean for traders/investors?
4. Any caveats or warnings?

Write in clear, professional language suitable for a financial dashboard.
"""
```

### 3. Vertex AI - Causal Model Training

**Purpose**: Train and deploy causal inference models at scale

**Pipeline**:
```python
from google.cloud import aiplatform
from google.cloud.aiplatform import CustomTrainingJob

# Initialize Vertex AI
aiplatform.init(project="your-project", location="us-central1")

# Define training job
job = CustomTrainingJob(
    display_name="causal-inference-training",
    script_path="training/causal_model.py",
    container_uri="us-docker.pkg.dev/vertex-ai/training/pytorch-gpu.1-13:latest",
    requirements=["dowhy", "econml", "statsmodels", "pandas"],
)

# Run training
model = job.run(
    dataset=dataset,
    model_display_name="cryptosentinel-causal-v1",
    args=["--epochs", "100", "--learning-rate", "0.001"],
    replica_count=1,
    machine_type="n1-standard-4",
)
```

---

## Causal Inference Engine

### Methodology Overview

CryptoSentinel implements multiple causal inference techniques:

| Technique | Purpose | When to Use |
|-----------|---------|-------------|
| **Granger Causality** | Temporal precedence | Time-series, lead-lag detection |
| **Transfer Entropy** | Information flow | Non-linear relationships |
| **Propensity Score Matching** | Confounder control | Observational data |
| **Instrumental Variables** | Exogenous shocks | Addressing endogeneity |
| **Difference-in-Differences** | Event impact | Before/after comparison |

### 1. Granger Causality

**Concept**: X "Granger-causes" Y if past values of X help predict Y, beyond what past values of Y alone can predict.

**Implementation**:
```python
from statsmodels.tsa.stattools import grangercausalitytests
import pandas as pd
import numpy as np

def compute_granger_causality(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    max_lag: int = 10
) -> dict:
    """
    Test if x_col Granger-causes y_col.
    
    Returns:
        dict with optimal_lag, p_value, f_statistic, is_significant
    """
    # Prepare data
    data = df[[y_col, x_col]].dropna()
    
    # Run Granger test for multiple lags
    results = grangercausalitytests(data, maxlag=max_lag, verbose=False)
    
    # Find optimal lag (lowest p-value)
    best_lag = min(results.keys(), 
                   key=lambda k: results[k][0]['ssr_ftest'][1])
    
    best_result = results[best_lag][0]['ssr_ftest']
    
    return {
        "optimal_lag": best_lag,
        "f_statistic": best_result[0],
        "p_value": best_result[1],
        "is_significant": best_result[1] < 0.05,
        "direction": f"{x_col} → {y_col}"
    }
```

### 2. Bidirectional Granger Test

**Purpose**: Determine if sentiment causes price, price causes sentiment, both, or neither.

```python
def bidirectional_granger_test(
    df: pd.DataFrame,
    sentiment_col: str = "avg_sentiment",
    price_col: str = "return_pct",
    max_lag: int = 10
) -> dict:
    """
    Test causality in both directions.
    
    Returns:
        dict with causal_direction, lead_lag_minutes, confidence
    """
    # Sentiment → Price
    s_to_p = compute_granger_causality(df, sentiment_col, price_col, max_lag)
    
    # Price → Sentiment
    p_to_s = compute_granger_causality(df, price_col, sentiment_col, max_lag)
    
    # Determine dominant direction
    if s_to_p["is_significant"] and not p_to_s["is_significant"]:
        direction = "SENTIMENT_LEADS"
        lead_lag = s_to_p["optimal_lag"]
        confidence = 1 - s_to_p["p_value"]
    elif p_to_s["is_significant"] and not s_to_p["is_significant"]:
        direction = "PRICE_LEADS"
        lead_lag = p_to_s["optimal_lag"]
        confidence = 1 - p_to_s["p_value"]
    elif s_to_p["is_significant"] and p_to_s["is_significant"]:
        direction = "BIDIRECTIONAL"
        # Stronger direction wins
        if s_to_p["f_statistic"] > p_to_s["f_statistic"]:
            lead_lag = s_to_p["optimal_lag"]
            confidence = s_to_p["f_statistic"] / (s_to_p["f_statistic"] + p_to_s["f_statistic"])
        else:
            lead_lag = -p_to_s["optimal_lag"]  # Negative = price leads
            confidence = p_to_s["f_statistic"] / (s_to_p["f_statistic"] + p_to_s["f_statistic"])
    else:
        direction = "NO_CAUSALITY"
        lead_lag = 0
        confidence = 0.0
    
    return {
        "direction": direction,
        "lead_lag_minutes": lead_lag,
        "confidence": confidence,
        "sentiment_to_price": s_to_p,
        "price_to_sentiment": p_to_s
    }
```

### 3. Rolling Window Causal Analysis

**Purpose**: Track how causal relationships change over time.

```python
def rolling_causal_analysis(
    df: pd.DataFrame,
    window_size: int = 60,  # 60 minutes
    step_size: int = 5,     # Recalculate every 5 minutes
    max_lag: int = 10
) -> list:
    """
    Perform causal analysis on rolling windows.
    """
    results = []
    
    for start in range(0, len(df) - window_size, step_size):
        window = df.iloc[start:start + window_size]
        
        if len(window) < window_size:
            continue
        
        causal_result = bidirectional_granger_test(window, max_lag=max_lag)
        
        results.append({
            "window_start": window.index[0],
            "window_end": window.index[-1],
            **causal_result
        })
    
    return results
```

### 4. Causal Alert Generation

**Purpose**: Trigger alerts when causal relationships shift.

```python
def generate_causal_alerts(
    current: dict,
    previous: dict,
    threshold: float = 0.3
) -> list:
    """
    Generate alerts when causal dynamics change significantly.
    """
    alerts = []
    
    # Direction change
    if current["direction"] != previous["direction"]:
        alerts.append({
            "type": "DIRECTION_CHANGE",
            "severity": "HIGH",
            "message": f"Causal direction shifted from {previous['direction']} to {current['direction']}",
            "action": "Review trading strategy"
        })
    
    # Lead-lag change
    lag_change = abs(current["lead_lag_minutes"] - previous["lead_lag_minutes"])
    if lag_change >= 3:  # 3+ minute shift
        alerts.append({
            "type": "LAG_SHIFT",
            "severity": "MEDIUM",
            "message": f"Lead-lag shifted by {lag_change} minutes",
            "action": "Adjust signal timing"
        })
    
    # Confidence change
    conf_change = current["confidence"] - previous["confidence"]
    if abs(conf_change) >= threshold:
        alerts.append({
            "type": "CONFIDENCE_CHANGE",
            "severity": "LOW",
            "message": f"Causal confidence {'increased' if conf_change > 0 else 'decreased'} by {abs(conf_change):.2f}",
            "action": "Monitor for stability"
        })
    
    return alerts
```

---

## Frontend Dashboard

### Dashboard Layout

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│  🔮 CryptoSentinel - Real-Time Causal Intelligence                    [BTC ▼]  │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  ┌───────────────────────────────────┐  ┌───────────────────────────────────┐  │
│  │       PRICE (Real-Time)           │  │      SENTIMENT (Real-Time)        │  │
│  │                                   │  │                                   │  │
│  │    $98,542.12  ▲ +2.34%          │  │    Score: 0.72  🟢 BULLISH        │  │
│  │    [Live price chart]             │  │    [Sentiment gauge]              │  │
│  │                                   │  │    Posts: 142/hr                  │  │
│  └───────────────────────────────────┘  └───────────────────────────────────┘  │
│                                                                                  │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │                    CAUSAL DIRECTION (Live)                                │  │
│  │                                                                           │  │
│  │     📊 SENTIMENT ────[12 min]────▶ 💰 PRICE                              │  │
│  │                                                                           │  │
│  │     Confidence: 94.2%     |     Last Updated: 2 seconds ago              │  │
│  │                                                                           │  │
│  │     "Reddit sentiment is currently LEADING Bitcoin price by 12 minutes.  │  │
│  │      Bullish posts in r/bitcoin are predictive of price increases."      │  │
│  │                                                      — Gemini Analysis    │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
│                                                                                  │
│  ┌───────────────────────────────────┐  ┌───────────────────────────────────┐  │
│  │    CAUSAL HISTORY (24h)           │  │    ALERTS                         │  │
│  │                                   │  │                                   │  │
│  │    [Timeline chart showing        │  │    🔴 Direction shifted at 14:32  │  │
│  │     when sentiment led vs         │  │    🟡 Lag increased to 15 min     │  │
│  │     when price led]               │  │    🟢 Confidence stabilized       │  │
│  │                                   │  │                                   │  │
│  └───────────────────────────────────┘  └───────────────────────────────────┘  │
│                                                                                  │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │    RECENT POSTS (Sorted by Impact)                                        │  │
│  │                                                                           │  │
│  │    r/bitcoin | "BTC to $150k by March" | Score: 1.2k | Sentiment: 0.89   │  │
│  │    r/crypto  | "SEC delays ETF again"  | Score: 892  | Sentiment: -0.42  │  │
│  │    r/wsb     | "All in on Bitcoin"     | Score: 3.4k | Sentiment: 0.95   │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
│                                                                                  │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │    HISTORICAL ANALYSIS                                        [7D ▼]     │  │
│  │                                                                           │  │
│  │    [Dual-axis chart: Price + Sentiment over time with shaded regions    │  │
│  │     showing when sentiment led (green) vs when price led (red)]          │  │
│  │                                                                           │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Streamlit Implementation

```python
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from confluent_kafka import Consumer
import google.genai as genai
import time

# Page config
st.set_page_config(
    page_title="CryptoSentinel",
    page_icon="🔮",
    layout="wide"
)

# Custom CSS
st.markdown("""
<style>
    .causal-arrow {
        font-size: 24px;
        color: #00ff88;
    }
    .metric-card {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        border-radius: 12px;
        padding: 20px;
        border: 1px solid #0f3460;
    }
    .sentiment-positive { color: #00ff88; }
    .sentiment-negative { color: #ff4757; }
    .sentiment-neutral { color: #ffa502; }
</style>
""", unsafe_allow_html=True)

# Title
st.title("🔮 CryptoSentinel")
st.markdown("### Real-Time Causal Intelligence for Crypto Markets")

# Coin selector
coin = st.selectbox(
    "Select Cryptocurrency",
    ["bitcoin", "ethereum", "solana", "dogecoin"],
    format_func=lambda x: x.upper()
)

# Main layout
col1, col2 = st.columns(2)

with col1:
    st.subheader("💰 Price (Real-Time)")
    price_placeholder = st.empty()
    price_chart_placeholder = st.empty()

with col2:
    st.subheader("📊 Sentiment (Real-Time)")
    sentiment_placeholder = st.empty()
    sentiment_gauge_placeholder = st.empty()

# Causal direction
st.subheader("🔗 Causal Direction")
causal_placeholder = st.empty()
gemini_insight_placeholder = st.empty()

# Two-column layout for history and alerts
col3, col4 = st.columns([2, 1])

with col3:
    st.subheader("📈 Causal History (24h)")
    history_chart_placeholder = st.empty()

with col4:
    st.subheader("🚨 Alerts")
    alerts_placeholder = st.empty()

# Recent posts
st.subheader("📝 Recent High-Impact Posts")
posts_placeholder = st.empty()

# Historical analysis
st.subheader("📊 Historical Analysis")
timeframe = st.selectbox("Timeframe", ["1H", "6H", "24H", "7D", "30D"])
historical_chart_placeholder = st.empty()

# Real-time update loop (in production, use websockets)
def update_dashboard():
    """Fetch latest data and update dashboard."""
    # This would connect to Kafka consumer in production
    pass

# Sidebar
with st.sidebar:
    st.header("⚙️ Settings")
    
    auto_refresh = st.checkbox("Auto-refresh", value=True)
    refresh_rate = st.slider("Refresh rate (seconds)", 1, 60, 5)
    
    st.header("📊 Causal Parameters")
    window_size = st.slider("Window size (minutes)", 30, 180, 60)
    max_lag = st.slider("Max lag (minutes)", 5, 30, 10)
    confidence_threshold = st.slider("Confidence threshold", 0.5, 0.99, 0.95)
    
    st.header("🔔 Alert Settings")
    direction_alerts = st.checkbox("Direction change alerts", value=True)
    lag_alerts = st.checkbox("Lag shift alerts", value=True)
    confidence_alerts = st.checkbox("Confidence change alerts", value=True)
```

### Key Visualizations

**1. Causal Direction Arrow**:
```python
def render_causal_arrow(direction: str, lag: int, confidence: float):
    """Render the causal direction visualization."""
    if direction == "SENTIMENT_LEADS":
        arrow = f"📊 SENTIMENT ────[{lag} min]────▶ 💰 PRICE"
        color = "#00ff88"
    elif direction == "PRICE_LEADS":
        arrow = f"💰 PRICE ────[{abs(lag)} min]────▶ 📊 SENTIMENT"
        color = "#ff4757"
    elif direction == "BIDIRECTIONAL":
        arrow = f"📊 SENTIMENT ◀────[{lag} min]────▶ 💰 PRICE"
        color = "#ffa502"
    else:
        arrow = "📊 SENTIMENT ─ ─ ─ ✗ ─ ─ ─ 💰 PRICE"
        color = "#636e72"
    
    st.markdown(f"""
    <div style="text-align: center; padding: 20px; background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%); border-radius: 12px;">
        <h2 style="color: {color}; font-family: monospace;">{arrow}</h2>
        <p>Confidence: {confidence:.1%} | Updated: just now</p>
    </div>
    """, unsafe_allow_html=True)
```

**2. Price-Sentiment Overlay Chart**:
```python
def create_overlay_chart(df: pd.DataFrame):
    """Create dual-axis chart with price and sentiment."""
    fig = go.Figure()
    
    # Price line
    fig.add_trace(go.Scatter(
        x=df['timestamp'],
        y=df['price_usd'],
        name='Price (USD)',
        line=dict(color='#00ff88', width=2),
        yaxis='y'
    ))
    
    # Sentiment line
    fig.add_trace(go.Scatter(
        x=df['timestamp'],
        y=df['avg_sentiment'],
        name='Sentiment Score',
        line=dict(color='#ff6b6b', width=2),
        yaxis='y2'
    ))
    
    # Layout with dual y-axes
    fig.update_layout(
        template='plotly_dark',
        yaxis=dict(title='Price (USD)', side='left'),
        yaxis2=dict(title='Sentiment', side='right', overlaying='y'),
        hovermode='x unified',
        legend=dict(orientation='h', yanchor='bottom', y=1.02)
    )
    
    return fig
```

---

## Technical Implementation

### Project Structure

```
cryptosentinel/
├── README.md
├── requirements.txt
├── docker-compose.yml
├── .env.example
│
├── config/
│   ├── __init__.py
│   ├── settings.py
│   └── kafka_config.py
│
├── producers/
│   ├── __init__.py
│   ├── base_producer.py
│   ├── reddit_producer.py
│   ├── price_producer.py
│   └── news_producer.py
│
├── consumers/
│   ├── __init__.py
│   ├── base_consumer.py
│   ├── enrichment_consumer.py
│   └── causal_consumer.py
│
├── enrichment/
│   ├── __init__.py
│   ├── gemini_sentiment.py
│   └── coin_extractor.py
│
├── causal/
│   ├── __init__.py
│   ├── granger.py
│   ├── transfer_entropy.py
│   ├── rolling_analysis.py
│   └── alerts.py
│
├── dashboard/
│   ├── __init__.py
│   ├── app.py
│   ├── components/
│   │   ├── price_card.py
│   │   ├── sentiment_card.py
│   │   ├── causal_arrow.py
│   │   └── charts.py
│   └── utils/
│       └── kafka_reader.py
│
├── schemas/
│   ├── crypto_price.avsc
│   ├── reddit_post.avsc
│   ├── enriched_post.avsc
│   └── causal_result.avsc
│
├── ksql/
│   ├── create_streams.sql
│   ├── create_tables.sql
│   └── create_joins.sql
│
├── terraform/
│   ├── main.tf
│   ├── confluent.tf
│   ├── gcp.tf
│   └── variables.tf
│
└── tests/
    ├── test_producers.py
    ├── test_causal.py
    └── test_enrichment.py
```

### Requirements

```txt
# Kafka
confluent-kafka==2.3.0
fastavro==1.9.0

# Google Cloud
google-cloud-aiplatform==1.38.0
google-generativeai==0.3.0
google-cloud-bigquery==3.14.0

# Causal Inference
statsmodels==0.14.0
dowhy==0.10.0
econml==0.14.1
scipy==1.11.0

# Data Processing
pandas==2.1.0
numpy==1.26.0
pyarrow==14.0.0

# API Clients
praw==7.7.0  # Reddit
requests==2.31.0
aiohttp==3.9.0

# Dashboard
streamlit==1.29.0
plotly==5.18.0
altair==5.2.0

# Utilities
python-dotenv==1.0.0
pydantic==2.5.0
loguru==0.7.0
tenacity==8.2.0

# Testing
pytest==7.4.0
pytest-asyncio==0.21.0
```

### Core Producer Implementation

```python
# producers/reddit_producer.py
import praw
import json
import time
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from config.settings import Settings
from loguru import logger

class RedditProducer:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.subreddits = ["cryptocurrency", "bitcoin", "ethereum", "solana", "wallstreetbets"]
        
        # Reddit client
        self.reddit = praw.Reddit(
            client_id=settings.REDDIT_CLIENT_ID,
            client_secret=settings.REDDIT_CLIENT_SECRET,
            user_agent="CryptoSentinel/1.0"
        )
        
        # Kafka producer
        self.producer = Producer({
            'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': settings.KAFKA_API_KEY,
            'sasl.password': settings.KAFKA_API_SECRET,
        })
        
        # Schema Registry
        schema_registry = SchemaRegistryClient({
            'url': settings.SCHEMA_REGISTRY_URL,
            'basic.auth.user.info': f"{settings.SR_API_KEY}:{settings.SR_API_SECRET}"
        })
        
        # Load Avro schema
        with open('schemas/reddit_post.avsc', 'r') as f:
            schema_str = f.read()
        
        self.serializer = AvroSerializer(
            schema_registry,
            schema_str,
            lambda post, ctx: post
        )
    
    def delivery_callback(self, err, msg):
        if err:
            logger.error(f"Delivery failed: {err}")
        else:
            logger.debug(f"Delivered to {msg.topic()} [{msg.partition()}]")
    
    def stream_posts(self):
        """Stream new posts from subreddits."""
        logger.info(f"Starting Reddit stream for: {self.subreddits}")
        
        subreddit = self.reddit.subreddit("+".join(self.subreddits))
        
        for submission in subreddit.stream.submissions(skip_existing=True):
            post = {
                "post_id": submission.id,
                "subreddit": submission.subreddit.display_name,
                "title": submission.title,
                "body": submission.selftext[:1000] if submission.selftext else "",
                "score": submission.score,
                "num_comments": submission.num_comments,
                "created_utc": int(submission.created_utc * 1000),
                "author": str(submission.author) if submission.author else "[deleted]",
                "url": submission.url
            }
            
            # Serialize and produce
            value = self.serializer(
                post,
                SerializationContext("social.reddit.raw", MessageField.VALUE)
            )
            
            self.producer.produce(
                topic="social.reddit.raw",
                key=post["post_id"],
                value=value,
                callback=self.delivery_callback
            )
            
            self.producer.poll(0)
            
            logger.info(f"Produced: r/{post['subreddit']} - {post['title'][:50]}...")
    
    def run(self):
        """Run producer with error handling."""
        while True:
            try:
                self.stream_posts()
            except Exception as e:
                logger.error(f"Stream error: {e}")
                time.sleep(30)  # Wait before reconnecting

if __name__ == "__main__":
    settings = Settings()
    producer = RedditProducer(settings)
    producer.run()
```

### Enrichment Consumer Implementation

```python
# consumers/enrichment_consumer.py
import json
from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
import google.genai as genai
from config.settings import Settings
from loguru import logger

class EnrichmentConsumer:
    def __init__(self, settings: Settings):
        self.settings = settings
        
        # Gemini client
        genai.configure(api_key=settings.GEMINI_API_KEY)
        self.model = genai.GenerativeModel("gemini-2.5-flash")
        
        # Kafka consumer
        self.consumer = Consumer({
            'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'enrichment-consumer-group',
            'auto.offset.reset': 'latest',
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': settings.KAFKA_API_KEY,
            'sasl.password': settings.KAFKA_API_SECRET,
        })
        
        # Kafka producer for enriched messages
        self.producer = Producer({
            'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': settings.KAFKA_API_KEY,
            'sasl.password': settings.KAFKA_API_SECRET,
        })
        
        self.consumer.subscribe(['social.reddit.raw', 'news.articles.raw'])
    
    def extract_sentiment(self, post: dict) -> dict:
        """Use Gemini to extract sentiment."""
        prompt = f"""
        Analyze this crypto social media post:
        
        Title: {post.get('title', '')}
        Body: {post.get('body', '')[:500]}
        Subreddit: {post.get('subreddit', 'unknown')}
        
        Return JSON only:
        {{
            "sentiment_label": "POSITIVE|NEGATIVE|NEUTRAL",
            "sentiment_score": <float -1 to 1>,
            "confidence": <float 0 to 1>,
            "mentioned_coins": ["BTC", "ETH", ...],
            "topics": ["price", "adoption", ...]
        }}
        """
        
        try:
            response = self.model.generate_content(prompt)
            # Extract JSON from response
            text = response.text.strip()
            if text.startswith("```"):
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]
            
            return json.loads(text)
        except Exception as e:
            logger.error(f"Gemini error: {e}")
            return {
                "sentiment_label": "NEUTRAL",
                "sentiment_score": 0.0,
                "confidence": 0.0,
                "mentioned_coins": [],
                "topics": []
            }
    
    def process_message(self, msg):
        """Process a single message."""
        topic = msg.topic()
        value = json.loads(msg.value().decode('utf-8'))
        
        # Extract sentiment
        sentiment = self.extract_sentiment(value)
        
        # Enrich message
        enriched = {
            **value,
            "sentiment_score": sentiment["sentiment_score"],
            "sentiment_label": sentiment["sentiment_label"],
            "sentiment_confidence": sentiment["confidence"],
            "mentioned_coins": sentiment["mentioned_coins"],
            "topics": sentiment["topics"],
            "processed_at": int(time.time() * 1000)
        }
        
        # Determine output topic
        output_topic = topic.replace('.raw', '.enriched')
        
        # Produce enriched message
        self.producer.produce(
            topic=output_topic,
            key=msg.key(),
            value=json.dumps(enriched).encode('utf-8')
        )
        
        logger.info(f"Enriched: {value.get('title', '')[:40]}... → {sentiment['sentiment_label']}")
    
    def run(self):
        """Run consumer loop."""
        logger.info("Starting enrichment consumer...")
        
        while True:
            msg = self.consumer.poll(1.0)
            
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue
            
            self.process_message(msg)
            self.producer.poll(0)

if __name__ == "__main__":
    settings = Settings()
    consumer = EnrichmentConsumer(settings)
    consumer.run()
```

---

## Demo Script

### 5-Minute Demo Flow

| Time | Action | Visual |
|------|--------|--------|
| 0:00 | **Hook**: "What if you could see the future of crypto prices?" | Dashboard showing live data |
| 0:30 | **Problem**: "Sentiment and price are correlated, but which causes which?" | Correlation chart |
| 1:00 | **Solution**: "CryptoSentinel uses real-time causal inference" | Architecture diagram |
| 1:30 | **Live Demo**: Show Reddit post → Sentiment extraction → Kafka flow | Terminal + Dashboard |
| 2:30 | **Causal Arrow**: "Right now, sentiment is LEADING price by 12 minutes" | Causal direction visual |
| 3:30 | **Alert Demo**: Simulate causal direction flip | Alert notification |
| 4:00 | **Gemini Insight**: Show natural language explanation | AI-generated insight |
| 4:30 | **Business Value**: "Traders can now act on leading, not lagging, indicators" | Value proposition |
| 5:00 | **Close**: "Real-time causality, powered by Confluent + Google AI" | Logo/team |

### Demo Commands

```bash
# Terminal 1: Start producers
python producers/reddit_producer.py &
python producers/price_producer.py &

# Terminal 2: Start enrichment consumer
python consumers/enrichment_consumer.py &

# Terminal 3: Start causal consumer
python consumers/causal_consumer.py &

# Terminal 4: Start dashboard
streamlit run dashboard/app.py
```

---

## Success Metrics

### Technical Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| End-to-end latency | < 2 seconds | Post → Dashboard |
| Sentiment accuracy | > 85% | Manual validation |
| Granger test validity | p < 0.05 | Statistical significance |
| System uptime | > 99% | Monitoring |

### Demo Metrics

| Metric | Target | Importance |
|--------|--------|------------|
| Real-time feel | Updates every 1-5 sec | Critical for "wow" factor |
| Causal insight clarity | Non-technical understandable | Judges may not be quants |
| Visual polish | Professional, modern UI | First impressions matter |
| Narrative flow | Clear problem → solution → value | Story wins hackathons |

---

## Project Timeline

### Hackathon Sprint (Assuming 48 hours)

| Phase | Hours | Deliverables |
|-------|-------|--------------|
| **Setup** | 0-4 | Confluent Cloud, GCP, APIs configured |
| **Data Pipeline** | 4-12 | Producers running, Kafka flowing |
| **Enrichment** | 12-18 | Gemini sentiment working |
| **Causal Engine** | 18-28 | Granger causality implemented |
| **Dashboard** | 28-38 | Streamlit UI complete |
| **Polish** | 38-44 | Bug fixes, demo prep |
| **Demo Prep** | 44-48 | Script, slides, rehearsal |

---

## Risk Mitigation

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Reddit API rate limit | Medium | High | Implement backoff, cache posts |
| CoinGecko rate limit | Medium | Medium | Use backup API (Finnhub) |
| Gemini API latency | Low | Medium | Batch requests, async processing |
| Confluent quota | Low | High | Monitor usage, have backup cluster |
| Demo network issues | Medium | Critical | Pre-record backup video |

---

## Future Enhancements

### Post-Hackathon Roadmap

1. **More Data Sources**: Twitter/X, Telegram, Discord, on-chain data
2. **Advanced Causal Methods**: Double ML, Synthetic Control, Bayesian Networks
3. **Pump-and-Dump Detection**: Anomaly detection + causal flags
4. **Multi-Coin Causality**: Cross-asset causal networks
5. **Trading Signals**: Integrate with exchange APIs
6. **Mobile App**: Real-time alerts on mobile

---

## Appendix

### A. Environment Variables

```env
# Confluent Cloud
KAFKA_BOOTSTRAP_SERVERS=pkc-xxxxx.us-central1.gcp.confluent.cloud:9092
KAFKA_API_KEY=your-api-key
KAFKA_API_SECRET=your-api-secret
SCHEMA_REGISTRY_URL=https://psrc-xxxxx.us-central1.gcp.confluent.cloud
SR_API_KEY=your-sr-key
SR_API_SECRET=your-sr-secret

# Google Cloud
GCP_PROJECT_ID=your-project-id
GEMINI_API_KEY=your-gemini-key
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json

# Reddit
REDDIT_CLIENT_ID=your-reddit-client-id
REDDIT_CLIENT_SECRET=your-reddit-client-secret

# NewsAPI
NEWSAPI_KEY=your-newsapi-key

# CoinGecko (optional, free tier works without key)
COINGECKO_API_KEY=your-coingecko-key
```

### B. Quick Start Commands

```bash
# Clone and setup
git clone https://github.com/your-repo/cryptosentinel.git
cd cryptosentinel
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Copy and configure environment
cp .env.example .env
# Edit .env with your credentials

# Start all services
docker-compose up -d

# Or run individually
python producers/reddit_producer.py &
python producers/price_producer.py &
python consumers/enrichment_consumer.py &
python consumers/causal_consumer.py &
streamlit run dashboard/app.py
```

---

**Document Version**: 1.0  
**Last Updated**: December 2024  
**Authors**: CryptoSentinel Team


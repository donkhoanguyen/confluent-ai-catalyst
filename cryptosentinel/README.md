# 🔮 CryptoSentinel

**Real-Time Causal Intelligence for Crypto Markets**

CryptoSentinel answers a fundamental question: **Does social sentiment CAUSE price movements, or do price movements CAUSE social sentiment?**

Built with Confluent Kafka, Google Gemini AI, and streaming causal inference.

---

## 📋 Development Process

We use a **parallel development** approach with two independent tracks:

| Track | Focus | Key Files |
|-------|-------|-----------|
| **Track 1: Data Integration** | Kafka, APIs, Enrichment | `producers/`, `consumers/`, `api/` |
| **Track 2: Causal Engine** | Statistical analysis, AI | `causal/`, `agent/`, `dashboard/` |

See **[DEVELOPMENT_PROCESS.md](../DEVELOPMENT_PROCESS.md)** for full details on file ownership, integration points, and workflow.

---

## 🎯 Key Features

- **Real-Time Streaming**: Reddit posts and crypto prices streamed through Confluent Kafka
- **AI Sentiment Analysis**: Google Gemini extracts sentiment from social media in real-time
- **Causal Inference**: Granger causality determines direction of influence (not just correlation!)
- **Live Dashboard**: Cyberpunk-themed Streamlit dashboard with real-time updates
- **API-First Design**: FastAPI backend allows easy frontend swaps

## 🏗️ Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Reddit API    │     │  CoinGecko API  │     │   NewsAPI       │
└────────┬────────┘     └────────┬────────┘     └────────┬────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌──────────────────────────────────────────────────────────────────┐
│                     Python Producers                              │
│   reddit_producer.py    price_producer.py    news_producer.py    │
└────────────────────────────────┬─────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────┐
│                     Confluent Cloud                               │
│   ┌───────────────┐  ┌───────────────┐  ┌───────────────┐       │
│   │ Schema Reg.   │  │ Kafka Topics  │  │    ksqlDB     │       │
│   └───────────────┘  └───────────────┘  └───────────────┘       │
└────────────────────────────────┬─────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────┐
│                     Enrichment Consumer                           │
│   Gemini API → Sentiment Extraction → Enriched Topics            │
└────────────────────────────────┬─────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────┐
│                     Causal Inference Engine                       │
│   Granger Causality → Direction Detection → Alert Generation     │
└────────────────────────────────┬─────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────┐
│                     FastAPI Backend                               │
│   REST Endpoints + WebSocket for Real-Time Updates               │
└────────────────────────────────┬─────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────┐
│                     Streamlit Dashboard                           │
│   Price Charts + Sentiment Gauges + Causal Arrow + Alerts        │
└──────────────────────────────────────────────────────────────────┘
```

---

## 🚀 Quick Start

### 1. Clone and Setup

```bash
cd cryptosentinel
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure Credentials

```bash
cp env.example .env
# Edit .env with your credentials (see Credential Setup below)
```

### 3. Run the Dashboard (Demo Mode)

```bash
# Start just the dashboard with demo data
streamlit run dashboard/app.py
```

The dashboard will show simulated data. Enable "Use demo data" in the sidebar.

### 4. Run Full Pipeline (Production)

```bash
# Terminal 1: Start API server
python -m api.main

# Terminal 2: Start Reddit producer
python -m producers.reddit_producer

# Terminal 3: Start price producer
python -m producers.price_producer

# Terminal 4: Start enrichment consumer
python -m consumers.enrichment_consumer

# Terminal 5: Start dashboard
streamlit run dashboard/app.py
```

---

## 🔑 Credential Setup

### Confluent Cloud (Required)

1. Sign up at [confluent.cloud](https://confluent.cloud) (free trial available)
2. Create a Basic cluster
3. Create topics:
   - `social.reddit.raw`
   - `social.reddit.enriched`
   - `crypto.prices.raw`
   - `causal.results`
4. Go to **Cluster Settings → API Keys → Create Key**
5. Go to **Schema Registry → API credentials**

```env
KAFKA_BOOTSTRAP_SERVERS=pkc-xxxxx.us-central1.gcp.confluent.cloud:9092
KAFKA_API_KEY=your-kafka-api-key
KAFKA_API_SECRET=your-kafka-api-secret
SCHEMA_REGISTRY_URL=https://psrc-xxxxx.us-central1.gcp.confluent.cloud
SCHEMA_REGISTRY_API_KEY=your-sr-api-key
SCHEMA_REGISTRY_API_SECRET=your-sr-api-secret
```

### Google Gemini (AI Studio or Vertex AI)

You can run the LLM pieces in one of two ways:

1. **AI Studio (API key)**: easiest to get started
2. **Vertex AI (GCP IAM)**: supports structured output for more reliable JSON (recommended if you already have GCP set up)

```env
GEMINI_API_KEY=your-gemini-api-key

# Set true to use Vertex AI instead of AI Studio
USE_VERTEX_AI=false

# Required when USE_VERTEX_AI=true
GCP_PROJECT_ID=your-gcp-project-id
GCP_REGION=us-central1
```

### Reddit API (Required)

1. Go to [reddit.com/prefs/apps](https://www.reddit.com/prefs/apps)
2. Click "create another app..."
3. Select "script" type
4. Note the client_id (under app name) and secret

```env
REDDIT_CLIENT_ID=your-reddit-client-id
REDDIT_CLIENT_SECRET=your-reddit-client-secret
```

### CoinGecko API (Optional)

Free tier works without API key (10-50 calls/minute).
Optional: Get free key at [coingecko.com/en/api](https://www.coingecko.com/en/api)

```env
COINGECKO_API_KEY=  # Leave empty for free tier
```

---

## 📁 Project Structure

```
cryptosentinel/
├── README.md
├── requirements.txt
├── env.example
├── .gitignore
│
├── config/
│   ├── __init__.py
│   └── settings.py          # Pydantic settings management
│
├── producers/
│   ├── __init__.py
│   ├── reddit_producer.py   # Stream Reddit posts to Kafka
│   └── price_producer.py    # Stream crypto prices to Kafka
│
├── consumers/
│   ├── __init__.py
│   └── enrichment_consumer.py  # Gemini sentiment enrichment
│
├── causal/
│   ├── __init__.py
│   └── granger.py           # Granger causality implementation
│
├── api/
│   ├── __init__.py
│   └── main.py              # FastAPI backend
│
├── dashboard/
│   ├── __init__.py
│   └── app.py               # Streamlit dashboard
│
└── schemas/
    ├── reddit_post.avsc
    ├── reddit_post_enriched.avsc
    ├── crypto_price.avsc
    └── causal_result.avsc
```

---

## 🎨 Dashboard Preview

The dashboard features:

- **Price Display**: Real-time crypto prices with 24h change
- **Sentiment Gauge**: Bullish/Bearish/Neutral sentiment meter
- **Causal Arrow**: Visual showing direction of causality
- **Confidence Meter**: Statistical confidence in causal relationship
- **Alerts**: Real-time notifications when causal dynamics change
- **Recent Posts**: Sentiment-scored Reddit posts

---

## 🔌 API Endpoints

The FastAPI backend provides these endpoints:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/prices` | GET | Current prices for all coins |
| `/api/prices/{coin_id}` | GET | Price for specific coin |
| `/api/sentiment` | GET | Current sentiment for all coins |
| `/api/sentiment/{coin_id}` | GET | Sentiment for specific coin |
| `/api/causal` | GET | Causal analysis for all coins |
| `/api/causal/{coin_id}` | GET | Causal analysis for specific coin |
| `/api/posts` | GET | Recent Reddit posts |
| `/api/alerts` | GET | Recent causal alerts |
| `/api/dashboard` | GET | Complete dashboard state |
| `/ws` | WebSocket | Real-time updates |

### Switching to a Different Frontend

Since the backend exposes APIs, you can easily build a React/Vue/Angular frontend:

```javascript
// Example: Fetching dashboard state
const response = await fetch('http://localhost:8000/api/dashboard');
const data = await response.json();

// Example: WebSocket for real-time updates
const ws = new WebSocket('ws://localhost:8000/ws');
ws.onmessage = (event) => {
    const update = JSON.parse(event.data);
    console.log('Update:', update);
};
```

---

## 📊 Understanding Causal Direction

CryptoSentinel uses Granger causality to determine:

| Direction | Meaning | Trading Implication |
|-----------|---------|---------------------|
| **SENTIMENT_LEADS** | Reddit buzz predicts price | Act on sentiment signals |
| **PRICE_LEADS** | Price movements cause social buzz | Sentiment is reactive, not predictive |
| **BIDIRECTIONAL** | Both influence each other | Complex dynamics, use caution |
| **NO_CAUSALITY** | No statistical relationship | Sentiment not useful for this coin |

The **lead-lag time** tells you how many minutes in advance the leading indicator moves.

---

## 🧪 Testing

```bash
# Run tests
pytest tests/

# Run with coverage
pytest --cov=. tests/
```

---

## 🚧 Known Limitations

- **Rate Limits**: Free tiers have API limits; implement backoff for production
- **Data Latency**: ~1-5 second delay from Reddit post to dashboard
- **Granger Assumptions**: Requires stationary time series; preprocessing applied
- **Demo Mode**: Simulated data doesn't reflect real market conditions

---

## 🔮 Future Enhancements

- [ ] Twitter/X integration
- [ ] On-chain data analysis
- [ ] Multi-coin causal networks
- [ ] Pump-and-dump detection
- [ ] Mobile app with push notifications
- [ ] Historical backtesting

---

## 📄 License

MIT License - See LICENSE file for details.

---

## 🙏 Acknowledgments

- **Confluent** for Kafka streaming infrastructure
- **Google AI** for Gemini API
- **Reddit** for social data access
- **CoinGecko** for crypto price data

---

Built with ❤️ for the Confluent + Google AI Hackathon


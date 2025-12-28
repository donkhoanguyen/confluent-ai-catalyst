# Causal Discovery Agent

This module implements an autonomous causal discovery agent that generates hypotheses and tests them using existing data sources.

## Key Components

### 1. Asset Registry (`asset_registry.py`)

The **Asset Registry** catalogs all available data sources, variables, and Kafka topics in the system. This grounds hypothesis generation and data source discovery to only use assets that actually exist.

**Available Assets (Cryptocurrency Domain):**

#### Variables:
- **Price Variables** (from `crypto.prices.raw`):
  - `price_usd`: Cryptocurrency price in USD (CONTINUOUS)
  - `volume_24h`: 24-hour trading volume in USD (CONTINUOUS)
  - `price_change_24h_pct`: 24-hour price change percentage / return (CONTINUOUS)
  - `market_cap`: Market capitalization in USD (CONTINUOUS)
  - `coin_id`: Cryptocurrency identifier (CATEGORICAL)

- **Sentiment Variables** (from `social.reddit.enriched`):
  - `avg_sentiment`: Average sentiment score from Reddit posts (-1.0 to 1.0) (CONTINUOUS)
  - `sentiment_score`: Individual post sentiment score (CONTINUOUS)
  - `post_count`: Number of Reddit posts mentioning a coin (DISCRETE)
  - `positive_count`: Number of positive sentiment posts (DISCRETE)
  - `negative_count`: Number of negative sentiment posts (DISCRETE)

#### Data Sources:
- `crypto_prices_kafka`: Kafka topic `crypto.prices.raw`
- `reddit_sentiment_kafka`: Kafka topic `social.reddit.enriched`
- `analytics_joined_kafka`: Kafka topic `analytics.joined`
- `in_memory_datastore`: In-memory DataStore from `api/main.py`

### 2. Hypothesis Generator (`hypothesis_generator.py`)

**Grounded to Registry**: The hypothesis generator now only generates hypotheses using variables that exist in the asset registry. It:
- Lists available variables and data sources in the prompt
- Validates generated hypotheses against the registry
- Maps variables to actual data sources
- Flags variables that are desired but not available

**Example Output:**
```python
Hypothesis(
    cause=Variable(name="avg_sentiment", ...),
    effect=Variable(name="price_change_24h_pct", ...),
    required_data_sources=[DataSource(name="analytics_joined_kafka", ...)],
    ...
)
```

### 3. Data Source Discovery (`data_discovery.py`)

**Grounded to Registry**: Instead of inventing new APIs, data source discovery:
- First checks the registry for exact variable matches
- Uses Gemini to suggest the closest available source if no exact match
- Only returns sources that exist in the registry

### 4. Canonical DataFrame Builder (`dataframe_builder.py`)

The **Canonical DataFrame Builder** assembles a clean, analysis-ready DataFrame from existing data stores with proper schema, joins, and quality checks.

#### Canonical Schema

The canonical DataFrame has the following structure:

| Column | Type | Description |
|--------|------|-------------|
| `timestamp` | datetime64[ns] | Time index (1-minute resolution) |
| `coin_id` | string | Cryptocurrency identifier |
| `price_usd` | float64 | Price in USD |
| `return_pct` | float64 | Price return percentage |
| `volume_24h` | float64 | 24-hour volume |
| `market_cap` | float64 | Market capitalization |
| `avg_sentiment` | float64 | Average sentiment score (-1.0 to 1.0) |
| `post_count` | int64 | Number of posts |
| `positive_count` | int64 | Positive post count |
| `negative_count` | int64 | Negative post count |

#### Data Assembly Process

1. **Input**: Price history and sentiment history from DataStore
2. **Time Alignment**: Resample to 1-minute intervals
   - Price: Forward fill (last known price)
   - Sentiment: Average over 1-minute windows
3. **Join**: Inner join on timestamp
4. **Quality Checks**:
   - Remove rows with all NaN data columns
   - Log missingness statistics
   - Validate value ranges (prices > 0, sentiment in [-1, 1])
5. **Output**: Clean DataFrame ready for causal analysis

#### Usage

```python
from agent.dataframe_builder import CanonicalDataFrameBuilder

builder = CanonicalDataFrameBuilder()

# Build from DataStore
df = builder.build_from_datastore(
    coin_id="bitcoin",
    price_history=list(data_store.price_history["bitcoin"]),
    sentiment_history=list(data_store.sentiment_history["bitcoin"]),
)

# Build for specific hypothesis (includes cause, effect, confounders)
df = builder.build_for_hypothesis(
    hypothesis=hypothesis,
    coin_id="bitcoin",
    price_history=...,
    sentiment_history=...,
)
```

### 5. Orchestrator (`orchestrator.py`)

The orchestrator coordinates the entire causal discovery process:
1. Generate hypotheses (grounded to registry)
2. Discover data sources (from registry)
3. Integrate pipelines via Confluent
4. Collect data
5. **Build canonical DataFrame** (new)
6. Run causal inference tests (using canonical DataFrame)
7. Discover and test confounders
8. Refine hypotheses

## Key Improvements

### Before:
- Hypothesis generator invented variables and APIs freely
- Data sources were hallucinated by LLM
- No single canonical table for analysis
- Data scattered across multiple stores

### After:
- ✅ Hypotheses only use variables that exist in registry
- ✅ Data sources are mapped to actual Kafka topics/stores
- ✅ Canonical DataFrame provides single source of truth
- ✅ Quality checks ensure data integrity
- ✅ Proper time alignment and joins

## Assumptions

1. **Time Resolution**: Data is resampled to 1-minute intervals for consistency
2. **Data Availability**: Assumes price and sentiment data are available in DataStore or Kafka topics
3. **Variable Mapping**: Variable names must match between registry and actual data
4. **Missing Data**: Rows with all NaN data columns are removed; partial missingness is logged but kept

## Future Enhancements

- Support for additional data sources (news articles, on-chain metrics)
- Automatic schema inference from Kafka topics
- Real-time DataFrame updates from Kafka streams
- Support for multiple coins in a single DataFrame
- Advanced quality checks (outlier detection, stationarity tests)


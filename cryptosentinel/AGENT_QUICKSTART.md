# Autonomous Causal Discovery Agent - Quick Start Guide

## Prerequisites

1. **Python 3.10+** installed
2. **Confluent Cloud** account (free tier works)
3. **Google Gemini API key** from [AI Studio](https://aistudio.google.com/app/apikey)
4. **Reddit API credentials** (optional, for crypto demo)

## Step 1: Install Dependencies

```bash
cd cryptosentinel
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

**Note**: Some causal inference libraries may take a few minutes to install.

## Step 2: Configure Environment

Copy the example environment file and fill in your credentials:

```bash
cp env.example .env
```

Edit `.env` and add your credentials:

```env
# Required: Confluent Cloud
KAFKA_BOOTSTRAP_SERVERS=pkc-xxxxx.us-central1.gcp.confluent.cloud:9092
KAFKA_API_KEY=your-kafka-api-key
KAFKA_API_SECRET=your-kafka-api-secret
SCHEMA_REGISTRY_URL=https://psrc-xxxxx.us-central1.gcp.confluent.cloud
SCHEMA_REGISTRY_API_KEY=your-sr-api-key
SCHEMA_REGISTRY_API_SECRET=your-sr-api-secret

# Required: Google Gemini
GEMINI_API_KEY=your-gemini-api-key

# Optional: Agent Configuration
AGENT_MODE=true
MCP_SERVER_URL=  # Leave empty to use Admin API
MAX_HYPOTHESES=10
DISCOVERY_TIMEOUT=3600
```

## Step 3: Start the Services

### Option A: Start Everything (Recommended)

```bash
# Terminal 1: Start API server
python -m uvicorn api.main:app --host 0.0.0.0 --port 8000

# Terminal 2: Start Dashboard
streamlit run dashboard/app.py --server.port 8501
```

### Option B: Use the Pipeline Script

```bash
./run_pipeline.sh
```

## Step 4: Using the Agent

### Method 1: Via Dashboard (Easiest)

1. Open your browser to `http://localhost:8501`
2. In the sidebar, select **"Agent Discovery"** from the navigation dropdown
3. Enter:
   - **Domain**: `cryptocurrency` (or any domain you want to explore)
   - **Research Question**: `Does social sentiment cause price movements?`
4. Click **"🚀 Start Discovery"**
5. Watch the agent:
   - Generate hypotheses
   - Discover data sources
   - Run causal tests
   - Identify confounders

### Method 2: Via API

#### Start a Discovery

```bash
curl -X POST "http://localhost:8000/api/agent/discover" \
  -H "Content-Type: application/json" \
  -d '{
    "domain": "cryptocurrency",
    "query": "Does social sentiment cause price movements?",
    "config": {}
  }'
```

#### Check Results

```bash
curl "http://localhost:8000/api/agent/results?domain=cryptocurrency"
```

#### List Hypotheses

```bash
curl "http://localhost:8000/api/agent/hypotheses?domain=cryptocurrency"
```

### Method 3: Python Script

Create a file `test_agent.py`:

```python
from agent.orchestrator import CausalDiscoveryAgent
from config.settings import get_settings

# Initialize agent
settings = get_settings()
agent = CausalDiscoveryAgent(settings)

# Start discovery
result = agent.discover(
    domain="cryptocurrency",
    query="Does social sentiment cause price movements?",
    config={"thread_id": "test_discovery_1"}
)

# Print results
print(f"Status: {result.status}")
print(f"Hypotheses: {len(result.hypotheses)}")
print(f"Results: {len(result.results)}")

for i, hyp in enumerate(result.hypotheses):
    print(f"\nHypothesis {i+1}:")
    print(f"  {hyp.cause.name} → {hyp.effect.name}")
    print(f"  Mechanism: {hyp.mechanism}")
    print(f"  Confidence: {hyp.confidence:.2%}")

for i, res in enumerate(result.results):
    if res.is_significant:
        print(f"\n✅ Significant Result {i+1}:")
        print(f"  Method: {res.method.value}")
        print(f"  {res.hypothesis.cause.name} → {res.hypothesis.effect.name}")
        print(f"  P-value: {res.p_value:.4f}")
        print(f"  Confidence: {res.confidence:.2%}")
```

Run it:

```bash
python test_agent.py
```

## Example Use Cases

### 1. Crypto Market Analysis

```python
result = agent.discover(
    domain="cryptocurrency",
    query="What factors cause Bitcoin price volatility?",
)
```

The agent will:
- Generate hypotheses about volatility causes
- Discover data sources (trading volume, news, on-chain metrics)
- Test causal relationships
- Identify confounders (market cap, time of day, etc.)

### 2. Social Media Impact

```python
result = agent.discover(
    domain="social_media",
    query="Does Twitter activity cause stock price movements?",
)
```

### 3. Healthcare Analytics

```python
result = agent.discover(
    domain="healthcare",
    query="Does medication adherence cause improved outcomes?",
)
```

## Understanding the Results

### Hypothesis Structure

```python
hypothesis = {
    "cause": Variable(name="sentiment", ...),
    "effect": Variable(name="price", ...),
    "mechanism": "Social sentiment influences trader behavior...",
    "confidence": 0.75,  # Initial confidence from Gemini
    "suggested_methods": [CausalMethod.GRANGER, CausalMethod.TRANSFER_ENTROPY],
    "potential_confounders": [Variable(name="trading_volume", ...)],
    "required_data_sources": [...]
}
```

### Causal Result Structure

```python
result = {
    "hypothesis": Hypothesis(...),
    "method": CausalMethod.GRANGER,
    "is_significant": True,  # p < 0.05
    "confidence": 0.92,
    "p_value": 0.03,
    "effect_size": 2.45,
    "direction": "sentiment → price",
    "lead_lag": 12,  # minutes
    "confounders_significant": [...]
}
```

## Agent Discovery Flow

The agent follows this iterative process:

1. **Hypothesis Generation**: Gemini generates causal hypotheses
2. **Data Source Discovery**: Finds APIs/data sources for variables
3. **Pipeline Integration**: Creates Kafka topics and schemas via Confluent
4. **Data Collection**: Producers fetch data from sources
5. **Causal Testing**: Runs multiple causal inference methods
6. **Confounder Discovery**: Identifies potential confounders
7. **Confounder Testing**: Tests for confounding effects
8. **Refinement**: Refines hypotheses based on results
9. **Evaluation**: Determines if discovery is complete

## Troubleshooting

### "LangGraph not available"

If you see this warning, the agent will use sequential execution instead of LangGraph. Install langgraph:

```bash
pip install langgraph
```

### "No hypotheses generated"

- Check your Gemini API key is correct
- Verify you have API quota remaining
- Check logs for Gemini API errors

### "Data source discovery failed"

- The agent uses Gemini to suggest APIs - it may not find sources for all variables
- You can manually add data sources to the domain registry

### "Causal test failed"

- Ensure you have enough data (minimum 30-50 samples)
- Check that data sources are providing data
- Some methods require specific data characteristics (time series, binary treatment, etc.)

## Advanced Configuration

### Enable Agent Mode by Default

In `.env`:

```env
AGENT_MODE=true
```

### Configure MCP Server

If you have a Confluent MCP server:

```env
MCP_SERVER_URL=http://localhost:8080/mcp
```

Otherwise, the agent will use Confluent Admin API.

### Limit Concurrent Hypotheses

```env
MAX_HYPOTHESES=5  # Reduce if hitting rate limits
```

## Next Steps

1. **Explore Different Domains**: Try the agent on different domains
2. **Add Custom Data Sources**: Extend the data discovery to include your own APIs
3. **Customize Causal Methods**: Add domain-specific causal inference methods
4. **Monitor Discovery**: Use the dashboard to watch the agent work in real-time

## API Documentation

Once the API is running, visit:

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

## Support

For issues or questions:
1. Check the logs in the terminal
2. Review the agent's state in the dashboard
3. Check that all required services are running

Happy discovering! 🚀


# 🚀 CausalLab Deployment Guide

Complete guide for deploying CausalLab (AI-Powered Causal Discovery Platform) to production.

---

## 📋 Table of Contents

1. [Prerequisites](#prerequisites)
2. [Local Development Setup](#local-development-setup)
3. [Cloud Deployment Options](#cloud-deployment-options)
4. [Environment Configuration](#environment-configuration)
5. [Service Setup](#service-setup)
6. [Production Considerations](#production-considerations)
7. [Troubleshooting](#troubleshooting)

---

## Prerequisites

### Required Accounts & Services

1. **Confluent Cloud** (Free tier available)
   - Sign up at [confluent.cloud](https://confluent.cloud)
   - Trial code: `CONFLUENTDEV1`

2. **Google Cloud Platform** (Free tier available)
   - Sign up at [cloud.google.com](https://cloud.google.com)
   - Enable Vertex AI API (or use AI Studio with API key)

3. **Python 3.10+**
   - Download from [python.org](https://www.python.org/downloads/)

### Optional Services

- **Reddit API** (for social sentiment data)
- **CoinGecko API** (for cryptocurrency prices)
- **News APIs** (for news sentiment)

---

## Local Development Setup

### Step 1: Clone Repository

```bash
git clone <repository-url>
cd confluent_ai_catalyst/cryptosentinel
```

### Step 2: Create Virtual Environment

```bash
python -m venv venv

# On macOS/Linux:
source venv/bin/activate

# On Windows:
venv\Scripts\activate
```

### Step 3: Install Dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

**Note**: Some causal inference libraries may take 5-10 minutes to install.

### Step 4: Configure Environment

```bash
cp env.example .env
```

Edit `.env` with your credentials (see [Environment Configuration](#environment-configuration)).

### Step 5: Start Services

#### Option A: Quick Start (Dashboard Only)

```bash
# Terminal 1: Start API server
python -m uvicorn api.main:app --host 0.0.0.0 --port 8000

# Terminal 2: Start dashboard
streamlit run dashboard/app.py --server.port 8501
```

Access:
- Dashboard: http://localhost:8501
- API Docs: http://localhost:8000/docs

#### Option B: Full Pipeline

```bash
# Use the pipeline script
chmod +x run_pipeline.sh
./run_pipeline.sh
```

Or manually start each service:

```bash
# Terminal 1: API Server
python -m uvicorn api.main:app --host 0.0.0.0 --port 8000

# Terminal 2: Price Producer
python -m producers.price_producer

# Terminal 3: Reddit Producer
python -m producers.reddit_producer

# Terminal 4: Enrichment Consumer
python -m consumers.enrichment_consumer

# Terminal 5: Dashboard
streamlit run dashboard/app.py --server.port 8501
```

---

## Environment Configuration

### Confluent Cloud Setup

1. **Create Cluster**
   - Go to [confluent.cloud](https://confluent.cloud)
   - Create a Basic cluster (free tier)
   - Select region (e.g., `us-central1`)

2. **Create Topics**
   - `social.reddit.raw`
   - `social.reddit.enriched`
   - `crypto.prices.raw`
   - `causal.results`

3. **Get API Credentials**
   - Go to **Cluster Settings → API Keys → Create Key**
   - Save `KAFKA_API_KEY` and `KAFKA_API_SECRET`
   - Note `KAFKA_BOOTSTRAP_SERVERS` (e.g., `pkc-xxxxx.us-central1.gcp.confluent.cloud:9092`)

4. **Schema Registry**
   - Go to **Schema Registry → API credentials**
   - Save `SCHEMA_REGISTRY_API_KEY` and `SCHEMA_REGISTRY_API_SECRET`
   - Note `SCHEMA_REGISTRY_URL` (e.g., `https://psrc-xxxxx.us-central1.gcp.confluent.cloud`)

5. **Register Schemas**
   ```bash
   # Schemas are in schemas/ directory
   # They will be auto-registered when producers start
   ```

**Add to `.env`**:
```env
KAFKA_BOOTSTRAP_SERVERS=pkc-xxxxx.us-central1.gcp.confluent.cloud:9092
KAFKA_API_KEY=your-kafka-api-key
KAFKA_API_SECRET=your-kafka-api-secret
SCHEMA_REGISTRY_URL=https://psrc-xxxxx.us-central1.gcp.confluent.cloud
SCHEMA_REGISTRY_API_KEY=your-sr-api-key
SCHEMA_REGISTRY_API_SECRET=your-sr-api-secret
```

### Google AI / Vertex AI Setup

#### Option 1: Vertex AI (Recommended)

1. **Enable Vertex AI API**
   ```bash
   gcloud services enable aiplatform.googleapis.com
   ```

2. **Set Up Authentication**
   ```bash
   # Option A: Application Default Credentials
   gcloud auth application-default login
   
   # Option B: Service Account
   gcloud iam service-accounts create causal-lab-sa
   gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
     --member="serviceAccount:causal-lab-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
     --role="roles/aiplatform.user"
   gcloud iam service-accounts keys create key.json \
     --iam-account=causal-lab-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com
   export GOOGLE_APPLICATION_CREDENTIALS=key.json
   ```

3. **Get Project ID**
   ```bash
   gcloud config get-value project
   ```

**Add to `.env`**:
```env
GCP_PROJECT_ID=your-gcp-project-id
GCP_REGION=us-central1
USE_VERTEX_AI=true
```

#### Option 2: AI Studio (Simpler, but less reliable)

1. **Get API Key**
   - Go to [aistudio.google.com/app/apikey](https://aistudio.google.com/app/apikey)
   - Create API key

**Add to `.env`**:
```env
GEMINI_API_KEY=your-gemini-api-key
USE_VERTEX_AI=false
```

### Reddit API Setup (Optional)

1. **Create App**
   - Go to [reddit.com/prefs/apps](https://www.reddit.com/prefs/apps)
   - Click "create another app..."
   - Select "script" type
   - Note `client_id` (under app name) and `secret`

**Add to `.env`**:
```env
REDDIT_CLIENT_ID=your-reddit-client-id
REDDIT_CLIENT_SECRET=your-reddit-client-secret
REDDIT_USER_AGENT=CausalLab/1.0
```

### CoinGecko API (Optional)

Free tier works without API key (10-50 calls/minute).

Optional: Get free key from [coingecko.com/en/api](https://www.coingecko.com/en/api)

**Add to `.env`**:
```env
COINGECKO_API_KEY=  # Leave empty for free tier
```

---

## Cloud Deployment Options

### Option 1: Streamlit Cloud (Easiest)

**Best for**: Quick demos and prototypes

1. **Push to GitHub**
   ```bash
   git add .
   git commit -m "Initial commit"
   git push origin main
   ```

2. **Deploy to Streamlit Cloud**
   - Go to [share.streamlit.io](https://share.streamlit.io)
   - Sign in with GitHub
   - Click "New app"
   - Select repository
   - Set main file: `cryptosentinel/dashboard/app.py`
   - Add environment variables in "Advanced settings"

3. **Configure Environment Variables**
   - Add all variables from `.env` to Streamlit Cloud settings
   - **Important**: Don't commit `.env` to GitHub!

**Limitations**:
- API server must run separately (or use Streamlit's built-in FastAPI)
- Limited to Streamlit's free tier resources
- No persistent storage

### Option 2: Google Cloud Run (Recommended)

**Best for**: Production deployments

1. **Create Dockerfile**
   ```dockerfile
   FROM python:3.10-slim
   
   WORKDIR /app
   
   COPY requirements.txt .
   RUN pip install --no-cache-dir -r requirements.txt
   
   COPY . .
   
   # Expose ports
   EXPOSE 8000 8501
   
   # Start both API and Streamlit
   CMD python -m uvicorn api.main:app --host 0.0.0.0 --port 8000 & \
       streamlit run dashboard/app.py --server.port 8501 --server.address 0.0.0.0
   ```

2. **Build and Deploy**
   ```bash
   # Build image
   gcloud builds submit --tag gcr.io/YOUR_PROJECT_ID/causal-lab
   
   # Deploy to Cloud Run
   gcloud run deploy causal-lab \
     --image gcr.io/YOUR_PROJECT_ID/causal-lab \
     --platform managed \
     --region us-central1 \
     --allow-unauthenticated \
     --set-env-vars="KAFKA_BOOTSTRAP_SERVERS=..." \
     --set-env-vars="KAFKA_API_KEY=..." \
     # ... add all env vars
   ```

3. **Set Up Secrets** (Recommended)
   ```bash
   # Store secrets in Secret Manager
   echo -n "your-secret" | gcloud secrets create kafka-api-secret --data-file=-
   
   # Reference in Cloud Run
   gcloud run services update causal-lab \
     --update-secrets=KAFKA_API_SECRET=kafka-api-secret:latest
   ```

### Option 3: Docker Compose (Local/Dev)

1. **Create `docker-compose.yml`**
   ```yaml
   version: '3.8'
   
   services:
     api:
       build: .
       ports:
         - "8000:8000"
       environment:
         - KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_SERVERS}
         # ... add all env vars
       command: python -m uvicorn api.main:app --host 0.0.0.0 --port 8000
   
     dashboard:
       build: .
       ports:
         - "8501:8501"
       environment:
         - API_URL=http://api:8000
         # ... add all env vars
       command: streamlit run dashboard/app.py --server.port 8501 --server.address 0.0.0.0
       depends_on:
         - api
   ```

2. **Run**
   ```bash
   docker-compose up -d
   ```

### Option 4: Kubernetes (Advanced)

See [kubernetes/](kubernetes/) directory for manifests (if created).

---

## Service Setup

### Starting Individual Services

#### API Server
```bash
python -m uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
```

#### Dashboard
```bash
streamlit run dashboard/app.py --server.port 8501
```

#### Producers

**Price Producer**:
```bash
python -m producers.price_producer
```

**Reddit Producer**:
```bash
python -m producers.reddit_producer
```

**Unified Collector** (all sources):
```bash
python -m producers.unified_collector
```

#### Consumers

**Enrichment Consumer**:
```bash
python -m consumers.enrichment_consumer
```

### Using Systemd (Linux)

Create service files:

**`/etc/systemd/system/causal-lab-api.service`**:
```ini
[Unit]
Description=CausalLab API Server
After=network.target

[Service]
Type=simple
User=your-user
WorkingDirectory=/path/to/cryptosentinel
Environment="PATH=/path/to/venv/bin"
ExecStart=/path/to/venv/bin/python -m uvicorn api.main:app --host 0.0.0.0 --port 8000
Restart=always

[Install]
WantedBy=multi-user.target
```

Enable and start:
```bash
sudo systemctl enable causal-lab-api
sudo systemctl start causal-lab-api
```

---

## Production Considerations

### Security

1. **Never commit `.env` files**
   - Add to `.gitignore`
   - Use environment variables or secret managers

2. **Use Secret Managers**
   - Google Secret Manager
   - AWS Secrets Manager
   - HashiCorp Vault

3. **API Authentication**
   - Add API keys or OAuth to FastAPI endpoints
   - Use middleware for rate limiting

4. **Network Security**
   - Use VPN or private networks for Kafka
   - Enable TLS for Kafka connections
   - Use HTTPS for API endpoints

### Performance

1. **Scaling**
   - Use load balancers for API servers
   - Scale consumers horizontally
   - Use Kafka consumer groups

2. **Caching**
   - Add Redis for caching causal results
   - Cache API responses

3. **Database**
   - Replace in-memory DataStore with PostgreSQL/Redis
   - Use time-series databases for historical data

4. **Monitoring**
   - Add Prometheus metrics
   - Use Grafana dashboards
   - Set up alerting

### Data Persistence

1. **Replace In-Memory Store**
   ```python
   # Current: In-memory DataStore
   # Production: Use PostgreSQL or Redis
   from sqlalchemy import create_engine
   engine = create_engine('postgresql://...')
   ```

2. **Backup Strategy**
   - Regular backups of causal results
   - Version control for hypotheses
   - Archive historical data

### Error Handling

1. **Retry Logic**
   - Add retries for API calls
   - Handle Kafka connection failures
   - Graceful degradation

2. **Logging**
   - Use structured logging (JSON)
   - Centralized log aggregation
   - Error tracking (Sentry, etc.)

---

## Troubleshooting

### Common Issues

#### 1. "Cannot connect to Kafka"

**Symptoms**: `ConnectionError` or `TimeoutException`

**Solutions**:
- Check `KAFKA_BOOTSTRAP_SERVERS` is correct
- Verify API keys are valid
- Check firewall/network rules
- Ensure cluster is running

#### 2. "Schema Registry error"

**Symptoms**: `401 Unauthorized` or schema not found

**Solutions**:
- Verify `SCHEMA_REGISTRY_API_KEY` and `SCHEMA_REGISTRY_API_SECRET`
- Check `SCHEMA_REGISTRY_URL` is correct
- Ensure schemas are registered

#### 3. "Gemini API error"

**Symptoms**: `Permission denied` or `API key invalid`

**Solutions**:
- If using Vertex AI: Check IAM permissions
- If using AI Studio: Verify `GEMINI_API_KEY` is correct
- Check API quotas/limits

#### 4. "Dashboard shows no data"

**Symptoms**: Empty dashboard, "API server not running"

**Solutions**:
- Ensure API server is running on port 8000
- Check `API_URL` in dashboard config
- Verify CORS settings in FastAPI
- Check browser console for errors

#### 5. "Import errors"

**Symptoms**: `ModuleNotFoundError` or `ImportError`

**Solutions**:
- Ensure virtual environment is activated
- Run `pip install -r requirements.txt`
- Check Python version (3.10+)
- Verify all dependencies installed

### Debug Mode

Enable debug logging:

```bash
export LOG_LEVEL=DEBUG
python -m uvicorn api.main:app --log-level debug
```

### Health Checks

Check API health:
```bash
curl http://localhost:8000/
```

Check dashboard:
```bash
curl http://localhost:8501
```

---

## Next Steps

1. **Load Demo Data**: See [README.md](README.md) for loading CSV data
2. **Run First Discovery**: Use the dashboard to start a causal discovery
3. **Customize for Your Domain**: Modify asset registry and prompts
4. **Scale Up**: Deploy to production with proper infrastructure

---

## Support

- **Documentation**: See [README.md](README.md)
- **Issues**: [GitHub Issues](https://github.com/your-repo/issues)
- **Questions**: [Discussions](https://github.com/your-repo/discussions)

---

**Happy Deploying! 🚀**


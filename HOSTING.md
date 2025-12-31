# 🌐 CausalLab Hosting Guide

Complete guide for hosting frontend, backend, and background services.

---

## 📋 Architecture Overview

```
┌─────────────────┐
│   Frontend      │  Streamlit Dashboard (Port 8501)
│   (Streamlit)   │  → Connects to Backend API
└────────┬────────┘
         │ HTTP Requests
         ▼
┌─────────────────┐
│   Backend       │  FastAPI Server (Port 8000)
│   (FastAPI)     │  → Serves REST API + WebSocket
└────────┬────────┘
         │
         ├──→ Confluent Kafka (Data Streaming)
         ├──→ Google Gemini AI (LLM)
         └──→ Data Store (In-Memory or Database)
         
┌─────────────────┐
│   Background    │  Producers & Consumers
│   Services       │  → Run separately or as jobs
└─────────────────┘
```

---

## 🎯 Hosting Strategy

### Option 1: Separate Hosting (Recommended for Production)

**Frontend**: Streamlit Cloud (Free)  
**Backend**: Google Cloud Run / Railway / Render  
**Background Services**: Same as backend or separate workers

**Pros**:
- Independent scaling
- Better resource allocation
- Easier debugging
- Cost-effective

**Cons**:
- Need to configure CORS
- Two URLs to manage

---

### Option 2: Combined Hosting (Easiest for Demo)

**Everything**: Google Cloud Run / Railway / Render

**Pros**:
- Single deployment
- One URL
- Simpler setup

**Cons**:
- Less flexible scaling
- Higher resource usage

---

## 🚀 Frontend Hosting (Streamlit Dashboard)

### Option 1: Streamlit Cloud (Recommended - Free)

**Best for**: Demos, prototypes, quick deployments

#### Setup Steps:

1. **Push to GitHub**
   ```bash
   git add .
   git commit -m "Deploy to Streamlit Cloud"
   git push origin main
   ```

2. **Deploy to Streamlit Cloud**
   - Go to [share.streamlit.io](https://share.streamlit.io)
   - Sign in with GitHub
   - Click "New app"
   - Select your repository
   - Set configuration:
     - **Main file**: `cryptosentinel/dashboard/app.py`
     - **Branch**: `main`
     - **Python version**: `3.10`

3. **Configure Environment Variables**
   - Go to app settings → "Secrets"
   - Add all variables from `.env`:
     ```toml
     KAFKA_BOOTSTRAP_SERVERS = "pkc-xxxxx.us-central1.gcp.confluent.cloud:9092"
     KAFKA_API_KEY = "your-key"
     KAFKA_API_SECRET = "your-secret"
     # ... add all other variables
     ```

4. **Update API URL**
   - The dashboard needs to know where your backend is
   - Update `dashboard/app.py`:
     ```python
     # Change from localhost to your backend URL
     API_URL = os.getenv("API_URL", "https://your-backend-url.run.app")
     api = APIClient(base_url=API_URL)
     ```

**Result**: Your dashboard will be live at `https://your-app.streamlit.app`

---

### Option 2: Google Cloud Run (Frontend Only)

**Best for**: Production, custom domains, more control

1. **Create Dockerfile for Frontend**
   ```dockerfile
   FROM python:3.10-slim
   
   WORKDIR /app
   
   COPY requirements.txt .
   RUN pip install --no-cache-dir -r requirements.txt
   
   COPY cryptosentinel/dashboard ./dashboard
   COPY cryptosentinel/config ./config
   
   # Set API URL via environment variable
   ENV API_URL=https://your-backend-url.run.app
   
   EXPOSE 8501
   
   CMD streamlit run dashboard/app.py --server.port 8501 --server.address 0.0.0.0
   ```

2. **Deploy**
   ```bash
   gcloud builds submit --tag gcr.io/YOUR_PROJECT_ID/causal-lab-frontend
   
   gcloud run deploy causal-lab-frontend \
     --image gcr.io/YOUR_PROJECT_ID/causal-lab-frontend \
     --platform managed \
     --region us-central1 \
     --allow-unauthenticated \
     --port 8501 \
     --set-env-vars="API_URL=https://your-backend-url.run.app"
   ```

---

### Option 3: Railway (Frontend)

**Best for**: Quick deployment, automatic HTTPS

1. **Connect GitHub repo**
2. **Set root directory**: `cryptosentinel`
3. **Set start command**: `streamlit run dashboard/app.py --server.port $PORT`
4. **Add environment variables** (including `API_URL`)

---

## 🔧 Backend Hosting (FastAPI)

### Option 1: Google Cloud Run (Recommended)

**Best for**: Production, auto-scaling, pay-per-use

#### Setup Steps:

1. **Create Dockerfile**
   ```dockerfile
   FROM python:3.10-slim
   
   WORKDIR /app
   
   COPY requirements.txt .
   RUN pip install --no-cache-dir -r requirements.txt
   
   COPY cryptosentinel ./cryptosentinel
   
   EXPOSE 8000
   
   CMD python -m uvicorn cryptosentinel.api.main:app --host 0.0.0.0 --port 8000
   ```

2. **Build and Deploy**
   ```bash
   # Build
   gcloud builds submit --tag gcr.io/YOUR_PROJECT_ID/causal-lab-backend
   
   # Deploy
   gcloud run deploy causal-lab-backend \
     --image gcr.io/YOUR_PROJECT_ID/causal-lab-backend \
     --platform managed \
     --region us-central1 \
     --allow-unauthenticated \
     --port 8000 \
     --memory 2Gi \
     --cpu 2 \
     --set-env-vars="KAFKA_BOOTSTRAP_SERVERS=..." \
     --set-env-vars="KAFKA_API_KEY=..." \
     # ... add all env vars
   ```

3. **Use Secret Manager** (Recommended)
   ```bash
   # Store secrets
   echo -n "your-secret" | gcloud secrets create kafka-api-secret --data-file=-
   
   # Reference in Cloud Run
   gcloud run services update causal-lab-backend \
     --update-secrets=KAFKA_API_SECRET=kafka-api-secret:latest
   ```

**Result**: Backend at `https://causal-lab-backend-xxxxx.run.app`

---

### Option 2: Railway

**Best for**: Simple deployment, automatic HTTPS

1. **Connect GitHub repo**
2. **Set root directory**: `cryptosentinel`
3. **Set start command**: `python -m uvicorn api.main:app --host 0.0.0.0 --port $PORT`
4. **Add environment variables**
5. **Deploy**

**Result**: Backend at `https://your-app.railway.app`

---

### Option 3: Render

**Best for**: Free tier, easy setup

1. **Create new Web Service**
2. **Connect GitHub repo**
3. **Settings**:
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `python -m uvicorn api.main:app --host 0.0.0.0 --port $PORT`
4. **Add environment variables**
5. **Deploy**

**Result**: Backend at `https://your-app.onrender.com`

---

### Option 4: Fly.io

**Best for**: Global edge deployment

1. **Install Fly CLI**: `curl -L https://fly.io/install.sh | sh`
2. **Create `fly.toml`**:
   ```toml
   app = "causal-lab-backend"
   primary_region = "iad"
   
   [build]
     dockerfile = "Dockerfile"
   
   [[services]]
     internal_port = 8000
     protocol = "tcp"
   
     [[services.ports]]
       port = 80
       handlers = ["http"]
   
     [[services.ports]]
       port = 443
       handlers = ["tls", "http"]
   ```
3. **Deploy**: `fly deploy`

---

## 🔗 Connecting Frontend to Backend

### Update Dashboard Configuration

The dashboard needs to know where the backend is. Update `cryptosentinel/dashboard/app.py`:

```python
import os

# Get API URL from environment variable
API_URL = os.getenv("API_URL", "http://localhost:8000")

class APIClient:
    def __init__(self, base_url: str = API_URL):
        self.base_url = base_url
        # ... rest of code
```

### Configure CORS in Backend

Update `cryptosentinel/api/main.py`:

```python
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://your-frontend.streamlit.app",  # Streamlit Cloud
        "https://your-frontend.run.app",         # Cloud Run
        "http://localhost:8501",                 # Local dev
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
```

---

## 🔄 Background Services (Producers & Consumers)

### Option 1: Same Container as Backend (Simple)

Run producers/consumers in the same Cloud Run service:

```dockerfile
CMD python -m uvicorn api.main:app --host 0.0.0.0 --port 8000 & \
    python -m producers.price_producer & \
    python -m consumers.enrichment_consumer
```

**Pros**: Simple, one deployment  
**Cons**: Less scalable, shared resources

---

### Option 2: Separate Cloud Run Services (Recommended)

Deploy each as separate service:

```bash
# Price Producer
gcloud run deploy causal-lab-price-producer \
  --image gcr.io/YOUR_PROJECT_ID/causal-lab-workers \
  --command "python" \
  --args "-m,producers.price_producer" \
  --no-allow-unauthenticated

# Enrichment Consumer
gcloud run deploy causal-lab-enrichment-consumer \
  --image gcr.io/YOUR_PROJECT_ID/causal-lab-workers \
  --command "python" \
  --args "-m,consumers.enrichment_consumer" \
  --no-allow-unauthenticated
```

**Pros**: Independent scaling, better resource management  
**Cons**: More deployments to manage

---

### Option 3: Cloud Scheduler + Cloud Run Jobs

For periodic tasks:

```bash
# Create Cloud Run Job
gcloud run jobs create causal-lab-data-collection \
  --image gcr.io/YOUR_PROJECT_ID/causal-lab-workers \
  --command "python" \
  --args "-m,producers.unified_collector" \
  --region us-central1

# Schedule it
gcloud scheduler jobs create http causal-lab-daily \
  --schedule="0 */6 * * *" \
  --uri="https://us-central1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/YOUR_PROJECT/jobs/causal-lab-data-collection:run" \
  --http-method=POST \
  --oauth-service-account-email=YOUR_SERVICE_ACCOUNT@YOUR_PROJECT.iam.gserviceaccount.com
```

---

## 📊 Recommended Hosting Setup

### For Hackathon Demo (Quick & Free)

```
Frontend:  Streamlit Cloud (Free)
Backend:   Railway / Render (Free tier)
Services:  Run locally or same as backend
```

**Total Cost**: $0/month  
**Setup Time**: 15 minutes

---

### For Production (Scalable)

```
Frontend:  Streamlit Cloud or Cloud Run
Backend:   Google Cloud Run (Auto-scaling)
Services:  Cloud Run Jobs or separate Cloud Run services
Database:  Cloud SQL or Redis (replace in-memory store)
```

**Total Cost**: ~$20-50/month (depending on usage)  
**Setup Time**: 1-2 hours

---

## 🔐 Security Considerations

### 1. Environment Variables

**Never commit `.env` files!**

Use:
- **Streamlit Cloud**: Secrets tab
- **Cloud Run**: Environment variables or Secret Manager
- **Railway/Render**: Environment variables in dashboard

### 2. CORS Configuration

Only allow your frontend domain:
```python
allow_origins=[
    "https://your-frontend.streamlit.app",
    # Don't use "*" in production!
]
```

### 3. API Authentication

Add API keys or OAuth:
```python
from fastapi import Header, HTTPException

async def verify_api_key(x_api_key: str = Header(...)):
    if x_api_key != os.getenv("API_KEY"):
        raise HTTPException(status_code=401)
```

### 4. HTTPS Only

All services should use HTTPS:
- Streamlit Cloud: Automatic
- Cloud Run: Automatic
- Railway/Render: Automatic

---

## 🧪 Testing Your Deployment

### 1. Test Backend

```bash
# Health check
curl https://your-backend-url.run.app/

# API docs
curl https://your-backend-url.run.app/docs
```

### 2. Test Frontend

1. Open your Streamlit Cloud URL
2. Check browser console for errors
3. Try making a request (e.g., start discovery)

### 3. Test Connection

```bash
# From frontend, test backend connection
curl https://your-backend-url.run.app/api/dashboard
```

---

## 🐛 Troubleshooting

### "CORS Error" in Browser

**Problem**: Frontend can't connect to backend

**Solution**:
1. Check CORS configuration in backend
2. Verify frontend URL is in `allow_origins`
3. Check browser console for exact error

### "API server not running"

**Problem**: Dashboard can't reach backend

**Solution**:
1. Verify `API_URL` environment variable is set
2. Check backend is deployed and running
3. Test backend URL directly in browser

### "Connection timeout"

**Problem**: Backend takes too long to respond

**Solution**:
1. Increase timeout in dashboard: `httpx.Client(timeout=60.0)`
2. Check backend logs for errors
3. Verify backend has enough resources (memory/CPU)

### "Environment variable not found"

**Problem**: Missing configuration

**Solution**:
1. Check all env vars are set in hosting platform
2. Verify variable names match exactly
3. Restart service after adding env vars

---

## 📝 Quick Reference

### Environment Variables Needed

**Backend**:
- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_API_KEY`
- `KAFKA_API_SECRET`
- `SCHEMA_REGISTRY_URL`
- `SCHEMA_REGISTRY_API_KEY`
- `SCHEMA_REGISTRY_API_SECRET`
- `GEMINI_API_KEY` or `GCP_PROJECT_ID`
- `USE_VERTEX_AI`
- `REDDIT_CLIENT_ID` (optional)
- `REDDIT_CLIENT_SECRET` (optional)

**Frontend**:
- `API_URL` (URL of your backend)

---

## 🚀 Deployment Checklist

- [ ] Backend deployed and accessible
- [ ] Frontend deployed and accessible
- [ ] `API_URL` set in frontend environment
- [ ] CORS configured in backend
- [ ] All environment variables set
- [ ] Backend health check passes
- [ ] Frontend can connect to backend
- [ ] Test discovery/curation features
- [ ] HTTPS enabled (automatic on most platforms)
- [ ] Secrets stored securely (not in code)

---

## 💡 Pro Tips

1. **Use the same region** for frontend and backend (lower latency)
2. **Enable auto-scaling** on Cloud Run for cost efficiency
3. **Set up monitoring** (Cloud Monitoring, Sentry, etc.)
4. **Use Secret Manager** for sensitive credentials
5. **Test locally first** before deploying
6. **Keep deployment simple** for hackathon (combined hosting is fine)

---

## 📞 Need Help?

- **Streamlit Cloud Docs**: [docs.streamlit.io/streamlit-community-cloud](https://docs.streamlit.io/streamlit-community-cloud)
- **Cloud Run Docs**: [cloud.google.com/run/docs](https://cloud.google.com/run/docs)
- **Railway Docs**: [docs.railway.app](https://docs.railway.app)
- **Render Docs**: [render.com/docs](https://render.com/docs)

---

**Happy Hosting! 🚀**


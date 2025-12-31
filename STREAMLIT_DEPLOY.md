# 🚀 Streamlit Cloud Deployment Guide

Quick guide to deploy the frontend dashboard to Streamlit Cloud.

---

## 📋 Prerequisites

1. **Backend deployed** (Railway, Cloud Run, etc.)
   - Get your backend URL (e.g., `https://your-backend.railway.app`)

2. **GitHub repository** (public or private)
   - Code must be pushed to GitHub

3. **Streamlit Cloud account**
   - Sign up at [share.streamlit.io](https://share.streamlit.io) (free)

---

## 🚀 Deployment Steps

### Step 1: Push Code to GitHub

Make sure your code is pushed to GitHub:

```bash
git add .
git commit -m "Ready for Streamlit Cloud deployment"
git push origin main
```

### Step 2: Deploy to Streamlit Cloud

1. **Go to Streamlit Cloud**
   - Visit [share.streamlit.io](https://share.streamlit.io)
   - Sign in with GitHub

2. **Create New App**
   - Click "New app"
   - Select your repository
   - Choose branch (usually `main`)

3. **Configure App**
   - **Main file path**: `cryptosentinel/dashboard/app.py`
   - **Python version**: `3.11` (or `3.10` if 3.11 not available)
   - **App URL**: Choose your custom subdomain (optional)

4. **Click "Deploy"**

---

## 🔐 Configure Environment Variables

After deployment, configure secrets:

1. **Go to App Settings**
   - Click on your app → "Settings" → "Secrets"

2. **Add Secrets** (in TOML format):

```toml
# Backend API URL (REQUIRED)
API_URL = "https://your-backend.railway.app"

# Confluent Cloud (if needed for agent features)
KAFKA_BOOTSTRAP_SERVERS = "pkc-xxxxx.us-central1.gcp.confluent.cloud:9092"
KAFKA_API_KEY = "your-kafka-api-key"
KAFKA_API_SECRET = "your-kafka-api-secret"
SCHEMA_REGISTRY_URL = "https://psrc-xxxxx.us-central1.gcp.confluent.cloud"
SCHEMA_REGISTRY_API_KEY = "your-sr-api-key"
SCHEMA_REGISTRY_API_SECRET = "your-sr-api-secret"

# Google AI (if needed for agent features)
GEMINI_API_KEY = "your-gemini-api-key"
# OR use Vertex AI:
USE_VERTEX_AI = "true"
GCP_PROJECT_ID = "your-gcp-project-id"
GCP_REGION = "us-central1"

# Reddit API (optional)
REDDIT_CLIENT_ID = "your-reddit-client-id"
REDDIT_CLIENT_SECRET = "your-reddit-client-secret"
```

**Important**: 
- Replace `https://your-backend.railway.app` with your actual backend URL
- Only add secrets you need (API_URL is minimum required)

---

## ✅ Verify Deployment

1. **Check App Status**
   - App should show "Running" status
   - URL: `https://your-app.streamlit.app`

2. **Test Connection**
   - Open your Streamlit app
   - Check if it can connect to backend
   - Look for any errors in the app

3. **Check Logs**
   - Click "Manage app" → "Logs"
   - Look for any errors or warnings

---

## 🐛 Troubleshooting

### "Fail to fetch" Error

**Problem**: Frontend can't connect to backend

**Solutions**:
1. Verify `API_URL` is set correctly in Streamlit secrets
2. Check backend is running and accessible
3. Test backend URL directly: `curl https://your-backend.railway.app/`
4. Check CORS is configured in backend (should allow `*.streamlit.app`)

### "API server not running"

**Problem**: Dashboard shows this message

**Solutions**:
1. Check `API_URL` in Streamlit secrets
2. Verify backend URL is correct (no trailing slash)
3. Make sure backend is deployed and running
4. Check backend logs for errors

### Import Errors

**Problem**: Module not found errors

**Solutions**:
1. Make sure `requirements.txt` is in `cryptosentinel/` directory
2. Check Python version matches (3.11 recommended)
3. Verify all dependencies are in `requirements.txt`

### CORS Errors

**Problem**: Browser console shows CORS errors

**Solutions**:
1. Update backend CORS to allow `*.streamlit.app`:
   ```python
   allow_origins=[
       "https://*.streamlit.app",
       "https://your-app.streamlit.app",
   ]
   ```
2. Or use `allow_origins=["*"]` for development (not recommended for production)

---

## 📝 Quick Checklist

- [ ] Code pushed to GitHub
- [ ] Streamlit Cloud app created
- [ ] Main file path: `cryptosentinel/dashboard/app.py`
- [ ] `API_URL` set in secrets (your backend URL)
- [ ] Backend is running and accessible
- [ ] CORS configured in backend
- [ ] App deployed and running
- [ ] Tested connection to backend

---

## 🔗 Your URLs

After deployment, you'll have:

- **Frontend**: `https://your-app.streamlit.app`
- **Backend**: `https://your-backend.railway.app` (or wherever you deployed)

---

## 💡 Pro Tips

1. **Use Environment-Specific URLs**
   - Development: `http://localhost:8000`
   - Production: Your Railway/Cloud Run URL

2. **Monitor Both Services**
   - Check Streamlit Cloud logs for frontend issues
   - Check Railway/Cloud Run logs for backend issues

3. **Test Locally First**
   - Run frontend locally with `API_URL` set
   - Verify it connects to your deployed backend
   - Then deploy to Streamlit Cloud

---

## 📞 Need Help?

- **Streamlit Cloud Docs**: [docs.streamlit.io/streamlit-community-cloud](https://docs.streamlit.io/streamlit-community-cloud)
- **Streamlit Forum**: [discuss.streamlit.io](https://discuss.streamlit.io)

---

**Happy Deploying! 🎉**


# ⚡ CausalLab Quick Start Guide

Get CausalLab running in 5 minutes!

---

## 🚀 Fastest Path to Demo

### 1. Install Dependencies (2 min)

```bash
cd cryptosentinel
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure Minimal Setup (2 min)

```bash
cp env.example .env
```

**Minimum required for demo**:
```env
# Google AI (choose one)
GEMINI_API_KEY=your-key  # OR
USE_VERTEX_AI=true
GCP_PROJECT_ID=your-project

# Confluent (optional for demo mode)
# Leave empty to use demo data
```

### 3. Start Dashboard (1 min)

```bash
# Terminal 1: API Server
python -m uvicorn api.main:app --host 0.0.0.0 --port 8000

# Terminal 2: Dashboard
streamlit run dashboard/app.py
```

**That's it!** Open http://localhost:8501

---

## 🎯 Try Your First Discovery

1. Go to **"Causal Inference"** tab
2. Enter domain: `cryptocurrency`
3. Enter question: `Does social sentiment cause price movements?`
4. Click **"Start Discovery"**

Watch the AI agents:
- Discover data sources
- Generate hypotheses
- Test causal relationships
- Report results

---

## 📚 Next Steps

- **Full Deployment**: See [DEPLOYMENT.md](DEPLOYMENT.md)
- **Complete Pitch**: See [PITCH.md](PITCH.md)
- **Detailed README**: See [cryptosentinel/README.md](cryptosentinel/README.md)

---

## 🆘 Troubleshooting

**"API server not running"**
- Make sure API server is running on port 8000
- Check Terminal 1 for errors

**"No data shown"**
- Use demo mode (no Confluent needed)
- Or load CSV data (see README)

**"Import errors"**
- Activate virtual environment
- Run `pip install -r requirements.txt`

---

**Need help?** Check [DEPLOYMENT.md](DEPLOYMENT.md) troubleshooting section.


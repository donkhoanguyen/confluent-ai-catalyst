# 🎨 Frontend Deployment - Quick Start

Deploy your Streamlit dashboard to Streamlit Cloud in 5 minutes!

---

## ⚡ Quick Steps

### 1. Get Your Backend URL
- From Railway: `https://your-app.railway.app`
- From Cloud Run: `https://your-app.run.app`
- Copy this URL - you'll need it!

### 2. Deploy to Streamlit Cloud

1. **Go to**: [share.streamlit.io](https://share.streamlit.io)
2. **Sign in** with GitHub
3. **Click**: "New app"
4. **Select**: Your repository
5. **Configure**:
   - Main file: `cryptosentinel/dashboard/app.py`
   - Python version: `3.11` (or `3.10`)
6. **Click**: "Deploy"

### 3. Set API URL

1. **Go to**: App Settings → Secrets
2. **Add**:
   ```toml
   API_URL = "https://your-backend.railway.app"
   ```
   (Replace with your actual backend URL)

3. **Save** - App will automatically redeploy

### 4. Done! 🎉

Your dashboard is live at: `https://your-app.streamlit.app`

---

## 🔧 What You Need

**Minimum Required**:
- `API_URL` = Your backend URL

**Optional** (for agent features):
- Confluent Cloud credentials
- Google AI credentials
- Reddit API credentials

---

## ✅ Test It

1. Open your Streamlit app URL
2. Check if dashboard loads
3. Try starting a discovery/curation
4. Check browser console (F12) for errors

---

## 🐛 Common Issues

**"Fail to fetch"**:
- Check `API_URL` is correct
- Verify backend is running
- Test backend URL in browser

**"API server not running"**:
- Verify `API_URL` in secrets
- Check backend logs
- Make sure backend is deployed

---

**That's it!** Your frontend should now connect to your backend. 🚀


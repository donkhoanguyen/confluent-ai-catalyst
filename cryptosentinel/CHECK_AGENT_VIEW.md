# Fix: Agent Discovery Not Showing in Dashboard

## Quick Fix

1. **Stop the Streamlit server** (Ctrl+C in the terminal where it's running)

2. **Restart the dashboard**:
   ```bash
   streamlit run dashboard/app.py --server.port 8501
   ```

3. **Clear browser cache** or use **incognito/private mode**

4. **Look at the TOP of the sidebar** - you should see:
   ```
   🧭 Navigation
   [Select Page ▼]
   ```

## If Still Not Showing

### Check 1: Verify Import Works
```bash
cd cryptosentinel
python test_dashboard_import.py
```

You should see:
```
✅ SUCCESS: agent_view imported
✅ SUCCESS: dashboard.app imported
   AGENT_VIEW_AVAILABLE: True
```

### Check 2: Check for Errors
When you start Streamlit, look for any warnings like:
```
WARNING: Agent view not available
```

If you see this, there's an import error. Check the terminal output.

### Check 3: Manual Test
Add this to the top of `dashboard/app.py` temporarily to debug:

```python
# Debug: Check if agent view is available
st.sidebar.write(f"DEBUG: AGENT_VIEW_AVAILABLE = {AGENT_VIEW_AVAILABLE}")
```

Then restart Streamlit and check the sidebar.

## Expected Behavior

When working correctly:
1. Open `http://localhost:8501`
2. Look at the **top of the left sidebar**
3. You should see a "🧭 Navigation" section
4. A dropdown with "Dashboard" and "Agent Discovery"
5. Select "Agent Discovery" to see the agent interface

## Still Having Issues?

1. Make sure you're in the `cryptosentinel` directory
2. Make sure `dashboard/agent_view.py` exists
3. Check that all dependencies are installed: `pip install -r requirements.txt`
4. Try running: `python -c "from dashboard.agent_view import render_agent_view; print('OK')"`


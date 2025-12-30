"""
CryptoSentinel Dashboard

Real-time causal intelligence dashboard built with Streamlit.
Consumes data from the FastAPI backend.
"""

import time
from datetime import datetime, timezone
from typing import Dict, Optional

import httpx
import plotly.graph_objects as go
import streamlit as st
from loguru import logger

# Import views - try multiple import paths for robustness
CURATION_VIEW_AVAILABLE = False
CAUSAL_VIEW_AVAILABLE = False
render_curation_view = None
render_causal_view = None

try:
    from dashboard.curation_view import render_curation_view
    CURATION_VIEW_AVAILABLE = True
except ImportError:
    try:
        from curation_view import render_curation_view
        CURATION_VIEW_AVAILABLE = True
    except ImportError as e:
        logger.warning(f"Curation view not available: {e}")

try:
    from dashboard.agent_view import render_causal_view
    CAUSAL_VIEW_AVAILABLE = True
except ImportError:
    try:
        from agent_view import render_causal_view
        CAUSAL_VIEW_AVAILABLE = True
    except ImportError as e:
        logger.warning(f"Causal view not available: {e}")

# =============================================================================
# Page Configuration
# =============================================================================

st.set_page_config(
    page_title="CryptoSentinel",
    page_icon="🔮",
    layout="wide",
    initial_sidebar_state="expanded",
)

# =============================================================================
# Custom CSS - Dark Cyberpunk Theme
# =============================================================================

st.markdown("""
<style>
    /* Import custom fonts */
    @import url('https://fonts.googleapis.com/css2?family=Orbitron:wght@400;700;900&family=JetBrains+Mono:wght@400;500&display=swap');
    
    /* Root variables */
    :root {
        --bg-primary: #0a0a0f;
        --bg-secondary: #12121a;
        --bg-card: #1a1a2e;
        --accent-cyan: #00f5d4;
        --accent-magenta: #f72585;
        --accent-purple: #7b2cbf;
        --accent-yellow: #fee440;
        --text-primary: #ffffff;
        --text-secondary: #a0a0b0;
        --positive: #00f5d4;
        --negative: #f72585;
        --neutral: #fee440;
    }
    
    /* Main container */
    .main {
        background: linear-gradient(135deg, var(--bg-primary) 0%, var(--bg-secondary) 100%);
    }
    
    .stApp {
        background: linear-gradient(135deg, #0a0a0f 0%, #12121a 50%, #1a1a2e 100%);
    }
    
    /* Headers */
    h1, h2, h3 {
        font-family: 'Orbitron', sans-serif !important;
        color: var(--accent-cyan) !important;
        text-shadow: 0 0 20px rgba(0, 245, 212, 0.3);
    }
    
    h1 {
        font-size: 2.5rem !important;
        font-weight: 900 !important;
        letter-spacing: 2px;
    }
    
    /* Metric cards */
    [data-testid="stMetric"] {
        background: linear-gradient(135deg, var(--bg-card) 0%, #16213e 100%);
        border: 1px solid rgba(0, 245, 212, 0.2);
        border-radius: 12px;
        padding: 20px;
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.3);
    }
    
    [data-testid="stMetricLabel"] {
        font-family: 'JetBrains Mono', monospace !important;
        color: var(--text-secondary) !important;
        font-size: 0.9rem !important;
    }
    
    [data-testid="stMetricValue"] {
        font-family: 'Orbitron', sans-serif !important;
        color: var(--text-primary) !important;
        font-size: 1.8rem !important;
    }
    
    /* Positive/negative delta colors */
    [data-testid="stMetricDelta"] svg {
        stroke: var(--positive);
    }
    
    [data-testid="stMetricDelta"][data-testid-delta-type="negative"] svg {
        stroke: var(--negative);
    }
    
    /* Cards and containers */
    .css-1r6slb0, .css-12oz5g7 {
        background: var(--bg-card);
        border-radius: 12px;
    }
    
    /* Sidebar */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0a0a0f 0%, #1a1a2e 100%);
        border-right: 1px solid rgba(0, 245, 212, 0.1);
    }
    
    [data-testid="stSidebar"] h1, [data-testid="stSidebar"] h2 {
        color: var(--accent-cyan) !important;
    }
    
    /* Buttons */
    .stButton > button {
        background: linear-gradient(90deg, var(--accent-purple) 0%, var(--accent-magenta) 100%);
        color: white;
        border: none;
        border-radius: 8px;
        font-family: 'Orbitron', sans-serif;
        font-weight: 700;
        padding: 10px 24px;
        transition: all 0.3s ease;
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 20px rgba(247, 37, 133, 0.4);
    }
    
    /* Select boxes */
    .stSelectbox > div > div {
        background: var(--bg-card);
        border: 1px solid rgba(0, 245, 212, 0.2);
        border-radius: 8px;
    }
    
    /* Causal arrow container */
    .causal-arrow-container {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        border: 2px solid rgba(0, 245, 212, 0.3);
        border-radius: 16px;
        padding: 30px;
        text-align: center;
        margin: 20px 0;
        box-shadow: 0 0 40px rgba(0, 245, 212, 0.1);
    }
    
    .causal-arrow {
        font-family: 'JetBrains Mono', monospace;
        font-size: 1.5rem;
        letter-spacing: 2px;
    }
    
    .causal-arrow.sentiment-leads {
        color: var(--positive);
        text-shadow: 0 0 20px rgba(0, 245, 212, 0.5);
    }
    
    .causal-arrow.price-leads {
        color: var(--negative);
        text-shadow: 0 0 20px rgba(247, 37, 133, 0.5);
    }
    
    .causal-arrow.bidirectional {
        color: var(--neutral);
        text-shadow: 0 0 20px rgba(254, 228, 64, 0.5);
    }
    
    .causal-arrow.no-causality {
        color: var(--text-secondary);
    }
    
    /* Alert cards */
    .alert-high {
        background: linear-gradient(90deg, rgba(247, 37, 133, 0.2) 0%, rgba(247, 37, 133, 0.1) 100%);
        border-left: 4px solid var(--negative);
        padding: 12px 16px;
        border-radius: 8px;
        margin: 8px 0;
    }
    
    .alert-medium {
        background: linear-gradient(90deg, rgba(254, 228, 64, 0.2) 0%, rgba(254, 228, 64, 0.1) 100%);
        border-left: 4px solid var(--neutral);
        padding: 12px 16px;
        border-radius: 8px;
        margin: 8px 0;
    }
    
    .alert-low {
        background: linear-gradient(90deg, rgba(0, 245, 212, 0.2) 0%, rgba(0, 245, 212, 0.1) 100%);
        border-left: 4px solid var(--positive);
        padding: 12px 16px;
        border-radius: 8px;
        margin: 8px 0;
    }
    
    /* Post cards */
    .post-card {
        background: var(--bg-card);
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 12px;
        padding: 16px;
        margin: 8px 0;
    }
    
    .post-card .subreddit {
        color: var(--accent-cyan);
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.85rem;
    }
    
    .post-card .title {
        color: var(--text-primary);
        font-size: 1rem;
        margin: 8px 0;
    }
    
    .post-card .sentiment-positive {
        color: var(--positive);
    }
    
    .post-card .sentiment-negative {
        color: var(--negative);
    }
    
    .post-card .sentiment-neutral {
        color: var(--neutral);
    }
    
    /* Confidence meter */
    .confidence-bar {
        background: rgba(255, 255, 255, 0.1);
        border-radius: 10px;
        height: 8px;
        overflow: hidden;
    }
    
    .confidence-fill {
        background: linear-gradient(90deg, var(--accent-cyan) 0%, var(--accent-purple) 100%);
        height: 100%;
        border-radius: 10px;
        transition: width 0.5s ease;
    }
    
    /* Plotly chart backgrounds */
    .js-plotly-plot .plotly .bg {
        fill: transparent !important;
    }
</style>
""", unsafe_allow_html=True)


# =============================================================================
# API Client
# =============================================================================


class APIClient:
    """Client for CryptoSentinel API."""

    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.client = httpx.Client(timeout=10.0, follow_redirects=True)

    def get_dashboard_state(self) -> Optional[dict]:
        """Fetch complete dashboard state."""
        try:
            response = self.client.get(f"{self.base_url}/api/dashboard")
            response.raise_for_status()
            return response.json()
        except httpx.ConnectError as e:
            logger.error(f"Connection error: Cannot connect to {self.base_url}. Is the API server running?")
            return None
        except httpx.TimeoutException as e:
            logger.error(f"Timeout error: Request to {self.base_url} timed out")
            return None
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error: {e.response.status_code} - {e.response.text}")
            return None
        except Exception as e:
            logger.error(f"API error: {type(e).__name__}: {e}")
            return None

    def get_prices(self) -> dict:
        """Fetch current prices."""
        try:
            response = self.client.get(f"{self.base_url}/api/prices")
            response.raise_for_status()
            return response.json()
        except httpx.ConnectError:
            logger.warning(f"Cannot connect to {self.base_url} for prices")
            return {}
        except Exception as e:
            logger.warning(f"Error fetching prices: {e}")
            return {}

    def get_causal(self, coin_id: str) -> Optional[dict]:
        """Fetch causal analysis for a coin."""
        try:
            response = self.client.get(f"{self.base_url}/api/causal/{coin_id}")
            response.raise_for_status()
            return response.json()
        except httpx.ConnectError:
            logger.warning(f"Cannot connect to {self.base_url} for causal data")
            return None
        except Exception as e:
            logger.warning(f"Error fetching causal data: {e}")
            return None


# =============================================================================
# Empty State Data (no hardcoded values)
# =============================================================================


def get_empty_data() -> dict:
    """Return empty data structure when API is not available."""
    return {
        "prices": {},
        "sentiment": {},
        "causal_results": {},
        "recent_posts": [],
        "alerts": [],
        "last_updated": datetime.now(timezone.utc).isoformat(),
    }


# =============================================================================
# Dashboard Components
# =============================================================================


def render_causal_arrow(direction: str, lag: int, confidence: float):
    """Render the causal direction visualization."""
    if direction == "SENTIMENT_LEADS":
        arrow = f"📊 SENTIMENT ────[ {abs(lag)} min ]────▶ 💰 PRICE"
        css_class = "sentiment-leads"
        description = "Social sentiment is LEADING price movements"
    elif direction == "PRICE_LEADS":
        arrow = f"💰 PRICE ────[ {abs(lag)} min ]────▶ 📊 SENTIMENT"
        css_class = "price-leads"
        description = "Price movements are LEADING social sentiment"
    elif direction == "BIDIRECTIONAL":
        arrow = f"📊 SENTIMENT ◀────[ {abs(lag)} min ]────▶ 💰 PRICE"
        css_class = "bidirectional"
        description = "Bidirectional causality detected"
    else:
        arrow = "📊 SENTIMENT ─ ─ ─ ✗ ─ ─ ─ 💰 PRICE"
        css_class = "no-causality"
        description = "No significant causal relationship detected"

    st.markdown(f"""
    <div class="causal-arrow-container">
        <div class="causal-arrow {css_class}">{arrow}</div>
        <p style="color: #a0a0b0; margin-top: 16px; font-size: 1rem;">{description}</p>
        <div style="margin-top: 20px;">
            <span style="color: #a0a0b0;">Confidence:</span>
            <span style="color: #00f5d4; font-family: 'Orbitron', sans-serif; font-size: 1.2rem; margin-left: 8px;">
                {confidence:.1%}
            </span>
        </div>
        <div class="confidence-bar" style="margin-top: 12px; max-width: 300px; margin-left: auto; margin-right: auto;">
            <div class="confidence-fill" style="width: {confidence * 100}%;"></div>
        </div>
    </div>
    """, unsafe_allow_html=True)


def render_price_chart(prices: dict, selected_coin: str):
    """Render price chart with Plotly."""
    if not prices or selected_coin not in prices:
        st.markdown("""
        <div style="text-align: center; padding: 40px; color: #a0a0b0;">
            <p>Waiting for price data...</p>
            <small>Start the price producer to see live data</small>
        </div>
        """, unsafe_allow_html=True)
        return

    price_data = prices[selected_coin]
    symbol = price_data.get("symbol", selected_coin.upper()[:3])
    price = price_data.get("price_usd", 0)
    change = price_data.get("price_change_24h_pct", 0)

    # Create a simple gauge-style indicator
    fig = go.Figure(go.Indicator(
        mode="number+delta",
        value=price,
        number={
            "prefix": "$",
            "font": {"size": 48, "color": "#00f5d4", "family": "Orbitron"},
            "valueformat": ",.2f",
        },
        delta={
            "reference": price / (1 + change / 100),
            "relative": True,
            "valueformat": ".2%",
            "increasing": {"color": "#00f5d4"},
            "decreasing": {"color": "#f72585"},
        },
        title={"text": f"{symbol} Price", "font": {"size": 18, "color": "#a0a0b0"}},
    ))

    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        height=200,
        margin=dict(l=20, r=20, t=50, b=20),
    )

    st.plotly_chart(fig, use_container_width=True)


def render_sentiment_gauge(sentiment_data: dict, selected_coin: str):
    """Render sentiment gauge."""
    if not sentiment_data or selected_coin not in sentiment_data:
        st.markdown("""
        <div style="text-align: center; padding: 40px; color: #a0a0b0;">
            <p>Waiting for sentiment data...</p>
            <small>Start the enrichment consumer to see live data</small>
        </div>
        """, unsafe_allow_html=True)
        return

    data = sentiment_data[selected_coin]
    score = data.get("avg_sentiment", 0)
    post_count = data.get("post_count", 0)

    # Determine sentiment label and color
    if score > 0.2:
        label = "BULLISH"
        color = "#00f5d4"
    elif score < -0.2:
        label = "BEARISH"
        color = "#f72585"
    else:
        label = "NEUTRAL"
        color = "#fee440"

    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=score,
        number={"font": {"size": 36, "color": color, "family": "Orbitron"}, "valueformat": "+.2f"},
        gauge={
            "axis": {"range": [-1, 1], "tickcolor": "#a0a0b0"},
            "bar": {"color": color},
            "bgcolor": "#1a1a2e",
            "bordercolor": "#2a2a3e",
            "steps": [
                {"range": [-1, -0.2], "color": "rgba(247, 37, 133, 0.2)"},
                {"range": [-0.2, 0.2], "color": "rgba(254, 228, 64, 0.2)"},
                {"range": [0.2, 1], "color": "rgba(0, 245, 212, 0.2)"},
            ],
        },
        title={"text": f"Sentiment: {label}", "font": {"size": 18, "color": "#a0a0b0"}},
    ))

    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        height=250,
        margin=dict(l=20, r=20, t=50, b=20),
    )

    st.plotly_chart(fig, use_container_width=True)
    st.markdown(f"<p style='text-align: center; color: #a0a0b0;'>Based on {post_count} posts</p>", unsafe_allow_html=True)


def render_alerts(alerts: list):
    """Render alert cards."""
    if not alerts:
        st.markdown("<p style='color: #a0a0b0;'>No recent alerts</p>", unsafe_allow_html=True)
        return

    for alert in alerts[:5]:
        severity = alert.get("severity", "LOW").lower()
        st.markdown(f"""
        <div class="alert-{severity}">
            <strong style="color: {'#f72585' if severity == 'high' else '#fee440' if severity == 'medium' else '#00f5d4'};">
                {alert.get('alert_type', 'ALERT')}
            </strong>
            <p style="margin: 4px 0; color: #ffffff;">{alert.get('message', '')}</p>
            <small style="color: #a0a0b0;">Action: {alert.get('action', '')}</small>
        </div>
        """, unsafe_allow_html=True)


def render_posts(posts: list):
    """Render recent posts."""
    if not posts:
        st.markdown("<p style='color: #a0a0b0;'>No recent posts</p>", unsafe_allow_html=True)
        return

    for post in posts[:8]:
        sentiment = post.get("sentiment_label", "NEUTRAL")
        sentiment_class = f"sentiment-{sentiment.lower()}"
        emoji = "🟢" if sentiment == "POSITIVE" else "🔴" if sentiment == "NEGATIVE" else "🟡"

        coins = ", ".join(post.get("mentioned_coins", []))

        st.markdown(f"""
        <div class="post-card">
            <span class="subreddit">r/{post.get('subreddit', 'unknown')}</span>
            <span style="float: right; color: #a0a0b0;">⬆️ {post.get('score', 0):,}</span>
            <p class="title">{post.get('title', '')[:100]}...</p>
            <span class="{sentiment_class}">{emoji} {sentiment} ({post.get('sentiment_score', 0):+.2f})</span>
            <span style="float: right; color: #7b2cbf;">{coins}</span>
        </div>
        """, unsafe_allow_html=True)


# =============================================================================
# Main Dashboard
# =============================================================================


def main():
    """Main dashboard entry point."""
    # Initialize API client
    api = APIClient()

    # Sidebar - Navigation FIRST (must be at top)
    with st.sidebar:
        # Navigation selector at the very top - ALWAYS show
        st.markdown("## 🧭 Navigation")
        
        # Build available pages list
        pages = ["Dashboard"]
        if CURATION_VIEW_AVAILABLE:
            pages.append("Data Curation")
        if CAUSAL_VIEW_AVAILABLE:
            pages.append("Causal Inference")
        
        page = st.selectbox(
            "Select Page",
            pages,
            key="page_selector",
            label_visibility="visible"
        )
        
        # Show warnings for unavailable views
        if not CURATION_VIEW_AVAILABLE and not CAUSAL_VIEW_AVAILABLE:
            st.warning("⚠️ Agent views not available")
            st.info("Check terminal logs for import errors")
        elif not CURATION_VIEW_AVAILABLE:
            st.warning("⚠️ Data Curation view not available")
        elif not CAUSAL_VIEW_AVAILABLE:
            st.warning("⚠️ Causal Inference view not available")
        
        st.markdown("---")

    # Handle page routing
    if page == "Data Curation":
        if CURATION_VIEW_AVAILABLE:
            render_curation_view()
        return
    elif page == "Causal Inference":
        if CAUSAL_VIEW_AVAILABLE:
            render_causal_view()
        return

    # Dashboard-specific Sidebar Settings
    with st.sidebar:
        st.markdown("## ⚙️ Settings")

        # Coin selector
        selected_coin = st.selectbox(
            "Select Cryptocurrency",
            ["bitcoin", "ethereum", "solana", "dogecoin"],
            format_func=lambda x: x.upper(),
        )

        st.markdown("---")

        # Auto-refresh
        auto_refresh = st.checkbox("Auto-refresh", value=True)
        refresh_rate = st.slider("Refresh rate (seconds)", 5, 60, 10)

        st.markdown("---")

        st.markdown("### 📊 Causal Parameters")
        st.slider("Window size (minutes)", 30, 180, 60, disabled=True)
        st.slider("Max lag (minutes)", 5, 30, 10, disabled=True)

        st.markdown("---")

        st.markdown("""
        <div style="text-align: center; padding: 20px;">
            <p style="color: #a0a0b0; font-size: 0.8rem;">
                Powered by<br/>
                <strong style="color: #00f5d4;">Confluent + Google AI</strong>
            </p>
        </div>
        """, unsafe_allow_html=True)

    # Fetch data from API
    data = api.get_dashboard_state()
    if not data:
        st.warning("⚠️ API server not running. Start it with: `python -m api.main`")
        data = get_empty_data()

    # Main content
    # Row 1: Price and Sentiment
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### 💰 Price")
        render_price_chart(data.get("prices", {}), selected_coin)

    with col2:
        st.markdown("### 📊 Sentiment")
        render_sentiment_gauge(data.get("sentiment", {}), selected_coin)

    # Row 2: Causal Direction (full width)
    st.markdown("### 🔗 Causal Direction")
    causal_data = data.get("causal_results", {}).get(selected_coin, {})
    render_causal_arrow(
        direction=causal_data.get("direction", "NO_CAUSALITY"),
        lag=causal_data.get("lead_lag_minutes", 0),
        confidence=causal_data.get("confidence", 0),
    )

    # Row 3: Alerts and Posts
    col3, col4 = st.columns([1, 2])

    with col3:
        st.markdown("### 🚨 Alerts")
        render_alerts(data.get("alerts", []))

    with col4:
        st.markdown("### 📝 Recent Posts")
        render_posts(data.get("recent_posts", []))

    # Footer with timestamp
    st.markdown(f"""
    <div style="text-align: center; padding: 40px 0 20px; color: #a0a0b0;">
        <small>Last updated: {data.get('last_updated', datetime.now(timezone.utc).isoformat())}</small>
    </div>
    """, unsafe_allow_html=True)

    # Auto-refresh
    if auto_refresh:
        time.sleep(refresh_rate)
        st.rerun()


if __name__ == "__main__":
    main()


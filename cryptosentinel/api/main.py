"""
CryptoSentinel API

FastAPI backend providing REST and WebSocket endpoints.
This API can be consumed by Streamlit or any other frontend.
"""

import asyncio
import json
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import pandas as pd
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger
from pydantic import BaseModel

from causal.granger import (
    bidirectional_granger_test,
    CausalDirection,
    CausalAnalysisResult,
    generate_causal_alerts,
)

# =============================================================================
# Pydantic Models (API Response Schemas)
# =============================================================================


class PriceData(BaseModel):
    """Current price data for a coin."""
    coin_id: str
    symbol: str
    price_usd: float
    volume_24h: float
    price_change_24h_pct: float
    timestamp: datetime


class SentimentData(BaseModel):
    """Aggregated sentiment data."""
    coin_id: str
    avg_sentiment: float
    post_count: int
    positive_count: int
    negative_count: int
    neutral_count: int
    timestamp: datetime


class CausalResultResponse(BaseModel):
    """Causal analysis result."""
    coin_id: str
    direction: str
    lead_lag_minutes: int
    confidence: float
    sentiment_to_price_pval: float
    price_to_sentiment_pval: float
    sample_size: int
    computed_at: datetime


class RedditPostResponse(BaseModel):
    """Enriched Reddit post."""
    post_id: str
    subreddit: str
    title: str
    body: Optional[str]
    score: int
    sentiment_label: str
    sentiment_score: float
    mentioned_coins: List[str]
    created_at: datetime


class AlertResponse(BaseModel):
    """Causal alert."""
    alert_type: str
    severity: str
    message: str
    action: str
    timestamp: datetime


class DashboardState(BaseModel):
    """Complete dashboard state for frontend."""
    prices: Dict[str, PriceData]
    sentiment: Dict[str, SentimentData]
    causal_results: Dict[str, CausalResultResponse]
    recent_posts: List[RedditPostResponse]
    alerts: List[AlertResponse]
    last_updated: datetime


# =============================================================================
# In-Memory Data Store (for demo - replace with Redis/DB in production)
# =============================================================================


class DataStore:
    """In-memory store for real-time data."""

    def __init__(self, max_history: int = 1000):
        self.max_history = max_history

        # Current prices by coin_id
        self.prices: Dict[str, PriceData] = {}

        # Price history for causal analysis
        self.price_history: Dict[str, deque] = {}

        # Current sentiment by coin_id
        self.sentiment: Dict[str, SentimentData] = {}

        # Sentiment history for causal analysis
        self.sentiment_history: Dict[str, deque] = {}

        # Recent posts (all coins)
        self.recent_posts: deque = deque(maxlen=100)

        # Causal results by coin_id
        self.causal_results: Dict[str, CausalResultResponse] = {}

        # Previous causal results (for alert generation)
        self.previous_causal: Dict[str, CausalAnalysisResult] = {}

        # Alerts
        self.alerts: deque = deque(maxlen=50)

        # Connected WebSocket clients
        self.websocket_clients: List[WebSocket] = []

    def update_price(self, price: PriceData):
        """Update price data for a coin."""
        self.prices[price.coin_id] = price

        # Add to history
        if price.coin_id not in self.price_history:
            self.price_history[price.coin_id] = deque(maxlen=self.max_history)
        self.price_history[price.coin_id].append({
            "timestamp": price.timestamp,
            "price_usd": price.price_usd,
            "return_pct": price.price_change_24h_pct,
        })

    def update_sentiment(self, sentiment: SentimentData):
        """Update sentiment data for a coin."""
        self.sentiment[sentiment.coin_id] = sentiment

        # Add to history
        if sentiment.coin_id not in self.sentiment_history:
            self.sentiment_history[sentiment.coin_id] = deque(maxlen=self.max_history)
        self.sentiment_history[sentiment.coin_id].append({
            "timestamp": sentiment.timestamp,
            "avg_sentiment": sentiment.avg_sentiment,
            "post_count": sentiment.post_count,
        })

    def add_post(self, post: RedditPostResponse):
        """Add a new enriched post."""
        self.recent_posts.appendleft(post)

    def compute_causal_analysis(self, coin_id: str) -> Optional[CausalResultResponse]:
        """Compute causal analysis for a coin using stored history."""
        if coin_id not in self.price_history or coin_id not in self.sentiment_history:
            return None

        price_history = list(self.price_history[coin_id])
        sentiment_history = list(self.sentiment_history[coin_id])

        if len(price_history) < 30 or len(sentiment_history) < 30:
            return None

        # Create DataFrame for analysis
        price_df = pd.DataFrame(price_history).set_index("timestamp")
        sentiment_df = pd.DataFrame(sentiment_history).set_index("timestamp")

        # Merge on timestamp (approximate join)
        df = price_df.join(sentiment_df, how="inner")

        if len(df) < 30:
            return None

        try:
            result = bidirectional_granger_test(
                df,
                sentiment_col="avg_sentiment",
                price_col="return_pct",
                max_lag=10,
            )

            # Generate alerts if we have previous results
            if coin_id in self.previous_causal:
                alerts = generate_causal_alerts(result, self.previous_causal[coin_id])
                for alert in alerts:
                    self.alerts.appendleft(AlertResponse(
                        alert_type=alert.alert_type,
                        severity=alert.severity,
                        message=alert.message,
                        action=alert.action,
                        timestamp=alert.timestamp,
                    ))

            self.previous_causal[coin_id] = result

            causal_response = CausalResultResponse(
                coin_id=coin_id,
                direction=result.direction.value,
                lead_lag_minutes=result.lead_lag_minutes,
                confidence=result.confidence,
                sentiment_to_price_pval=result.sentiment_to_price.p_value,
                price_to_sentiment_pval=result.price_to_sentiment.p_value,
                sample_size=result.sample_size,
                computed_at=datetime.now(timezone.utc),
            )

            self.causal_results[coin_id] = causal_response
            return causal_response

        except Exception as e:
            logger.error(f"Causal analysis failed for {coin_id}: {e}")
            return None

    def get_dashboard_state(self) -> DashboardState:
        """Get complete dashboard state."""
        return DashboardState(
            prices=self.prices,
            sentiment=self.sentiment,
            causal_results=self.causal_results,
            recent_posts=list(self.recent_posts)[:20],
            alerts=list(self.alerts)[:10],
            last_updated=datetime.now(timezone.utc),
        )


# Initialize data store
data_store = DataStore()


# =============================================================================
# FastAPI Application
# =============================================================================


# Import agent router
try:
    from api.agent import router as agent_router
    AGENT_AVAILABLE = True
except ImportError:
    AGENT_AVAILABLE = False
    logger.warning("Agent module not available")

app = FastAPI(
    title="CryptoSentinel API",
    description="Real-time causal intelligence for crypto markets",
    version="1.0.0",
)

# CORS for frontend access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include agent router if available
if AGENT_AVAILABLE:
    app.include_router(agent_router)


# =============================================================================
# REST Endpoints
# =============================================================================


@app.get("/")
async def root():
    """API health check."""
    return {
        "status": "healthy",
        "service": "CryptoSentinel API",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api/prices", response_model=Dict[str, PriceData])
async def get_prices():
    """Get current prices for all tracked coins."""
    return data_store.prices


@app.get("/api/prices/{coin_id}", response_model=PriceData)
async def get_price(coin_id: str):
    """Get current price for a specific coin."""
    if coin_id not in data_store.prices:
        return {"error": f"Coin {coin_id} not found"}
    return data_store.prices[coin_id]


@app.get("/api/sentiment", response_model=Dict[str, SentimentData])
async def get_sentiment():
    """Get current sentiment for all tracked coins."""
    return data_store.sentiment


@app.get("/api/sentiment/{coin_id}", response_model=SentimentData)
async def get_coin_sentiment(coin_id: str):
    """Get current sentiment for a specific coin."""
    if coin_id not in data_store.sentiment:
        return {"error": f"Coin {coin_id} not found"}
    return data_store.sentiment[coin_id]


@app.get("/api/causal", response_model=Dict[str, CausalResultResponse])
async def get_causal_results():
    """Get causal analysis results for all coins."""
    return data_store.causal_results


@app.get("/api/causal/{coin_id}", response_model=CausalResultResponse)
async def get_coin_causal(coin_id: str):
    """Get causal analysis for a specific coin."""
    if coin_id in data_store.causal_results:
        return data_store.causal_results[coin_id]

    # Try to compute if not available
    result = data_store.compute_causal_analysis(coin_id)
    if result:
        return result

    return {"error": f"Insufficient data for {coin_id}"}


@app.get("/api/posts", response_model=List[RedditPostResponse])
async def get_recent_posts(
    limit: int = Query(20, ge=1, le=100),
    coin: Optional[str] = None,
):
    """Get recent Reddit posts, optionally filtered by coin."""
    posts = list(data_store.recent_posts)

    if coin:
        coin_upper = coin.upper()
        posts = [p for p in posts if coin_upper in p.mentioned_coins]

    return posts[:limit]


@app.get("/api/alerts", response_model=List[AlertResponse])
async def get_alerts(limit: int = Query(10, ge=1, le=50)):
    """Get recent causal alerts."""
    return list(data_store.alerts)[:limit]


@app.get("/api/dashboard", response_model=DashboardState)
async def get_dashboard_state():
    """Get complete dashboard state in a single request."""
    return data_store.get_dashboard_state()


# =============================================================================
# WebSocket for Real-Time Updates
# =============================================================================


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    WebSocket endpoint for real-time updates.

    Clients receive updates whenever new data arrives.
    """
    await websocket.accept()
    data_store.websocket_clients.append(websocket)
    logger.info(f"WebSocket client connected. Total: {len(data_store.websocket_clients)}")

    try:
        # Send initial state
        state = data_store.get_dashboard_state()
        await websocket.send_json(state.model_dump(mode="json"))

        # Keep connection alive and send updates
        while True:
            # Wait for data or timeout (heartbeat)
            try:
                # Check for client messages (e.g., subscription changes)
                data = await asyncio.wait_for(websocket.receive_text(), timeout=5.0)
                # Handle client commands if needed
                logger.debug(f"Received from client: {data}")

            except asyncio.TimeoutError:
                # Send heartbeat/state update
                state = data_store.get_dashboard_state()
                await websocket.send_json({
                    "type": "update",
                    "data": state.model_dump(mode="json"),
                })

    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        if websocket in data_store.websocket_clients:
            data_store.websocket_clients.remove(websocket)


async def broadcast_update(update_type: str, data: dict):
    """Broadcast update to all connected WebSocket clients."""
    if not data_store.websocket_clients:
        return

    message = {"type": update_type, "data": data}

    disconnected = []
    for client in data_store.websocket_clients:
        try:
            await client.send_json(message)
        except Exception:
            disconnected.append(client)

    # Clean up disconnected clients
    for client in disconnected:
        data_store.websocket_clients.remove(client)


# =============================================================================
# Data Ingestion Endpoints (for producers to push data)
# =============================================================================


@app.post("/api/ingest/price")
async def ingest_price(price: PriceData):
    """Receive price update from producer."""
    data_store.update_price(price)
    await broadcast_update("price", price.model_dump(mode="json"))
    return {"status": "ok"}


@app.post("/api/ingest/post")
async def ingest_post(post: RedditPostResponse):
    """Receive enriched post from consumer."""
    data_store.add_post(post)

    # Update aggregated sentiment for mentioned coins
    for coin in post.mentioned_coins:
        coin_lower = coin.lower()
        # In production, aggregate properly from history
        # For demo, just update with latest post sentiment
        sentiment = SentimentData(
            coin_id=coin_lower,
            avg_sentiment=post.sentiment_score,
            post_count=1,
            positive_count=1 if post.sentiment_label == "POSITIVE" else 0,
            negative_count=1 if post.sentiment_label == "NEGATIVE" else 0,
            neutral_count=1 if post.sentiment_label == "NEUTRAL" else 0,
            timestamp=datetime.now(timezone.utc),
        )
        data_store.update_sentiment(sentiment)

    await broadcast_update("post", post.model_dump(mode="json"))
    return {"status": "ok"}


@app.post("/api/compute/causal/{coin_id}")
async def trigger_causal_analysis(coin_id: str):
    """Trigger causal analysis computation for a coin."""
    result = data_store.compute_causal_analysis(coin_id)
    if result:
        await broadcast_update("causal", result.model_dump(mode="json"))
        return result
    return {"error": "Insufficient data"}


# =============================================================================
# Entry Point
# =============================================================================


def main():
    """Run the API server."""
    import uvicorn

    from config.settings import get_settings
    settings = get_settings()

    uvicorn.run(
        "api.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=True,
    )


if __name__ == "__main__":
    main()


"""
Crypto News Collector

Fetches cryptocurrency news from NewsData.io API.
Free tier: 200 requests/day, includes built-in sentiment analysis.

API Docs: https://newsdata.io/documentation
"""

import os
import json
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any
from collections import defaultdict

import requests
import google.genai as genai
from loguru import logger

from config.settings import Settings, get_settings


# Mapping of coin IDs to search keywords
COIN_KEYWORDS = {
    "bitcoin": ["bitcoin", "btc"],
    "ethereum": ["ethereum", "eth"],
    "solana": ["solana", "sol"],
    "dogecoin": ["dogecoin", "doge"],
    "cardano": ["cardano", "ada"],
    "ripple": ["ripple", "xrp"],
    "polkadot": ["polkadot", "dot"],
}


class NewsDataCollector:
    """
    Collector for crypto news from NewsData.io.
    
    Features:
    - Free tier: 200 requests/day
    - Uses Gemini AI for sentiment analysis (free tier doesn't include sentiment)
    - Supports keyword search and category filtering
    - Returns article metadata including title, description, sentiment
    
    Environment Variables:
        NEWSDATA_API_KEY: Your NewsData.io API key (free signup at https://newsdata.io)
        GEMINI_API_KEY: Your Gemini API key (for sentiment analysis)
    """

    API_URL = "https://newsdata.io/api/1/news"
    
    def __init__(self, settings: Optional[Settings] = None, api_key: Optional[str] = None):
        self.settings = settings or get_settings()
        # Check in order: explicit parameter > settings > environment variable
        self.api_key = api_key or getattr(self.settings, 'newsdata_api_key', None) or os.getenv("NEWSDATA_API_KEY")
        
        if not self.api_key:
            logger.warning(
                "NEWSDATA_API_KEY not set. News collector will not function. "
                "Get a free API key at https://newsdata.io"
            )
        else:
            logger.info("NewsDataCollector initialized with API key")
        
        # Initialize Gemini for sentiment analysis (free tier doesn't include sentiment)
        try:
            self.gemini_client = genai.Client(api_key=self.settings.gemini_api_key)
            logger.info("Gemini client initialized for news sentiment analysis")
        except Exception as e:
            logger.warning(f"Gemini not available for sentiment analysis: {e}")
            self.gemini_client = None
        
        self._request_count = 0
        self._last_reset = datetime.now(timezone.utc)

    def get_variables(self) -> List[str]:
        """Return list of variables this collector provides."""
        return [
            "news_count",
            "news_sentiment_avg",
            "news_positive_count",
            "news_negative_count",
            "news_neutral_count",
            "latest_headline",
        ]

    def _check_rate_limit(self):
        """Check and manage rate limiting (200 requests/day)."""
        now = datetime.now(timezone.utc)
        
        # Reset counter if it's been more than 24 hours
        if (now - self._last_reset).total_seconds() > 86400:
            self._request_count = 0
            self._last_reset = now
        
        if self._request_count >= 190:  # Leave buffer
            logger.warning("Approaching NewsData.io daily rate limit")
        
        self._request_count += 1

    def fetch_news(
        self,
        query: str,
        language: str = "en",
        category: Optional[str] = "business",
        size: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Fetch news articles matching a query.
        
        Args:
            query: Search query (e.g., "bitcoin OR btc")
            language: Language code (default: "en")
            category: News category (default: "business")
            size: Number of articles to return (max 10 on free tier)
            
        Returns:
            List of article dicts with title, description, sentiment, etc.
        """
        if not self.api_key:
            logger.error("NewsData API key not configured")
            return []
        
        self._check_rate_limit()
        
        try:
            params = {
                "apikey": self.api_key,
                "q": query,
                "language": language,
                "size": min(size, 10),  # Free tier max
            }
            
            if category:
                params["category"] = category
            
            response = requests.get(self.API_URL, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
            
            if data.get("status") != "success":
                logger.error(f"NewsData API error: {data.get('message', 'Unknown error')}")
                return []
            
            articles = []
            for article in data.get("results", []):
                articles.append({
                    "title": article.get("title", ""),
                    "description": article.get("description", ""),
                    "content": article.get("content", ""),
                    "source": article.get("source_id", "unknown"),
                    "link": article.get("link", ""),
                    "pubDate": article.get("pubDate"),
                    "sentiment": article.get("sentiment"),  # Built-in sentiment!
                    "category": article.get("category", []),
                    "keywords": article.get("keywords", []),
                })
            
            logger.debug(f"Fetched {len(articles)} news articles for query: {query}")
            return articles
            
        except requests.exceptions.RequestException as e:
            logger.error(f"NewsData API request failed: {e}")
            return []
        except Exception as e:
            logger.error(f"Failed to process NewsData response: {e}")
            return []

    def fetch_crypto_news(self, coin_id: str, size: int = 10) -> List[Dict[str, Any]]:
        """
        Fetch news for a specific cryptocurrency.
        
        Args:
            coin_id: Coin identifier (e.g., "bitcoin")
            size: Number of articles to return
            
        Returns:
            List of article dicts
        """
        keywords = COIN_KEYWORDS.get(coin_id.lower(), [coin_id])
        query = " OR ".join(keywords)
        return self.fetch_news(query, size=size)

    def _analyze_article_sentiment(self, article: Dict[str, Any]) -> Dict[str, Any]:
        """
        Use Gemini to analyze sentiment of a news article.
        
        Args:
            article: Article dict with title and description
            
        Returns:
            Dict with sentiment_label, sentiment_score, confidence
        """
        if not self.gemini_client:
            return {
                "sentiment_label": "NEUTRAL",
                "sentiment_score": 0.0,
                "confidence": 0.0,
            }
        
        prompt = f"""Analyze this cryptocurrency news article and extract sentiment.

Title: {article.get('title', '')}
Description: {article.get('description', '')[:500]}

Respond with ONLY valid JSON (no markdown):
{{
  "sentiment_label": "POSITIVE" or "NEGATIVE" or "NEUTRAL",
  "sentiment_score": <float from -1.0 (very bearish) to 1.0 (very bullish)>,
  "confidence": <float from 0.0 to 1.0>
}}"""
        
        try:
            response = self.gemini_client.models.generate_content(
                model="gemini-2.5-flash",
                contents=prompt,
            )
            text = response.text.strip()
            
            # Clean JSON
            if text.startswith("```"):
                lines = text.split("\n")
                text = "\n".join(lines[1:-1] if lines[-1] == "```" else lines[1:])
                if text.startswith("json"):
                    text = text[4:].strip()
            
            sentiment_data = json.loads(text)
            return {
                "sentiment_label": sentiment_data.get("sentiment_label", "NEUTRAL"),
                "sentiment_score": float(sentiment_data.get("sentiment_score", 0.0)),
                "confidence": float(sentiment_data.get("confidence", 0.5)),
            }
        except Exception as e:
            logger.debug(f"Gemini sentiment analysis failed: {e}")
            return {
                "sentiment_label": "NEUTRAL",
                "sentiment_score": 0.0,
                "confidence": 0.0,
            }

    def aggregate_sentiment(
        self,
        articles: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Aggregate sentiment from a list of articles.
        
        Uses Gemini to analyze sentiment if NewsData.io doesn't provide it.
        
        Args:
            articles: List of article dicts
            
        Returns:
            Dict with aggregated sentiment metrics
        """
        if not articles:
            return {
                "news_count": 0,
                "news_sentiment_avg": None,
                "news_positive_count": 0,
                "news_negative_count": 0,
                "news_neutral_count": 0,
                "latest_headline": None,
            }
        
        sentiment_counts = {"positive": 0, "negative": 0, "neutral": 0}
        sentiment_scores = []
        
        for article in articles:
            # Check if NewsData.io provided sentiment (paid plans only)
            sentiment = article.get("sentiment")
            
            # If sentiment is a placeholder string, use Gemini
            if not sentiment or "ONLY AVAILABLE" in str(sentiment).upper():
                if self.gemini_client:
                    sentiment_data = self._analyze_article_sentiment(article)
                    sentiment_score = sentiment_data["sentiment_score"]
                    sentiment_label = sentiment_data["sentiment_label"]
                else:
                    sentiment_score = 0.0
                    sentiment_label = "NEUTRAL"
            else:
                # Use provided sentiment
                sent_lower = sentiment.lower() if isinstance(sentiment, str) else "neutral"
                if sent_lower == "positive":
                    sentiment_score = 1.0
                    sentiment_label = "POSITIVE"
                elif sent_lower == "negative":
                    sentiment_score = -1.0
                    sentiment_label = "NEGATIVE"
                else:
                    sentiment_score = 0.0
                    sentiment_label = "NEUTRAL"
            
            # Count by label
            if sentiment_label == "POSITIVE":
                sentiment_counts["positive"] += 1
            elif sentiment_label == "NEGATIVE":
                sentiment_counts["negative"] += 1
            else:
                sentiment_counts["neutral"] += 1
            
            sentiment_scores.append(sentiment_score)
            
            # Rate limit Gemini calls
            if self.gemini_client:
                time.sleep(0.5)
        
        avg_sentiment = (
            sum(sentiment_scores) / len(sentiment_scores) 
            if sentiment_scores else None
        )
        
        return {
            "news_count": len(articles),
            "news_sentiment_avg": avg_sentiment,
            "news_positive_count": sentiment_counts["positive"],
            "news_negative_count": sentiment_counts["negative"],
            "news_neutral_count": sentiment_counts["neutral"],
            "latest_headline": articles[0]["title"] if articles else None,
        }

    def collect(self, coin_ids: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Collect news sentiment data for given coins.
        
        Args:
            coin_ids: List of coin IDs to collect for
            
        Returns:
            Dict mapping coin_id -> aggregated sentiment data
        """
        if not self.api_key:
            logger.warning("NewsData API key not configured - skipping news collection")
            return {}
        
        result = {}
        
        for coin_id in coin_ids:
            try:
                articles = self.fetch_crypto_news(coin_id, size=10)
                aggregated = self.aggregate_sentiment(articles)
                aggregated["coin_id"] = coin_id
                aggregated["collected_at"] = datetime.now(timezone.utc)
                result[coin_id] = aggregated
                
                # Rate limit between requests
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Failed to collect news for {coin_id}: {e}")
                result[coin_id] = self.aggregate_sentiment([])
        
        return result

    def get_general_crypto_news(self, size: int = 10) -> List[Dict[str, Any]]:
        """
        Fetch general cryptocurrency news (not coin-specific).
        
        Args:
            size: Number of articles to return
            
        Returns:
            List of article dicts
        """
        return self.fetch_news(
            query="cryptocurrency OR crypto OR blockchain",
            category="business,technology",
            size=size,
        )


class GNewsCollector:
    """
    Alternative news collector using GNews API.
    Free tier: 100 requests/day.
    
    Use this as a fallback if NewsData.io is unavailable.
    
    API Docs: https://gnews.io/docs/v4
    Environment Variable: GNEWS_API_KEY
    """

    API_URL = "https://gnews.io/api/v4/search"
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("GNEWS_API_KEY")
        
        if not self.api_key:
            logger.warning(
                "GNEWS_API_KEY not set. GNews collector will not function. "
                "Get a free API key at https://gnews.io"
            )

    def get_variables(self) -> List[str]:
        return ["news_count", "latest_headline"]

    def fetch_news(self, query: str, max_articles: int = 10) -> List[Dict[str, Any]]:
        """Fetch news from GNews API."""
        if not self.api_key:
            return []
        
        try:
            params = {
                "q": query,
                "token": self.api_key,
                "lang": "en",
                "max": min(max_articles, 10),
            }
            
            response = requests.get(self.API_URL, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
            
            articles = []
            for article in data.get("articles", []):
                articles.append({
                    "title": article.get("title", ""),
                    "description": article.get("description", ""),
                    "content": article.get("content", ""),
                    "source": article.get("source", {}).get("name", "unknown"),
                    "link": article.get("url", ""),
                    "pubDate": article.get("publishedAt"),
                })
            
            return articles
            
        except Exception as e:
            logger.error(f"GNews API error: {e}")
            return []

    def collect(self, coin_ids: List[str]) -> Dict[str, Dict[str, Any]]:
        """Collect news for given coins (without sentiment - GNews doesn't provide it)."""
        result = {}
        
        for coin_id in coin_ids:
            keywords = COIN_KEYWORDS.get(coin_id.lower(), [coin_id])
            query = " OR ".join(keywords)
            articles = self.fetch_news(query)
            
            result[coin_id] = {
                "news_count": len(articles),
                "latest_headline": articles[0]["title"] if articles else None,
                "collected_at": datetime.now(timezone.utc),
            }
            
            time.sleep(0.5)
        
        return result


def main():
    """Test the news collectors."""
    
    # Test NewsData.io collector
    print("\n=== NewsData.io Collector ===")
    news_collector = NewsDataCollector()
    
    if news_collector.api_key:
        # Test single coin
        print("\nFetching Bitcoin news...")
        articles = news_collector.fetch_crypto_news("bitcoin", size=5)
        
        for article in articles[:3]:
            print(f"\nTitle: {article['title'][:80]}...")
            print(f"Sentiment: {article.get('sentiment', 'N/A')}")
            print(f"Source: {article['source']}")
        
        # Test aggregation
        print("\n=== Aggregated Sentiment ===")
        aggregated = news_collector.aggregate_sentiment(articles)
        print(f"Article count: {aggregated['news_count']}")
        print(f"Avg sentiment: {aggregated['news_sentiment_avg']}")
        print(f"Positive: {aggregated['news_positive_count']}")
        print(f"Negative: {aggregated['news_negative_count']}")
        
        # Test multi-coin collection
        print("\n=== Multi-coin Collection ===")
        data = news_collector.collect(["bitcoin", "ethereum"])
        for coin, values in data.items():
            print(f"{coin}: {values['news_count']} articles, sentiment: {values['news_sentiment_avg']}")
    else:
        print("NEWSDATA_API_KEY not set - skipping NewsData tests")
    
    # Test Fear & Greed (no key needed)
    print("\n=== Fear & Greed Index (for comparison) ===")
    from producers.fear_greed_collector import FearGreedCollector
    fng = FearGreedCollector()
    current = fng.fetch_current()
    if current:
        print(f"Current: {current['fear_greed_value']} ({current['fear_greed_classification']})")


if __name__ == "__main__":
    main()


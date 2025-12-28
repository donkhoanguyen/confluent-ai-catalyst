"""
Fear & Greed Index Collector

Fetches the Crypto Fear & Greed Index from Alternative.me API.
This is a free API with no authentication required.

API Docs: https://alternative.me/crypto/fear-and-greed-index/
"""

import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

import requests
from loguru import logger

from config.settings import Settings, get_settings


class FearGreedCollector:
    """
    Collector for the Crypto Fear & Greed Index.
    
    The Fear & Greed Index is a market-wide sentiment indicator:
    - 0-24: Extreme Fear
    - 25-49: Fear
    - 50-74: Greed
    - 75-100: Extreme Greed
    
    This is a contrarian indicator - extreme fear may signal buying opportunities,
    while extreme greed may signal a market top.
    """

    API_URL = "https://api.alternative.me/fng/"
    
    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings or get_settings()
        self._last_value: Optional[Dict[str, Any]] = None
        logger.info("FearGreedCollector initialized (no auth required)")

    def get_variables(self) -> List[str]:
        """Return list of variables this collector provides."""
        return [
            "fear_greed_value",
            "fear_greed_classification", 
            "fear_greed_timestamp",
        ]

    def fetch_current(self) -> Optional[Dict[str, Any]]:
        """
        Fetch the current Fear & Greed Index value.
        
        Returns:
            Dict with fear_greed_value (0-100), classification, and timestamp
            or None if the request fails.
        """
        try:
            # Don't use date_format=us to get Unix timestamp
            response = requests.get(
                self.API_URL,
                params={"limit": 1},
                timeout=10,
            )
            response.raise_for_status()
            data = response.json()
            
            if data.get("data") and len(data["data"]) > 0:
                fng_data = data["data"][0]
                result = {
                    "fear_greed_value": int(fng_data["value"]),
                    "fear_greed_classification": fng_data["value_classification"],
                    "fear_greed_timestamp": datetime.fromtimestamp(
                        int(fng_data["timestamp"]), tz=timezone.utc
                    ),
                }
                self._last_value = result
                logger.debug(
                    f"Fear & Greed: {result['fear_greed_value']} "
                    f"({result['fear_greed_classification']})"
                )
                return result
            
            logger.warning("No data returned from Fear & Greed API")
            return None
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Fear & Greed API request failed: {e}")
            return None
        except (KeyError, ValueError, TypeError) as e:
            logger.error(f"Failed to parse Fear & Greed response: {e}")
            return None

    def fetch_history(self, limit: int = 30) -> List[Dict[str, Any]]:
        """
        Fetch historical Fear & Greed Index values.
        
        Args:
            limit: Number of historical days to fetch (max ~2000)
            
        Returns:
            List of dicts with fear_greed data, newest first
        """
        try:
            # Don't use date_format=us to get Unix timestamp
            response = requests.get(
                self.API_URL,
                params={"limit": limit},
                timeout=15,
            )
            response.raise_for_status()
            data = response.json()
            
            results = []
            for fng_data in data.get("data", []):
                results.append({
                    "fear_greed_value": int(fng_data["value"]),
                    "fear_greed_classification": fng_data["value_classification"],
                    "fear_greed_timestamp": datetime.fromtimestamp(
                        int(fng_data["timestamp"]), tz=timezone.utc
                    ),
                })
            
            logger.info(f"Fetched {len(results)} days of Fear & Greed history")
            return results
            
        except Exception as e:
            logger.error(f"Failed to fetch Fear & Greed history: {e}")
            return []

    def collect(self, coin_ids: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Collect Fear & Greed data for given coins.
        
        Note: Fear & Greed Index is market-wide (primarily Bitcoin-driven),
        so we return the same value for all coins.
        
        Args:
            coin_ids: List of coin IDs to collect for
            
        Returns:
            Dict mapping coin_id -> fear_greed data
        """
        current = self.fetch_current()
        
        if not current:
            return {}
        
        # Return same value for all coins (it's a market-wide indicator)
        result = {}
        for coin_id in coin_ids:
            result[coin_id] = current.copy()
        
        return result

    def get_last_value(self) -> Optional[Dict[str, Any]]:
        """Get the last fetched value without making a new API call."""
        return self._last_value


def main():
    """Test the Fear & Greed collector."""
    collector = FearGreedCollector()
    
    # Test current value
    print("\n=== Current Fear & Greed Index ===")
    current = collector.fetch_current()
    if current:
        print(f"Value: {current['fear_greed_value']}")
        print(f"Classification: {current['fear_greed_classification']}")
        print(f"Timestamp: {current['fear_greed_timestamp']}")
    
    # Test history
    print("\n=== Last 7 Days ===")
    history = collector.fetch_history(limit=7)
    for day in history:
        print(
            f"{day['fear_greed_timestamp'].strftime('%Y-%m-%d')}: "
            f"{day['fear_greed_value']} ({day['fear_greed_classification']})"
        )
    
    # Test collect for multiple coins
    print("\n=== Collect for coins ===")
    coins = ["bitcoin", "ethereum"]
    data = collector.collect(coins)
    for coin, values in data.items():
        print(f"{coin}: {values['fear_greed_value']}")


if __name__ == "__main__":
    main()


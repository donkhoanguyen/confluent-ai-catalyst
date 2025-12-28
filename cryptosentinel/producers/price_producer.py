"""
Price Producer

Fetches cryptocurrency prices from CoinGecko and streams to Kafka + API.
"""

import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

import requests
from loguru import logger

from config.settings import Settings, get_settings

# Kafka imports - optional, will gracefully degrade if not configured
try:
    from confluent_kafka import Producer
    from confluent_kafka.serialization import SerializationContext, MessageField
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroSerializer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    logger.warning("Kafka libraries not available - running in API-only mode")


# Mapping from CoinGecko IDs to symbols
COIN_SYMBOLS = {
    "bitcoin": "BTC",
    "ethereum": "ETH",
    "solana": "SOL",
    "dogecoin": "DOGE",
    "cardano": "ADA",
    "ripple": "XRP",
    "polkadot": "DOT",
    "avalanche-2": "AVAX",
    "chainlink": "LINK",
    "polygon": "MATIC",
}


class PriceProducer:
    """Producer that streams crypto prices to Kafka and API."""

    TOPIC = "crypto.prices.raw"
    COINGECKO_API_URL = "https://api.coingecko.com/api/v3"
    API_URL = "http://localhost:8000"

    def __init__(self, settings: Optional[Settings] = None, api_url: Optional[str] = None):
        self.settings = settings or get_settings()
        self.api_url = api_url or self.API_URL
        self.kafka_enabled = False
        self.producer = None
        self.serializer = None
        
        self._setup_kafka_producer()

    def _setup_kafka_producer(self):
        """Initialize Kafka producer with Avro serialization."""
        if not KAFKA_AVAILABLE:
            logger.info("Running without Kafka - API-only mode")
            return
            
        try:
            # Kafka producer
            self.producer = Producer(self.settings.get_kafka_producer_config())

            # Schema Registry client
            schema_registry = SchemaRegistryClient(
                self.settings.get_schema_registry_config()
            )

            # Load Avro schema
            with open("schemas/crypto_price.avsc", "r") as f:
                schema_str = f.read()

            # Create Avro serializer
            self.serializer = AvroSerializer(
                schema_registry,
                schema_str,
                to_dict=lambda price, ctx: price,
            )

            self.kafka_enabled = True
            logger.info("Kafka producer initialized for prices")
        except Exception as e:
            logger.warning(f"Kafka setup failed: {e} - running in API-only mode")
            self.kafka_enabled = False

    def _delivery_callback(self, err, msg):
        """Callback for message delivery confirmation."""
        if err:
            logger.error(f"Delivery failed for {msg.key()}: {err}")
        else:
            logger.debug(
                f"Delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}"
            )

    def _get_headers(self) -> Dict[str, str]:
        """Get request headers with optional API key."""
        headers = {"accept": "application/json"}
        if self.settings.coingecko_api_key:
            headers["x-cg-demo-api-key"] = self.settings.coingecko_api_key
        return headers

    def fetch_prices(self) -> List[dict]:
        """
        Fetch current prices for tracked coins from CoinGecko.

        Returns:
            List of price dictionaries
        """
        coin_ids = ",".join(self.settings.tracked_coins)

        url = f"{self.COINGECKO_API_URL}/simple/price"
        params = {
            "ids": coin_ids,
            "vs_currencies": "usd",
            "include_24hr_vol": "true",
            "include_24hr_change": "true",
            "include_market_cap": "true",
            "include_last_updated_at": "true",
        }

        try:
            response = requests.get(
                url,
                params=params,
                headers=self._get_headers(),
                timeout=10,
            )
            response.raise_for_status()
            data = response.json()

            prices = []
            timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)

            for coin_id, price_data in data.items():
                prices.append({
                    "coin_id": coin_id,
                    "symbol": COIN_SYMBOLS.get(coin_id, coin_id.upper()[:4]),
                    "price_usd": price_data.get("usd", 0.0),
                    "volume_24h": price_data.get("usd_24h_vol", 0.0),
                    "price_change_24h_pct": price_data.get("usd_24h_change", 0.0),
                    "market_cap": price_data.get("usd_market_cap", 0.0),
                    "timestamp": timestamp,
                })

            return prices

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching prices: {e}")
            return []

    def _push_to_api(self, price: dict):
        """Push price data to the API for dashboard display."""
        try:
            # Convert timestamp to ISO format for API
            api_payload = {
                "coin_id": price["coin_id"],
                "symbol": price["symbol"],
                "price_usd": price["price_usd"],
                "volume_24h": price["volume_24h"],
                "price_change_24h_pct": price["price_change_24h_pct"],
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            
            response = requests.post(
                f"{self.api_url}/api/ingest/price",
                json=api_payload,
                timeout=5,
            )
            response.raise_for_status()
            logger.debug(f"Pushed {price['coin_id']} to API")
        except Exception as e:
            logger.warning(f"Failed to push to API: {e}")

    def publish_prices(self, prices: List[dict]):
        """
        Publish price data to Kafka and API.

        Args:
            prices: List of price dictionaries
        """
        for price in prices:
            # Always push to API for dashboard
            self._push_to_api(price)
            
            # Push to Kafka if enabled
            if self.kafka_enabled and self.producer and self.serializer:
                try:
                    value = self.serializer(
                        price,
                        SerializationContext(self.TOPIC, MessageField.VALUE),
                    )

                    self.producer.produce(
                        topic=self.TOPIC,
                        key=price["coin_id"].encode("utf-8"),
                        value=value,
                        callback=self._delivery_callback,
                    )
                except Exception as e:
                    logger.error(f"Kafka error for {price['coin_id']}: {e}")

            logger.info(
                f"💰 {price['symbol']}: ${price['price_usd']:,.2f} "
                f"({price['price_change_24h_pct']:+.2f}%)"
            )

        if self.kafka_enabled and self.producer:
            self.producer.poll(0)

    def run(self, interval_seconds: int = 30):
        """
        Run the price producer in a loop.

        Args:
            interval_seconds: Time between price fetches
        """
        mode = "Kafka + API" if self.kafka_enabled else "API-only"
        logger.info(
            f"Starting price producer ({mode}, interval: {interval_seconds}s) "
            f"for coins: {self.settings.tracked_coins}"
        )

        try:
            while True:
                prices = self.fetch_prices()

                if prices:
                    self.publish_prices(prices)
                    if self.kafka_enabled and self.producer:
                        self.producer.flush()
                else:
                    logger.warning("No prices fetched, retrying...")

                time.sleep(interval_seconds)

        except KeyboardInterrupt:
            logger.info("Price producer interrupted by user")
        finally:
            if self.kafka_enabled and self.producer:
                self.producer.flush()
            logger.info("Producer stopped")

    def fetch_once(self):
        """Fetch and publish prices once (for testing)."""
        prices = self.fetch_prices()
        if prices:
            self.publish_prices(prices)
            if self.kafka_enabled and self.producer:
                self.producer.flush()
        return prices


def main():
    """Entry point for price producer."""
    import argparse

    parser = argparse.ArgumentParser(description="Crypto Price Producer")
    parser.add_argument(
        "--interval",
        type=int,
        default=30,
        help="Interval between price fetches in seconds",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Fetch prices once and exit",
    )

    args = parser.parse_args()

    producer = PriceProducer()

    if args.once:
        producer.fetch_once()
    else:
        producer.run(interval_seconds=args.interval)


if __name__ == "__main__":
    main()


"""
Unified Data Collector

Orchestrates all API collectors to gather data at a single timestamp
and combine into unified records for analysis.

Strategy: At each polling interval:
1. Record the current timestamp
2. Call all registered APIs in parallel
3. Merge results into unified records (one per coin)
4. Stream to Confluent and save to CSV
"""

import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from loguru import logger

from config.settings import Settings, get_settings

# Import collectors
from producers.fear_greed_collector import FearGreedCollector
from producers.news_collector import NewsDataCollector
from producers.price_producer import PriceProducer


@dataclass
class UnifiedRecord:
    """
    A single timestamp-keyed record containing data from all sources.
    
    This is the canonical data format for causal analysis.
    """
    timestamp: datetime
    coin_id: str
    
    # Price data (from CoinGecko)
    price_usd: Optional[float] = None
    volume_24h: Optional[float] = None
    price_change_24h_pct: Optional[float] = None
    market_cap: Optional[float] = None
    
    # Fear & Greed Index (market-wide)
    fear_greed_value: Optional[int] = None
    fear_greed_classification: Optional[str] = None
    
    # News sentiment (from NewsData.io)
    news_count: int = 0
    news_sentiment_avg: Optional[float] = None
    news_positive_count: int = 0
    news_negative_count: int = 0
    
    # Metadata
    sources_available: List[str] = field(default_factory=list)
    collection_latency_ms: Optional[int] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "timestamp_ms": int(self.timestamp.timestamp() * 1000),
            "coin_id": self.coin_id,
            "price_usd": self.price_usd,
            "volume_24h": self.volume_24h,
            "price_change_24h_pct": self.price_change_24h_pct,
            "market_cap": self.market_cap,
            "fear_greed_value": self.fear_greed_value,
            "fear_greed_classification": self.fear_greed_classification,
            "news_count": self.news_count,
            "news_sentiment_avg": self.news_sentiment_avg,
            "news_positive_count": self.news_positive_count,
            "news_negative_count": self.news_negative_count,
            "sources_available": ",".join(self.sources_available),
            "collection_latency_ms": self.collection_latency_ms,
        }

    def to_flat_dict(self) -> Dict[str, Any]:
        """Convert to flat dictionary for CSV storage."""
        return {
            "timestamp": self.timestamp,
            "coin_id": self.coin_id,
            "price_usd": self.price_usd,
            "volume_24h": self.volume_24h,
            "return_pct": self.price_change_24h_pct,  # Alias for compatibility
            "market_cap": self.market_cap,
            "fear_greed_value": self.fear_greed_value,
            "fear_greed_classification": self.fear_greed_classification,
            "news_count": self.news_count,
            "news_sentiment_avg": self.news_sentiment_avg,
            "news_positive_count": self.news_positive_count,
            "news_negative_count": self.news_negative_count,
        }


class CoinGeckoCollector:
    """
    Wrapper for CoinGecko price data collection.
    
    Adapts the existing PriceProducer for use in unified collection.
    """
    
    COINGECKO_API_URL = "https://api.coingecko.com/api/v3"
    
    def __init__(self, settings: Settings):
        self.settings = settings
    
    def get_variables(self) -> List[str]:
        return ["price_usd", "volume_24h", "price_change_24h_pct", "market_cap"]
    
    def collect(self, coin_ids: List[str]) -> Dict[str, Dict[str, Any]]:
        """Fetch prices for all coins in one API call."""
        ids_str = ",".join(coin_ids)
        
        headers = {"accept": "application/json"}
        if self.settings.coingecko_api_key:
            headers["x-cg-demo-api-key"] = self.settings.coingecko_api_key
        
        try:
            response = requests.get(
                f"{self.COINGECKO_API_URL}/simple/price",
                params={
                    "ids": ids_str,
                    "vs_currencies": "usd",
                    "include_24hr_vol": "true",
                    "include_24hr_change": "true",
                    "include_market_cap": "true",
                },
                headers=headers,
                timeout=10,
            )
            response.raise_for_status()
            data = response.json()
            
            result = {}
            for coin_id, price_data in data.items():
                result[coin_id] = {
                    "price_usd": price_data.get("usd"),
                    "volume_24h": price_data.get("usd_24h_vol"),
                    "price_change_24h_pct": price_data.get("usd_24h_change"),
                    "market_cap": price_data.get("usd_market_cap"),
                }
            
            logger.debug(f"Collected prices for {len(result)} coins")
            return result
            
        except Exception as e:
            logger.error(f"CoinGecko API error: {e}")
            return {}


class UnifiedDataCollector:
    """
    Main collector that orchestrates all APIs and produces unified records.
    
    This implements the timestamp-based collection strategy:
    - At each interval, collect from ALL APIs
    - Merge into unified records (one per coin per timestamp)
    - Save to CSV and optionally stream to Confluent
    """
    
    UNIFIED_TOPIC = "agent.analytics_joined_kafka.raw"  # Match existing Confluent Cloud topic
    
    def __init__(
        self,
        settings: Optional[Settings] = None,
        data_dir: Optional[str] = None,
        api_url: str = "http://localhost:8000",
        unified_table: bool = True,
    ):
        self.settings = settings or get_settings()
        self.api_url = api_url
        self.unified_table = unified_table  # Save to single table vs. per-coin files
        
        # Data storage
        if data_dir is None:
            data_dir = "./data/canonical"
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize collectors
        self.collectors: Dict[str, Any] = {}
        self._register_collectors()
        
        # Kafka (optional)
        self.kafka_enabled = False
        self._setup_kafka()
        
        # History buffer for analysis
        self.history: List[UnifiedRecord] = []
        self.max_history = 1000
    
    def _register_collectors(self):
        """Register all available data collectors."""
        # CoinGecko - always available
        self.collectors["coingecko"] = CoinGeckoCollector(self.settings)
        logger.info("Registered collector: coingecko")
        
        # Fear & Greed - no auth required
        self.collectors["fear_greed"] = FearGreedCollector(self.settings)
        logger.info("Registered collector: fear_greed")
        
        # News - requires API key
        news_collector = NewsDataCollector(self.settings)
        if news_collector.api_key:
            self.collectors["news"] = news_collector
            logger.info("Registered collector: news")
        else:
            logger.warning("News collector not registered (no API key)")
    
    def _setup_kafka(self):
        """Initialize Kafka producer for unified topic."""
        try:
            from confluent_kafka import Producer
            from confluent_kafka.admin import AdminClient, NewTopic
            from confluent_kafka.schema_registry import SchemaRegistryClient
            from confluent_kafka.schema_registry.avro import AvroSerializer
            from confluent_kafka.serialization import SerializationContext, MessageField
            import json
            
            self.producer = Producer(self.settings.get_kafka_producer_config())
            
            # Check if topic exists, create if not
            admin_client = AdminClient(self.settings.get_kafka_producer_config())
            metadata = admin_client.list_topics(timeout=10)
            
            if self.UNIFIED_TOPIC not in metadata.topics:
                logger.info(f"Topic {self.UNIFIED_TOPIC} does not exist, creating it...")
                topic = NewTopic(
                    self.UNIFIED_TOPIC,
                    num_partitions=3,
                    replication_factor=3,  # Confluent Cloud requires RF=3
                    config={
                        "retention.ms": "604800000",  # 7 days
                    }
                )
                futures = admin_client.create_topics([topic])
                for topic_name, future in futures.items():
                    try:
                        future.result()  # Wait for creation
                        logger.info(f"Created topic: {topic_name}")
                    except Exception as e:
                        logger.error(f"Failed to create topic {topic_name}: {e}")
                        raise
            else:
                logger.info(f"Topic {self.UNIFIED_TOPIC} already exists")
            
            # Register unified schema
            schema_registry = SchemaRegistryClient(
                self.settings.get_schema_registry_config()
            )
            
            # Load schema from file
            schema_path = Path(__file__).parent.parent / "schemas" / "unified_market_data.avsc"
            if schema_path.exists():
                with open(schema_path) as f:
                    schema_str = f.read()
            else:
                # Fallback to inline schema
                unified_schema = {
                    "type": "record",
                    "name": "UnifiedMarketData",
                    "namespace": "com.cryptosentinel.analytics",
                    "fields": [
                        {"name": "timestamp_ms", "type": "long"},
                        {"name": "coin_id", "type": "string"},
                        {"name": "price_usd", "type": ["null", "double"], "default": None},
                        {"name": "volume_24h", "type": ["null", "double"], "default": None},
                        {"name": "price_change_24h_pct", "type": ["null", "double"], "default": None},
                        {"name": "market_cap", "type": ["null", "double"], "default": None},
                        {"name": "fear_greed_value", "type": ["null", "int"], "default": None},
                        {"name": "fear_greed_classification", "type": ["null", "string"], "default": None},
                        {"name": "news_count", "type": "int", "default": 0},
                        {"name": "news_sentiment_avg", "type": ["null", "double"], "default": None},
                        {"name": "news_positive_count", "type": "int", "default": 0},
                        {"name": "news_negative_count", "type": "int", "default": 0},
                        {"name": "sources_available", "type": "string"},
                        {"name": "collection_latency_ms", "type": ["null", "int"], "default": None},
                    ]
                }
                schema_str = json.dumps(unified_schema)
            
            self.serializer = AvroSerializer(
                schema_registry,
                schema_str,
                to_dict=lambda r, ctx: r,
            )
            
            # Store SerializationContext for later use
            self._serialization_context = SerializationContext(
                self.UNIFIED_TOPIC, MessageField.VALUE
            )
            
            self.kafka_enabled = True
            logger.info("Kafka producer initialized for unified topic")
            
        except ImportError:
            logger.info("Kafka libraries not available - running in local mode")
            self.kafka_enabled = False
        except Exception as e:
            logger.warning(f"Kafka setup failed: {e} - running in local mode")
            self.kafka_enabled = False
    
    def get_registered_collectors(self) -> List[str]:
        """Return list of registered collector names."""
        return list(self.collectors.keys())
    
    def collect_all_at_timestamp(self, coin_ids: List[str]) -> List[UnifiedRecord]:
        """
        Collect data from ALL APIs at the current timestamp.
        
        This is the core method implementing the timestamp-based strategy:
        1. Record the current timestamp
        2. Call all registered APIs in parallel
        3. Merge results into unified records (one per coin)
        """
        timestamp = datetime.now(timezone.utc)
        start_time = time.time()
        
        # Collect from all APIs in parallel
        api_results: Dict[str, Dict[str, Dict]] = {}
        
        with ThreadPoolExecutor(max_workers=len(self.collectors)) as executor:
            futures = {
                executor.submit(collector.collect, coin_ids): name
                for name, collector in self.collectors.items()
            }
            
            for future in as_completed(futures):
                api_name = futures[future]
                try:
                    api_results[api_name] = future.result()
                    logger.debug(f"Collected from {api_name}")
                except Exception as e:
                    logger.error(f"Collector {api_name} failed: {e}")
                    api_results[api_name] = {}
        
        collection_latency = int((time.time() - start_time) * 1000)
        
        # Merge results into unified records
        records = []
        for coin_id in coin_ids:
            record = UnifiedRecord(
                timestamp=timestamp,
                coin_id=coin_id,
                collection_latency_ms=collection_latency,
            )
            
            # Merge from CoinGecko
            if "coingecko" in api_results and coin_id in api_results["coingecko"]:
                cg_data = api_results["coingecko"][coin_id]
                record.price_usd = cg_data.get("price_usd")
                record.volume_24h = cg_data.get("volume_24h")
                record.price_change_24h_pct = cg_data.get("price_change_24h_pct")
                record.market_cap = cg_data.get("market_cap")
                record.sources_available.append("coingecko")
            
            # Merge from Fear & Greed
            if "fear_greed" in api_results and coin_id in api_results["fear_greed"]:
                fg_data = api_results["fear_greed"][coin_id]
                record.fear_greed_value = fg_data.get("fear_greed_value")
                record.fear_greed_classification = fg_data.get("fear_greed_classification")
                record.sources_available.append("fear_greed")
            
            # Merge from News
            if "news" in api_results and coin_id in api_results["news"]:
                news_data = api_results["news"][coin_id]
                record.news_count = news_data.get("news_count", 0)
                record.news_sentiment_avg = news_data.get("news_sentiment_avg")
                record.news_positive_count = news_data.get("news_positive_count", 0)
                record.news_negative_count = news_data.get("news_negative_count", 0)
                record.sources_available.append("news")
            
            records.append(record)
            
            # Add to history buffer
            self.history.append(record)
            if len(self.history) > self.max_history:
                self.history = self.history[-self.max_history:]
        
        return records
    
    def save_to_csv(self, records: List[UnifiedRecord], append: bool = True, unified_table: bool = True):
        """
        Save unified records to CSV files.
        
        Args:
            records: List of unified records
            append: If True, append to existing files
            unified_table: If True, save all coins to one table; if False, one file per coin
        """
        if not records:
            return
        
        if unified_table:
            # Save ALL coins to a SINGLE unified table
            all_records = [record.to_flat_dict() for record in records]
            df = pd.DataFrame(all_records)
            
            filepath = self.data_dir / "unified_market_data.csv"
            
            if append and filepath.exists():
                # Append to existing unified table
                try:
                    existing_df = pd.read_csv(filepath, parse_dates=["timestamp"])
                    # Check if file is empty or only has headers
                    if len(existing_df) == 0:
                        # File exists but is empty, just write new data
                        df = df.sort_values(["timestamp", "coin_id"])
                        df.to_csv(filepath, index=False)
                        logger.debug(f"Saved {len(df)} records to empty unified table: {filepath}")
                    else:
                        # File has data, append and deduplicate
                        combined_df = pd.concat([existing_df, df], ignore_index=True)
                        # Remove duplicates based on timestamp + coin_id
                        combined_df = combined_df.drop_duplicates(
                            subset=["timestamp", "coin_id"], keep="last"
                        )
                        combined_df = combined_df.sort_values(["timestamp", "coin_id"])
                        combined_df.to_csv(filepath, index=False)
                        logger.debug(f"Appended {len(df)} records to unified table: {filepath}")
                except (pd.errors.EmptyDataError, ValueError) as e:
                    # File exists but is corrupted or empty, overwrite it
                    logger.warning(f"Existing CSV file is empty or corrupted, overwriting: {e}")
                    df = df.sort_values(["timestamp", "coin_id"])
                    df.to_csv(filepath, index=False)
                    logger.debug(f"Saved {len(df)} records to unified table (overwrote corrupted file): {filepath}")
            else:
                df = df.sort_values(["timestamp", "coin_id"])
                df.to_csv(filepath, index=False)
                logger.debug(f"Saved {len(df)} records to unified table: {filepath}")
        else:
            # Group by coin (original behavior - one file per coin)
            coin_records: Dict[str, List[Dict]] = {}
            for record in records:
                if record.coin_id not in coin_records:
                    coin_records[record.coin_id] = []
                coin_records[record.coin_id].append(record.to_flat_dict())
            
            # Save each coin to its own file
            for coin_id, record_dicts in coin_records.items():
                df = pd.DataFrame(record_dicts)
                
                filepath = self.data_dir / f"{coin_id}_unified.csv"
                
                if append and filepath.exists():
                    # Append to existing
                    try:
                        existing_df = pd.read_csv(filepath, parse_dates=["timestamp"])
                        # Check if file is empty or only has headers
                        if len(existing_df) == 0:
                            # File exists but is empty, just write new data
                            df.to_csv(filepath, index=False)
                            logger.debug(f"Saved {len(df)} records to empty file: {filepath}")
                        else:
                            # File has data, append and deduplicate
                            combined_df = pd.concat([existing_df, df], ignore_index=True)
                            # Remove duplicates based on timestamp
                            combined_df = combined_df.drop_duplicates(
                                subset=["timestamp", "coin_id"], keep="last"
                            )
                            combined_df = combined_df.sort_values("timestamp")
                            combined_df.to_csv(filepath, index=False)
                            logger.debug(f"Appended {len(df)} records to {filepath}")
                    except (pd.errors.EmptyDataError, ValueError) as e:
                        # File exists but is corrupted or empty, overwrite it
                        logger.warning(f"Existing CSV file is empty or corrupted, overwriting: {e}")
                        df.to_csv(filepath, index=False)
                        logger.debug(f"Saved {len(df)} records (overwrote corrupted file): {filepath}")
                else:
                    df.to_csv(filepath, index=False)
                    logger.debug(f"Saved {len(df)} records to {filepath}")
    
    def publish_to_kafka(self, records: List[UnifiedRecord]):
        """Publish unified records to Confluent Kafka."""
        if not self.kafka_enabled or not hasattr(self, 'serializer') or self.serializer is None:
            return
        
        for record in records:
            try:
                value = self.serializer(
                    record.to_dict(),
                    self._serialization_context,
                )
                
                self.producer.produce(
                    topic=self.UNIFIED_TOPIC,
                    key=f"{record.coin_id}:{record.timestamp.isoformat()}".encode(),
                    value=value,
                )
            except Exception as e:
                logger.error(f"Failed to publish record to Kafka: {e}")
        
        self.producer.poll(0)
    
    def push_to_api(self, records: List[UnifiedRecord]):
        """Push unified records to the API for real-time dashboard."""
        for record in records:
            try:
                response = requests.post(
                    f"{self.api_url}/api/ingest/unified",
                    json=record.to_dict(),
                    timeout=5,
                )
                if response.status_code != 200:
                    logger.warning(f"API push returned {response.status_code}")
            except Exception as e:
                logger.debug(f"API push skipped (API may not be running): {e}")
    
    def get_dataframe(self, coin_id: Optional[str] = None) -> pd.DataFrame:
        """
        Get collected data as a DataFrame.
        
        Args:
            coin_id: Filter to specific coin (optional)
            
        Returns:
            DataFrame with all collected records
        """
        records = [r.to_flat_dict() for r in self.history]
        df = pd.DataFrame(records)
        
        if coin_id and not df.empty:
            df = df[df["coin_id"] == coin_id]
        
        return df
    
    def load_from_csv(self, coin_id: Optional[str] = None) -> Optional[pd.DataFrame]:
        """
        Load historical data from CSV.
        
        Args:
            coin_id: If provided, filter to specific coin. If None, load all coins.
            
        Returns:
            DataFrame with historical data
        """
        if self.unified_table:
            # Load from unified table
            filepath = self.data_dir / "unified_market_data.csv"
        else:
            # Load from per-coin file
            if coin_id is None:
                logger.warning("coin_id required when using per-coin files")
                return None
            filepath = self.data_dir / f"{coin_id}_unified.csv"
        
        if not filepath.exists():
            logger.debug(f"CSV file not found: {filepath}")
            return None
        
        try:
            df = pd.read_csv(filepath, parse_dates=["timestamp"])
            
            # Check if file is empty
            if len(df) == 0:
                logger.debug(f"CSV file is empty: {filepath}")
                return None
            
            # Filter by coin if requested
            if coin_id and "coin_id" in df.columns:
                df = df[df["coin_id"] == coin_id]
            
            logger.info(f"Loaded {len(df)} records from {filepath}")
            return df
        except (pd.errors.EmptyDataError, ValueError) as e:
            logger.warning(f"CSV file is empty or corrupted: {filepath} - {e}")
            return None
        except Exception as e:
            logger.error(f"Failed to load CSV: {e}")
            return None
    
    def collect_once(self, coin_ids: Optional[List[str]] = None) -> List[UnifiedRecord]:
        """
        Collect data once (for testing or manual triggering).
        
        Args:
            coin_ids: Coins to collect for (default: from settings)
            
        Returns:
            List of unified records
        """
        if coin_ids is None:
            coin_ids = self.settings.tracked_coins
        
        records = self.collect_all_at_timestamp(coin_ids)
        self.save_to_csv(records, unified_table=self.unified_table)
        self.push_to_api(records)
        
        if self.kafka_enabled:
            self.publish_to_kafka(records)
            self.producer.flush()
        
        return records
    
    def run(self, interval_seconds: int = 60, coin_ids: Optional[List[str]] = None):
        """
        Main loop: collect from all APIs at each interval.
        
        Args:
            interval_seconds: Time between collections
            coin_ids: Coins to collect for (default: from settings)
        """
        if coin_ids is None:
            coin_ids = self.settings.tracked_coins
        
        logger.info(
            f"Starting unified collector (interval={interval_seconds}s) "
            f"for coins: {coin_ids}"
        )
        logger.info(f"Registered collectors: {self.get_registered_collectors()}")
        logger.info(f"Data directory: {self.data_dir}")
        
        try:
            while True:
                # Collect all data at this timestamp
                records = self.collect_all_at_timestamp(coin_ids)
                
                # Stream to all destinations
                self.save_to_csv(records, unified_table=self.unified_table)
                self.push_to_api(records)
                
                if self.kafka_enabled:
                    self.publish_to_kafka(records)
                    self.producer.flush()
                
                # Log summary
                for record in records:
                    emoji = "🟢" if (record.fear_greed_value or 50) >= 50 else "🔴"
                    price_str = f"${record.price_usd:,.2f}" if record.price_usd else "N/A"
                    fng_str = f"{record.fear_greed_value}" if record.fear_greed_value else "N/A"
                    news_str = f"{record.news_count} articles" if record.news_count else "no news"
                    
                    logger.info(
                        f"{emoji} {record.coin_id.upper()}: {price_str} | "
                        f"F&G: {fng_str} | {news_str} | "
                        f"latency: {record.collection_latency_ms}ms"
                    )
                
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            logger.info("Collector stopped by user")
        finally:
            if self.kafka_enabled and hasattr(self, 'producer'):
                self.producer.flush()
            logger.info("Unified collector stopped")


def main():
    """Entry point for unified collector."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Unified Data Collector")
    parser.add_argument(
        "--interval",
        type=int,
        default=60,
        help="Collection interval in seconds (default: 60)",
    )
    parser.add_argument(
        "--coins",
        type=str,
        default=None,
        help="Comma-separated list of coins (default: from settings)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Collect once and exit",
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default="./data/canonical",
        help="Directory for CSV output",
    )
    
    args = parser.parse_args()
    
    # Parse coins
    coin_ids = None
    if args.coins:
        coin_ids = [c.strip() for c in args.coins.split(",")]
    
    # Create collector
    collector = UnifiedDataCollector(data_dir=args.data_dir)
    
    if args.once:
        print("\n=== Collecting data once ===")
        records = collector.collect_once(coin_ids)
        
        print(f"\nCollected {len(records)} records:")
        for record in records:
            print(f"\n{record.coin_id.upper()}:")
            print(f"  Price: ${record.price_usd:,.2f}" if record.price_usd else "  Price: N/A")
            print(f"  24h Change: {record.price_change_24h_pct:+.2f}%" if record.price_change_24h_pct else "  24h Change: N/A")
            print(f"  Fear & Greed: {record.fear_greed_value} ({record.fear_greed_classification})" if record.fear_greed_value else "  Fear & Greed: N/A")
            print(f"  News: {record.news_count} articles, sentiment: {record.news_sentiment_avg}" if record.news_count else "  News: N/A")
            print(f"  Sources: {record.sources_available}")
    else:
        collector.run(interval_seconds=args.interval, coin_ids=coin_ids)


if __name__ == "__main__":
    main()


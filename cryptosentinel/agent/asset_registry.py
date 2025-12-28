"""
Asset Registry: Catalog of existing data sources, variables, and Kafka topics.

This registry grounds hypothesis generation and data source discovery to
only use variables and sources that actually exist in the system.
"""

from typing import Dict, List, Optional
from dataclasses import dataclass, field

from .models import Variable, DataSource, VariableType, DataSourceType


@dataclass
class TopicSchema:
    """Schema information for a Kafka topic."""
    topic_name: str
    namespace: str
    fields: List[Dict[str, any]]  # Avro field definitions
    description: str = ""


@dataclass
class AssetRegistry:
    """
    Registry of all available data assets in the system.
    
    This includes:
    - Kafka topics and their schemas
    - Variables available from each source
    - In-memory data stores
    """
    
    # Kafka topics
    topics: Dict[str, TopicSchema] = field(default_factory=dict)
    
    # Variables indexed by name
    variables: Dict[str, Variable] = field(default_factory=dict)
    
    # Data sources indexed by name
    data_sources: Dict[str, DataSource] = field(default_factory=dict)
    
    # Domain-specific groupings
    domain_variables: Dict[str, List[str]] = field(default_factory=dict)
    
    def __post_init__(self):
        """Initialize registry with existing assets after dataclass fields are set."""
        # Dataclass will populate dict fields via default_factory; ensure they exist before use.
        self.topics = self.topics or {}
        self.variables = self.variables or {}
        self.data_sources = self.data_sources or {}
        self.domain_variables = self.domain_variables or {}
        self._initialize_crypto_assets()
    
    def _initialize_crypto_assets(self):
        """Initialize cryptocurrency domain assets."""
        domain = "cryptocurrency"
        
        # Register Kafka topics
        self._register_crypto_price_topic()
        self._register_reddit_enriched_topic()
        self._register_analytics_joined_topic()
        
        # Register variables from crypto.prices.raw
        price_vars = [
            Variable(
                name="price_usd",
                description="Cryptocurrency price in USD",
                variable_type=VariableType.CONTINUOUS,
                unit="USD",
                domain=domain,
                metadata={"source": "crypto.prices.raw", "field": "price_usd"}
            ),
            Variable(
                name="volume_24h",
                description="24-hour trading volume in USD",
                variable_type=VariableType.CONTINUOUS,
                unit="USD",
                domain=domain,
                metadata={"source": "crypto.prices.raw", "field": "volume_24h"}
            ),
            Variable(
                name="price_change_24h_pct",
                description="24-hour price change percentage (return)",
                variable_type=VariableType.CONTINUOUS,
                unit="percent",
                domain=domain,
                metadata={"source": "crypto.prices.raw", "field": "price_change_24h_pct"}
            ),
            Variable(
                name="market_cap",
                description="Market capitalization in USD",
                variable_type=VariableType.CONTINUOUS,
                unit="USD",
                domain=domain,
                metadata={"source": "crypto.prices.raw", "field": "market_cap"}
            ),
            Variable(
                name="coin_id",
                description="Cryptocurrency identifier",
                variable_type=VariableType.CATEGORICAL,
                domain=domain,
                metadata={"source": "crypto.prices.raw", "field": "coin_id"}
            ),
        ]
        
        # Register variables from social.reddit.enriched (legacy) and unified collector (new)
        # Unified collector uses News API, but we maintain backward compatibility with Reddit variable names
        sentiment_vars = [
            Variable(
                name="avg_sentiment",
                description="Average sentiment score from news articles or Reddit posts (-1.0 to 1.0)",
                variable_type=VariableType.CONTINUOUS,
                unit="score",
                domain=domain,
                metadata={"source": "unified_market_data", "field": "news_sentiment_avg", "legacy": "social.reddit.enriched"}
            ),
            Variable(
                name="sentiment_score",
                description="Individual post/article sentiment score (-1.0 to 1.0)",
                variable_type=VariableType.CONTINUOUS,
                unit="score",
                domain=domain,
                metadata={"source": "unified_market_data", "field": "news_sentiment_avg"}
            ),
            Variable(
                name="post_count",
                description="Number of news articles or Reddit posts mentioning a coin",
                variable_type=VariableType.DISCRETE,
                unit="count",
                domain=domain,
                metadata={"source": "unified_market_data", "field": "news_count", "legacy": "social.reddit.enriched"}
            ),
            Variable(
                name="positive_count",
                description="Number of positive sentiment news articles or posts",
                variable_type=VariableType.DISCRETE,
                unit="count",
                domain=domain,
                metadata={"source": "unified_market_data", "field": "news_positive_count", "legacy": "social.reddit.enriched"}
            ),
            Variable(
                name="negative_count",
                description="Number of negative sentiment news articles or posts",
                variable_type=VariableType.DISCRETE,
                unit="count",
                domain=domain,
                metadata={"source": "unified_market_data", "field": "news_negative_count", "legacy": "social.reddit.enriched"}
            ),
            # Unified CSV column names (for direct access)
            Variable(
                name="news_sentiment_avg",
                description="Average sentiment score from news articles (-1.0 to 1.0)",
                variable_type=VariableType.CONTINUOUS,
                unit="score",
                domain=domain,
                metadata={"source": "unified_market_data", "field": "news_sentiment_avg"}
            ),
            Variable(
                name="news_count",
                description="Number of news articles mentioning a coin",
                variable_type=VariableType.DISCRETE,
                unit="count",
                domain=domain,
                metadata={"source": "unified_market_data", "field": "news_count"}
            ),
            Variable(
                name="news_positive_count",
                description="Number of positive sentiment news articles",
                variable_type=VariableType.DISCRETE,
                unit="count",
                domain=domain,
                metadata={"source": "unified_market_data", "field": "news_positive_count"}
            ),
            Variable(
                name="news_negative_count",
                description="Number of negative sentiment news articles",
                variable_type=VariableType.DISCRETE,
                unit="count",
                domain=domain,
                metadata={"source": "unified_market_data", "field": "news_negative_count"}
            ),
        ]
        
        # Register Fear & Greed Index variables (new)
        fear_greed_vars = [
            Variable(
                name="fear_greed_value",
                description="Fear & Greed Index value (0-100 scale, market-wide indicator)",
                variable_type=VariableType.CONTINUOUS,
                unit="index",
                domain=domain,
                metadata={"source": "unified_market_data", "field": "fear_greed_value", "range": "0-100"}
            ),
            Variable(
                name="fear_greed_classification",
                description="Fear & Greed Index classification (e.g., 'Extreme Fear', 'Greed')",
                variable_type=VariableType.CATEGORICAL,
                unit="label",
                domain=domain,
                metadata={"source": "unified_market_data", "field": "fear_greed_classification"}
            ),
        ]
        
        # Register all variables
        for var in price_vars + sentiment_vars + fear_greed_vars:
            self.register_variable(var)
        
        # Register data sources
        price_source = DataSource(
            name="crypto_prices_kafka",
            source_type=DataSourceType.KAFKA_TOPIC,
            endpoint="crypto.prices.raw",
            kafka_topic="crypto.prices.raw",
            variables=price_vars,
            metadata={
                "description": "Raw cryptocurrency price data from CoinGecko",
                "retention_days": 7,
                "partitions": 3
            }
        )
        
        sentiment_source = DataSource(
            name="reddit_sentiment_kafka",
            source_type=DataSourceType.KAFKA_TOPIC,
            endpoint="social.reddit.enriched",
            kafka_topic="social.reddit.enriched",
            variables=sentiment_vars,
            metadata={
                "description": "Reddit posts enriched with sentiment analysis",
                "retention_days": 14,
                "partitions": 6
            }
        )
        
        joined_source = DataSource(
            name="analytics_joined_kafka",
            source_type=DataSourceType.KAFKA_TOPIC,
            endpoint="analytics.joined",
            kafka_topic="analytics.joined",
            variables=price_vars + sentiment_vars,
            metadata={
                "description": "Joined price and sentiment data for analysis",
                "retention_days": 30,
                "partitions": 6
            }
        )
        
        # Unified market data source (CSV + Kafka)
        unified_source = DataSource(
            name="unified_market_data",
            source_type=DataSourceType.FILE,
            endpoint="data/canonical/unified_market_data.csv",
            kafka_topic="agent.analytics_joined_kafka.raw",
            variables=price_vars + sentiment_vars + fear_greed_vars,
            metadata={
                "description": "Unified timestamp-based market data (CSV + Kafka)",
                "format": "unified_table",
                "sources": ["CoinGecko", "Fear & Greed Index", "NewsData.io"]
            }
        )
        
        # Register in-memory data store as a source
        in_memory_source = DataSource(
            name="in_memory_datastore",
            source_type=DataSourceType.DATABASE,
            endpoint="api/main.py:DataStore",
            variables=price_vars + sentiment_vars,
            metadata={
                "description": "In-memory data store with price_history and sentiment_history",
                "max_history": 1000
            }
        )
        
        for source in [price_source, sentiment_source, joined_source, unified_source, in_memory_source]:
            self.register_data_source(source)
        
        # Group by domain
        self.domain_variables[domain] = [v.name for v in price_vars + sentiment_vars + fear_greed_vars]
    
    def _register_crypto_price_topic(self):
        """Register crypto.prices.raw topic schema."""
        self.topics["crypto.prices.raw"] = TopicSchema(
            topic_name="crypto.prices.raw",
            namespace="com.cryptosentinel.market",
            fields=[
                {"name": "coin_id", "type": "string"},
                {"name": "symbol", "type": "string"},
                {"name": "price_usd", "type": "double"},
                {"name": "volume_24h", "type": "double"},
                {"name": "price_change_24h_pct", "type": "double"},
                {"name": "market_cap", "type": "double"},
                {"name": "timestamp", "type": "long", "logicalType": "timestamp-millis"},
            ],
            description="Raw cryptocurrency price data from CoinGecko"
        )
    
    def _register_reddit_enriched_topic(self):
        """Register social.reddit.enriched topic schema."""
        self.topics["social.reddit.enriched"] = TopicSchema(
            topic_name="social.reddit.enriched",
            namespace="com.cryptosentinel.social",
            fields=[
                {"name": "post_id", "type": "string"},
                {"name": "subreddit", "type": "string"},
                {"name": "title", "type": "string"},
                {"name": "body", "type": ["null", "string"]},
                {"name": "score", "type": "int"},
                {"name": "num_comments", "type": "int"},
                {"name": "author", "type": "string"},
                {"name": "url", "type": "string"},
                {"name": "created_utc", "type": "long", "logicalType": "timestamp-millis"},
                {"name": "sentiment_score", "type": "double"},
                {"name": "sentiment_label", "type": "enum", "symbols": ["POSITIVE", "NEGATIVE", "NEUTRAL"]},
                {"name": "sentiment_confidence", "type": "double"},
                {"name": "mentioned_coins", "type": "array", "items": "string"},
                {"name": "topics", "type": "array", "items": "string"},
                {"name": "processed_at", "type": "long", "logicalType": "timestamp-millis"},
            ],
            description="Reddit posts enriched with Gemini sentiment analysis"
        )
    
    def _register_analytics_joined_topic(self):
        """Register analytics.joined topic schema."""
        self.topics["analytics.joined"] = TopicSchema(
            topic_name="analytics.joined",
            namespace="com.cryptosentinel.analytics",
            fields=[
                {"name": "coin_id", "type": "string"},
                {"name": "price_usd", "type": "double"},
                {"name": "price_change_pct", "type": "double"},
                {"name": "volume_24h", "type": "double"},
                {"name": "avg_sentiment", "type": "double"},
                {"name": "post_count", "type": "int"},
                {"name": "positive_count", "type": "int"},
                {"name": "negative_count", "type": "int"},
                {"name": "timestamp", "type": "long", "logicalType": "timestamp-millis"},
            ],
            description="Joined price and sentiment data for causal analysis"
        )
    
    def register_variable(self, variable: Variable):
        """Register a variable."""
        key = self._make_variable_key(variable)
        self.variables[key] = variable
        
        # Add to domain grouping
        if variable.domain:
            if variable.domain not in self.domain_variables:
                self.domain_variables[variable.domain] = []
            if variable.name not in self.domain_variables[variable.domain]:
                self.domain_variables[variable.domain].append(variable.name)
    
    def register_data_source(self, source: DataSource):
        """Register a data source."""
        self.data_sources[source.name] = source
    
    def get_variable(self, name: str, domain: Optional[str] = None) -> Optional[Variable]:
        """Get a variable by name."""
        if domain:
            key = f"{domain}:{name}"
        else:
            # Try to find in any domain
            key = None
            for k, v in self.variables.items():
                if v.name == name:
                    key = k
                    break
        
        return self.variables.get(key) if key else None
    
    def get_variables_by_domain(self, domain: str) -> List[Variable]:
        """Get all variables for a domain."""
        var_names = self.domain_variables.get(domain, [])
        return [self.variables[self._make_variable_key_from_name(name, domain)] 
                for name in var_names 
                if self._make_variable_key_from_name(name, domain) in self.variables]
    
    def get_data_source(self, name: str) -> Optional[DataSource]:
        """Get a data source by name."""
        return self.data_sources.get(name)
    
    def get_data_sources_for_variable(self, variable_name: str, domain: Optional[str] = None) -> List[DataSource]:
        """Get all data sources that provide a variable."""
        var = self.get_variable(variable_name, domain)
        if not var:
            return []
        
        sources = []
        for source in self.data_sources.values():
            if var in source.variables:
                sources.append(source)
        
        return sources
    
    def list_available_variables(self, domain: Optional[str] = None) -> List[Variable]:
        """List all available variables, optionally filtered by domain."""
        if domain:
            return self.get_variables_by_domain(domain)
        return list(self.variables.values())
    
    def list_available_data_sources(self) -> List[DataSource]:
        """List all available data sources."""
        return list(self.data_sources.values())
    
    def _make_variable_key(self, variable: Variable) -> str:
        """Create a unique key for a variable."""
        return f"{variable.domain or 'default'}:{variable.name}"
    
    def _make_variable_key_from_name(self, name: str, domain: str) -> str:
        """Create a variable key from name and domain."""
        return f"{domain}:{name}"


# Global singleton instance
_global_registry: Optional[AssetRegistry] = None


def get_asset_registry() -> AssetRegistry:
    """Get the global asset registry instance."""
    global _global_registry
    if _global_registry is None:
        _global_registry = AssetRegistry()
    return _global_registry


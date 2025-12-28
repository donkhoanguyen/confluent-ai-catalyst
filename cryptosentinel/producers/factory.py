"""
Factory for creating producers dynamically based on data source type.
"""

from typing import Optional
from loguru import logger

from agent.models import DataSource, DataSourceType
from config.settings import Settings

# Import existing producers
from .reddit_producer import RedditProducer
from .price_producer import PriceProducer


class ProducerFactory:
    """Factory for creating data source producers."""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self._producers = {}
    
    def create_producer(self, source: DataSource):
        """
        Create a producer for a data source.
        
        Args:
            source: DataSource to create producer for
            
        Returns:
            Producer instance
        """
        # Check cache
        if source.name in self._producers:
            return self._producers[source.name]
        
        # Create based on source type
        if source.source_type == DataSourceType.API_REST:
            producer = self._create_rest_producer(source)
        elif source.source_type == DataSourceType.API_WEBSOCKET:
            producer = self._create_websocket_producer(source)
        elif source.source_type == DataSourceType.KAFKA_TOPIC:
            producer = self._create_kafka_consumer_producer(source)
        else:
            raise ValueError(f"Unsupported source type: {source.source_type}")
        
        self._producers[source.name] = producer
        return producer
    
    def _create_rest_producer(self, source: DataSource):
        """Create a REST API producer."""
        from .base_producer import BaseProducer
        
        class RESTProducer(BaseProducer):
            def __init__(self, source, settings):
                super().__init__(source, settings)
                self.endpoint = source.endpoint
                self.api_key = source.api_key
            
            def fetch_data(self):
                """Fetch data from REST API."""
                import requests
                headers = {}
                if self.api_key:
                    headers["Authorization"] = f"Bearer {self.api_key}"
                
                response = requests.get(self.endpoint, headers=headers, timeout=10)
                response.raise_for_status()
                return response.json()
        
        return RESTProducer(source, self.settings)
    
    def _create_websocket_producer(self, source: DataSource):
        """Create a WebSocket producer."""
        from .base_producer import BaseProducer
        
        class WebSocketProducer(BaseProducer):
            def __init__(self, source, settings):
                super().__init__(source, settings)
                self.endpoint = source.endpoint
            
            def fetch_data(self):
                """Fetch data from WebSocket (would be async in real implementation)."""
                # WebSocket implementation would be async
                logger.warning("WebSocket producer not fully implemented")
                return {}
        
        return WebSocketProducer(source, self.settings)
    
    def _create_kafka_consumer_producer(self, source: DataSource):
        """Create a producer that reads from existing Kafka topic."""
        from .base_producer import BaseProducer
        
        class KafkaConsumerProducer(BaseProducer):
            def __init__(self, source, settings):
                super().__init__(source, settings)
                self.topic = source.kafka_topic or source.name
            
            def fetch_data(self):
                """Consume from Kafka topic."""
                # This would consume from Kafka and republish
                logger.warning("Kafka consumer producer not fully implemented")
                return {}
        
        return KafkaConsumerProducer(source, self.settings)


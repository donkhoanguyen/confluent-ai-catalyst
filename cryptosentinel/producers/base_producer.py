"""
Base producer class for all data sources.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from datetime import datetime
from loguru import logger

from agent.models import DataSource
from config.settings import Settings

try:
    from confluent_kafka import Producer
    from confluent_kafka.serialization import SerializationContext, MessageField
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroSerializer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    logger.warning("Kafka libraries not available")


class BaseProducer(ABC):
    """Abstract base class for all producers."""
    
    def __init__(self, source: DataSource, settings: Settings):
        self.source = source
        self.settings = settings
        self.kafka_enabled = False
        self.producer = None
        self.serializer = None
        
        if KAFKA_AVAILABLE and source.kafka_topic:
            self._setup_kafka()
    
    def _setup_kafka(self):
        """Setup Kafka producer and serializer."""
        try:
            self.producer = Producer(self.settings.get_kafka_producer_config())
            
            schema_registry = SchemaRegistryClient(
                self.settings.get_schema_registry_config()
            )
            
            # Generate schema from source
            from agent.domain import SchemaTemplate
            schema = SchemaTemplate.for_data_source(self.source)
            
            import json
            schema_str = json.dumps(schema)
            
            self.serializer = AvroSerializer(
                schema_registry,
                schema_str,
                to_dict=lambda data, ctx: data,
            )
            
            self.kafka_enabled = True
            logger.info(f"Kafka producer initialized for {self.source.name}")
            
        except Exception as e:
            logger.warning(f"Kafka setup failed for {self.source.name}: {e}")
            self.kafka_enabled = False
    
    @abstractmethod
    def fetch_data(self) -> Dict[str, Any]:
        """Fetch data from the source."""
        pass
    
    def publish(self, data: Dict[str, Any]):
        """Publish data to Kafka."""
        if not self.kafka_enabled:
            logger.debug(f"Kafka not enabled for {self.source.name}")
            return
        
        try:
            # Add timestamp
            data["timestamp"] = int(datetime.utcnow().timestamp() * 1000)
            
            # Serialize
            value = self.serializer(
                data,
                SerializationContext(self.source.kafka_topic, MessageField.VALUE),
            )
            
            # Produce
            key = data.get("id", self.source.name).encode("utf-8")
            self.producer.produce(
                topic=self.source.kafka_topic,
                key=key,
                value=value,
                callback=self._delivery_callback,
            )
            
            self.producer.poll(0)
            
        except Exception as e:
            logger.error(f"Error publishing data: {e}")
    
    def _delivery_callback(self, err, msg):
        """Callback for message delivery."""
        if err:
            logger.error(f"Delivery failed: {err}")
        else:
            logger.debug(f"Delivered to {msg.topic()}")
    
    def run(self, interval_seconds: int = 30):
        """Run producer in a loop."""
        logger.info(f"Starting producer for {self.source.name}")
        
        try:
            while True:
                data = self.fetch_data()
                if data:
                    self.publish(data)
                
                import time
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            logger.info("Producer interrupted")
        finally:
            if self.producer:
                self.producer.flush()


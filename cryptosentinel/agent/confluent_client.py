"""
Confluent integration client supporting both MCP servers and Admin API.
"""

from typing import Optional, Dict, Any
import requests
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from loguru import logger

from .models import DataSource, Variable
from .domain import SchemaTemplate
from config.settings import Settings


class ConfluentClient:
    """
    Client for Confluent operations (MCP or Admin API).
    Supports dynamic topic creation, schema registration, and ksqlDB operations.
    """
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.mcp_server_url = getattr(settings, 'mcp_server_url', None)
        self.use_mcp = self.mcp_server_url is not None
        
        # Initialize Admin Client for Kafka operations
        self.admin_client = AdminClient(settings.get_kafka_producer_config())
        
        # Initialize Schema Registry client
        self.schema_registry = SchemaRegistryClient(
            settings.get_schema_registry_config()
        )
    
    def create_topic_for_source(self, source: DataSource) -> str:
        """
        Create a Kafka topic for a data source.
        
        Args:
            source: DataSource to create topic for
            
        Returns:
            Topic name
        """
        topic_name = f"agent.{source.name.lower().replace(' ', '_')}.raw"
        
        if self.use_mcp:
            return self._create_topic_via_mcp(topic_name, source)
        else:
            return self._create_topic_via_admin(topic_name, source)
    
    def _create_topic_via_mcp(self, topic_name: str, source: DataSource) -> str:
        """Create topic using MCP server."""
        try:
            # MCP server call (if available)
            # This would use the MCP protocol to call the Confluent MCP server
            # For now, fallback to Admin API
            logger.info(f"Attempting MCP topic creation for {topic_name}")
            # TODO: Implement MCP client call when MCP server is available
            return self._create_topic_via_admin(topic_name, source)
        except Exception as e:
            logger.warning(f"MCP topic creation failed, falling back to Admin API: {e}")
            return self._create_topic_via_admin(topic_name, source)
    
    def _create_topic_via_admin(self, topic_name: str, source: DataSource) -> str:
        """Create topic using Admin API."""
        try:
            # Check if topic already exists
            metadata = self.admin_client.list_topics(timeout=10)
            if topic_name in metadata.topics:
                logger.info(f"Topic {topic_name} already exists")
                return topic_name
            
            # Create new topic
            topic = NewTopic(
                topic_name,
                num_partitions=3,
                replication_factor=3,  # Confluent Cloud policy requires RF=3
                config={
                    "retention.ms": "604800000",  # 7 days
                }
            )
            
            futures = self.admin_client.create_topics([topic])
            
            # Wait for creation
            for topic_name_future, future in futures.items():
                try:
                    future.result()  # Wait for topic creation
                    logger.info(f"Created topic: {topic_name}")
                except Exception as e:
                    logger.error(f"Failed to create topic {topic_name}: {e}")
                    raise
            
            return topic_name
            
        except Exception as e:
            logger.error(f"Error creating topic {topic_name}: {e}")
            raise
    
    def register_schema(self, source: DataSource, schema: Dict[str, Any]) -> str:
        """
        Register Avro schema in Schema Registry.
        
        Args:
            source: DataSource the schema is for
            schema: Avro schema dictionary
            
        Returns:
            Schema ID
        """
        subject_name = f"{source.name.lower().replace(' ', '_')}-value"
        
        try:
            # Check if schema already exists
            try:
                existing = self.schema_registry.get_latest_version(subject_name)
                logger.info(f"Schema already exists for {subject_name}: {existing.schema_id}")
                return str(existing.schema_id)
            except Exception:
                # Schema doesn't exist, create it
                pass
            
            # Register new schema
            import json
            schema_str = json.dumps(schema)

            # NOTE: confluent_kafka Schema Registry client expects a Schema object,
            # not a raw string. If you pass a string, it will try to call .to_dict()
            # and fail with: "'str' object has no attribute 'to_dict'".
            avro_schema = Schema(schema_str, schema_type="AVRO")
            schema_id = self.schema_registry.register_schema(subject_name, avro_schema)
            
            logger.info(f"Registered schema {subject_name} with ID: {schema_id}")
            return str(schema_id)
            
        except Exception as e:
            logger.error(f"Error registering schema for {subject_name}: {e}")
            raise
    
    def create_ksql_stream(self, hypothesis, data_sources: list[DataSource]) -> Optional[str]:
        """
        Create ksqlDB stream for joining data sources.
        
        Args:
            hypothesis: Hypothesis to create stream for
            data_sources: Data sources to join
            
        Returns:
            Stream name if created
        """
        # For now, return None - ksqlDB integration would require ksqlDB client
        # This can be implemented later with ksql-python or REST API
        logger.info(f"KSQL stream creation not yet implemented for {hypothesis.cause.name} -> {hypothesis.effect.name}")
        return None
    
    def setup_connector(self, source: DataSource, sink: str) -> bool:
        """
        Setup Kafka Connect connector for a data source.
        
        Args:
            source: Data source to connect
            sink: Sink destination (e.g., "bigquery")
            
        Returns:
            True if successful
        """
        # Connector setup would require Confluent Cloud API or MCP
        logger.info(f"Connector setup not yet implemented for {source.name}")
        return False
    
    def get_topic_info(self, topic_name: str) -> Dict[str, Any]:
        """Get information about a topic."""
        try:
            metadata = self.admin_client.list_topics(timeout=10)
            if topic_name in metadata.topics:
                topic_metadata = metadata.topics[topic_name]
                return {
                    "name": topic_name,
                    "partitions": len(topic_metadata.partitions),
                    "exists": True,
                }
            return {"name": topic_name, "exists": False}
        except Exception as e:
            logger.error(f"Error getting topic info: {e}")
            return {"name": topic_name, "exists": False, "error": str(e)}


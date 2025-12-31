"""
Confluent integration client supporting both MCP servers and Admin API.

This module provides a unified interface for Confluent Cloud operations,
with MCP server as the primary method and Admin API as fallback.
"""

import json
from typing import Optional, Dict, Any, List

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from loguru import logger

from .models import DataSource, DataSourceType, Variable
from .domain import SchemaTemplate
from config.settings import Settings


class ConfluentClient:
    """
    Client for Confluent operations (MCP or Admin API).
    
    Supports:
    - Dynamic topic creation
    - Schema registration
    - Connector management (via MCP)
    - ksqlDB/Flink SQL operations (via MCP)
    
    Uses MCP server when available, falls back to Admin API otherwise.
    """
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.mcp_server_url = getattr(settings, 'mcp_server_url', None)
        self.use_mcp = self.mcp_server_url is not None
        self.mcp_client = None
        
        # Initialize MCP client if URL is provided
        if self.use_mcp:
            try:
                from .mcp_client import ConfluentMCPClient
                self.mcp_client = ConfluentMCPClient(
                    mcp_server_url=self.mcp_server_url,
                    api_key=settings.kafka_api_key,
                    api_secret=settings.kafka_api_secret,
                )
                # Verify MCP is healthy
                if not self.mcp_client.health_check():
                    logger.warning("MCP server not healthy, falling back to Admin API")
                    self.mcp_client = None
                    self.use_mcp = False
                else:
                    logger.info("MCP client initialized successfully")
            except Exception as e:
                logger.warning(f"Failed to initialize MCP client: {e}")
                self.mcp_client = None
                self.use_mcp = False
        
        # Initialize Admin Client for Kafka operations (fallback)
        try:
            self.admin_client = AdminClient(settings.get_kafka_producer_config())
        except Exception as e:
            logger.warning(f"Failed to initialize Admin client: {e}")
            self.admin_client = None
        
        # Initialize Schema Registry client
        try:
            self.schema_registry = SchemaRegistryClient(
                settings.get_schema_registry_config()
            )
        except Exception as e:
            logger.warning(f"Failed to initialize Schema Registry client: {e}")
            self.schema_registry = None
    
    # =========================================================================
    # Topic Operations
    # =========================================================================
    
    def create_topic_for_source(self, source: DataSource) -> str:
        """
        Create a Kafka topic for a data source.
        
        Args:
            source: DataSource to create topic for
            
        Returns:
            Topic name
        """
        topic_name = f"agent.{source.name.lower().replace(' ', '_')}.raw"
        
        if self.use_mcp and self.mcp_client:
            return self._create_topic_via_mcp(topic_name, source)
        else:
            return self._create_topic_via_admin(topic_name, source)
    
    def _create_topic_via_mcp(self, topic_name: str, source: DataSource) -> str:
        """Create topic using MCP server."""
        try:
            logger.info(f"Creating topic via MCP: {topic_name}")
            
            # Get topic configuration from source metadata
            partitions = source.metadata.get("partitions", 3)
            retention_ms = source.metadata.get("retention_ms", "604800000")  # 7 days
            
            config = {
                "retention.ms": retention_ms,
            }
            
            self.mcp_client.create_topic(
                topic_name=topic_name,
                partitions=partitions,
                replication_factor=3,  # Confluent Cloud requires RF=3
                config=config,
            )
            
            logger.info(f"Topic created via MCP: {topic_name}")
            return topic_name
            
        except Exception as e:
            logger.warning(f"MCP topic creation failed, falling back to Admin API: {e}")
            return self._create_topic_via_admin(topic_name, source)
    
    def _create_topic_via_admin(self, topic_name: str, source: DataSource) -> str:
        """Create topic using Admin API."""
        if not self.admin_client:
            raise RuntimeError("Admin client not available")
        
        try:
            # Check if topic already exists
            metadata = self.admin_client.list_topics(timeout=10)
            if topic_name in metadata.topics:
                logger.info(f"Topic {topic_name} already exists")
                return topic_name
            
            # Get configuration from source metadata
            partitions = source.metadata.get("partitions", 3)
            retention_ms = source.metadata.get("retention_ms", "604800000")
            
            # Create new topic
            topic = NewTopic(
                topic_name,
                num_partitions=partitions,
                replication_factor=3,  # Confluent Cloud policy requires RF=3
                config={
                    "retention.ms": retention_ms,
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
    
    def get_topic_info(self, topic_name: str) -> Dict[str, Any]:
        """Get information about a topic."""
        if self.use_mcp and self.mcp_client:
            try:
                return self.mcp_client.describe_topic(topic_name)
            except Exception as e:
                logger.warning(f"MCP describe_topic failed: {e}")
        
        # Fallback to Admin API
        if not self.admin_client:
            return {"name": topic_name, "exists": False, "error": "Admin client not available"}
        
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
    
    def list_topics(self) -> List[str]:
        """List all Kafka topics."""
        if self.use_mcp and self.mcp_client:
            try:
                return self.mcp_client.list_topics()
            except Exception as e:
                logger.warning(f"MCP list_topics failed: {e}")
        
        # Fallback to Admin API
        if not self.admin_client:
            return []
        
        try:
            metadata = self.admin_client.list_topics(timeout=10)
            return list(metadata.topics.keys())
        except Exception as e:
            logger.error(f"Error listing topics: {e}")
            return []
    
    # =========================================================================
    # Schema Operations
    # =========================================================================
    
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
        
        if self.use_mcp and self.mcp_client:
            try:
                result = self.mcp_client.register_schema(
                    subject=subject_name,
                    schema=schema,
                    schema_type="AVRO",
                )
                return str(result.get("schema_id", ""))
            except Exception as e:
                logger.warning(f"MCP schema registration failed: {e}")
        
        # Fallback to Schema Registry client
        return self._register_schema_via_client(subject_name, schema)
    
    def _register_schema_via_client(self, subject_name: str, schema: Dict[str, Any]) -> str:
        """Register schema using Schema Registry client."""
        if not self.schema_registry:
            raise RuntimeError("Schema Registry client not available")
        
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
            schema_str = json.dumps(schema)
            avro_schema = Schema(schema_str, schema_type="AVRO")
            schema_id = self.schema_registry.register_schema(subject_name, avro_schema)
            
            logger.info(f"Registered schema {subject_name} with ID: {schema_id}")
            return str(schema_id)
            
        except Exception as e:
            logger.error(f"Error registering schema for {subject_name}: {e}")
            raise
    
    def get_schema(self, subject: str, version: str = "latest") -> Optional[Dict[str, Any]]:
        """Get a schema from Schema Registry."""
        if self.use_mcp and self.mcp_client:
            try:
                return self.mcp_client.get_schema(subject, version)
            except Exception as e:
                logger.warning(f"MCP get_schema failed: {e}")
        
        # Fallback to Schema Registry client
        if not self.schema_registry:
            return None
        
        try:
            schema_version = self.schema_registry.get_latest_version(subject)
            return {
                "schema_id": schema_version.schema_id,
                "schema": schema_version.schema.schema_str,
                "version": schema_version.version,
            }
        except Exception as e:
            logger.error(f"Error getting schema: {e}")
            return None
    
    # =========================================================================
    # Connector Operations (MCP only)
    # =========================================================================
    
    def create_connector(self, source: DataSource) -> Optional[str]:
        """
        Create a Kafka Connect connector for a data source.
        
        Args:
            source: Data source to create connector for
            
        Returns:
            Connector name if created, None otherwise
        """
        if not self.use_mcp or not self.mcp_client:
            logger.info("Connector creation requires MCP server")
            return None
        
        if source.source_type != DataSourceType.API_REST:
            logger.info(f"Connector not needed for source type: {source.source_type}")
            return None
        
        try:
            from .mcp_client import connector_config_for_data_source
            
            config = connector_config_for_data_source(source)
            if not config:
                return None
            
            result = self.mcp_client.create_connector(config)
            connector_name = result.get("connector_name", config.name)
            
            logger.info(f"Created connector: {connector_name}")
            return connector_name
            
        except Exception as e:
            logger.error(f"Error creating connector for {source.name}: {e}")
            return None
    
    def create_connector_from_config(self, name: str, config: Dict[str, Any]) -> Optional[str]:
        """
        Create a connector from a raw configuration dictionary.
        
        This is useful when AI generates custom connector configurations.
        
        Args:
            name: Connector name
            config: Raw Kafka Connect configuration
            
        Returns:
            Connector name if created, None otherwise
        """
        if not self.use_mcp or not self.mcp_client:
            logger.info("Connector creation requires MCP server")
            return None
        
        try:
            result = self.mcp_client.create_connector_from_dict(name, config)
            connector_name = result.get("connector_name", name)
            
            logger.info(f"Created connector: {connector_name}")
            return connector_name
            
        except Exception as e:
            logger.error(f"Error creating connector {name}: {e}")
            return None
    
    def list_connectors(self) -> List[str]:
        """List all Kafka Connect connectors."""
        if not self.use_mcp or not self.mcp_client:
            return []
        
        try:
            return self.mcp_client.list_connectors()
        except Exception as e:
            logger.error(f"Error listing connectors: {e}")
            return []
    
    def get_connector_status(self, connector_name: str) -> Optional[Dict[str, Any]]:
        """Get the status of a connector."""
        if not self.use_mcp or not self.mcp_client:
            return None
        
        try:
            return self.mcp_client.get_connector_status(connector_name)
        except Exception as e:
            logger.error(f"Error getting connector status: {e}")
            return None
    
    def delete_connector(self, connector_name: str) -> bool:
        """Delete a connector."""
        if not self.use_mcp or not self.mcp_client:
            return False
        
        try:
            self.mcp_client.delete_connector(connector_name)
            logger.info(f"Deleted connector: {connector_name}")
            return True
        except Exception as e:
            logger.error(f"Error deleting connector: {e}")
            return False
    
    # =========================================================================
    # Flink SQL Operations (MCP only)
    # =========================================================================
    
    def execute_flink_sql(self, sql: str) -> Optional[Dict[str, Any]]:
        """
        Execute a Flink SQL statement.
        
        Args:
            sql: Flink SQL statement
            
        Returns:
            Execution result, or None if MCP not available
        """
        if not self.use_mcp or not self.mcp_client:
            logger.info("Flink SQL requires MCP server")
            return None
        
        try:
            return self.mcp_client.execute_flink_sql(sql)
        except Exception as e:
            logger.error(f"Error executing Flink SQL: {e}")
            return None
    
    def create_ksql_stream(self, hypothesis, data_sources: list) -> Optional[str]:
        """
        Create ksqlDB/Flink stream for joining data sources.
        
        Args:
            hypothesis: Hypothesis to create stream for
            data_sources: Data sources to join
            
        Returns:
            Stream name if created
        """
        if not self.use_mcp or not self.mcp_client:
            logger.info("KSQL/Flink stream creation requires MCP server")
            return None
        
        try:
            # Generate Flink SQL for joining data sources
            cause_topic = None
            effect_topic = None
            
            for source in data_sources:
                if hypothesis.cause in source.variables:
                    cause_topic = source.kafka_topic
                if hypothesis.effect in source.variables:
                    effect_topic = source.kafka_topic
            
            if not cause_topic or not effect_topic:
                logger.warning("Could not find topics for hypothesis variables")
                return None
            
            # Create joined stream
            stream_name = f"joined_{hypothesis.cause.name}_{hypothesis.effect.name}"
            
            sql = f"""
            CREATE TABLE {stream_name} AS
            SELECT
                c.timestamp AS timestamp,
                c.{hypothesis.cause.name} AS cause_value,
                e.{hypothesis.effect.name} AS effect_value
            FROM {cause_topic} c
            JOIN {effect_topic} e
                ON c.coin_id = e.coin_id
            """
            
            result = self.mcp_client.execute_flink_sql(sql)
            
            if result:
                logger.info(f"Created Flink stream: {stream_name}")
                return stream_name
            
            return None
            
        except Exception as e:
            logger.error(f"Error creating stream: {e}")
            return None
    
    # =========================================================================
    # Producer/Consumer Operations (MCP only)
    # =========================================================================
    
    def produce_message(
        self,
        topic: str,
        value: Dict[str, Any],
        key: Optional[str] = None,
    ) -> bool:
        """
        Produce a message to a Kafka topic via MCP.
        
        Args:
            topic: Topic name
            value: Message value
            key: Optional message key
            
        Returns:
            True if successful
        """
        if not self.use_mcp or not self.mcp_client:
            return False
        
        try:
            self.mcp_client.produce_message(topic, value, key)
            return True
        except Exception as e:
            logger.error(f"Error producing message: {e}")
            return False
    
    def consume_messages(
        self,
        topic: str,
        count: int = 10,
        from_beginning: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Consume messages from a Kafka topic via MCP.
        
        Args:
            topic: Topic name
            count: Number of messages to consume
            from_beginning: Whether to consume from the beginning
            
        Returns:
            List of consumed messages
        """
        if not self.use_mcp or not self.mcp_client:
            return []
        
        try:
            return self.mcp_client.consume_messages(topic, count, from_beginning)
        except Exception as e:
            logger.error(f"Error consuming messages: {e}")
            return []
    
    # =========================================================================
    # Utility Methods
    # =========================================================================
    
    def setup_connector(self, source: DataSource, sink: str) -> bool:
        """
        Setup Kafka Connect connector for a data source.
        
        Args:
            source: Data source to connect
            sink: Sink destination (e.g., "bigquery")
            
        Returns:
            True if successful
        """
        connector_name = self.create_connector(source)
        return connector_name is not None
    
    def is_mcp_available(self) -> bool:
        """Check if MCP is available and healthy."""
        if not self.mcp_client:
            return False
        return self.mcp_client.health_check()
    
    def get_capabilities(self) -> Dict[str, bool]:
        """Get available capabilities."""
        mcp_available = self.is_mcp_available()
        return {
            "topics": True,  # Always available via Admin API
            "schemas": self.schema_registry is not None,
            "connectors": mcp_available,
            "flink_sql": mcp_available,
            "produce_consume": mcp_available,
            "mcp": mcp_available,
        }

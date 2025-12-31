"""
Confluent MCP Client - Interface to Confluent MCP Server.

This module provides a client for interacting with the Confluent MCP Server,
enabling AI-driven management of Kafka topics, connectors, schemas, and Flink SQL.

The MCP (Model Context Protocol) server allows natural language and programmatic
control over Confluent Cloud resources.
"""

import json
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum

import requests
from loguru import logger

from .models import DataSource, DataSourceType, Variable


class ConnectorType(str, Enum):
    """Types of Kafka Connect connectors."""
    HTTP_SOURCE = "HTTP_SOURCE"
    JDBC_SOURCE = "JDBC_SOURCE"
    S3_SOURCE = "S3_SOURCE"
    GCS_SOURCE = "GCS_SOURCE"
    BIGQUERY_SINK = "BIGQUERY_SINK"
    S3_SINK = "S3_SINK"
    ELASTICSEARCH_SINK = "ELASTICSEARCH_SINK"
    CUSTOM = "CUSTOM"


class AuthType(str, Enum):
    """Authentication types for API connectors."""
    NONE = "NONE"
    API_KEY_HEADER = "API_KEY_HEADER"
    API_KEY_QUERY = "API_KEY_QUERY"
    BEARER_TOKEN = "BEARER_TOKEN"
    BASIC_AUTH = "BASIC_AUTH"
    OAUTH2 = "OAUTH2"


@dataclass
class ConnectorConfig:
    """Configuration for a Kafka Connect connector."""
    name: str
    connector_type: ConnectorType
    topic: str
    
    # Source-specific
    endpoint: Optional[str] = None
    auth_type: AuthType = AuthType.NONE
    auth_config: Dict[str, str] = field(default_factory=dict)
    
    # Polling/scheduling
    poll_interval_ms: int = 60000  # 1 minute default
    
    # Transformation
    value_converter: str = "io.confluent.connect.avro.AvroConverter"
    transforms: List[Dict[str, Any]] = field(default_factory=list)
    
    # Additional config
    extra_config: Dict[str, Any] = field(default_factory=dict)
    
    def to_connect_config(self) -> Dict[str, Any]:
        """Convert to Kafka Connect configuration format."""
        config = {
            "name": self.name,
            "connector.class": self._get_connector_class(),
            "topics": self.topic,
            "tasks.max": "1",
            "value.converter": self.value_converter,
            "value.converter.schema.registry.url": "${schema.registry.url}",
        }
        
        # Add source-specific config
        if self.endpoint:
            config["http.url"] = self.endpoint
            config["http.request.method"] = "GET"
            config["http.timer.interval.millis"] = str(self.poll_interval_ms)
        
        # Add authentication
        config.update(self._get_auth_config())
        
        # Add transforms
        if self.transforms:
            transform_names = []
            for i, transform in enumerate(self.transforms):
                name = transform.get("name", f"transform{i}")
                transform_names.append(name)
                for key, value in transform.items():
                    if key != "name":
                        config[f"transforms.{name}.{key}"] = value
            config["transforms"] = ",".join(transform_names)
        
        # Add extra config
        config.update(self.extra_config)
        
        return config
    
    def _get_connector_class(self) -> str:
        """Get the connector class name."""
        mapping = {
            ConnectorType.HTTP_SOURCE: "io.confluent.connect.http.HttpSourceConnector",
            ConnectorType.JDBC_SOURCE: "io.confluent.connect.jdbc.JdbcSourceConnector",
            ConnectorType.S3_SOURCE: "io.confluent.connect.s3.source.S3SourceConnector",
            ConnectorType.GCS_SOURCE: "io.confluent.connect.gcs.GcsSourceConnector",
            ConnectorType.BIGQUERY_SINK: "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector",
            ConnectorType.S3_SINK: "io.confluent.connect.s3.S3SinkConnector",
            ConnectorType.ELASTICSEARCH_SINK: "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
        }
        return mapping.get(self.connector_type, "io.confluent.connect.http.HttpSourceConnector")
    
    def _get_auth_config(self) -> Dict[str, str]:
        """Get authentication configuration."""
        if self.auth_type == AuthType.NONE:
            return {}
        
        if self.auth_type == AuthType.API_KEY_HEADER:
            header_name = self.auth_config.get("header_name", "X-API-Key")
            api_key = self.auth_config.get("api_key", "")
            return {
                "http.request.headers": f"{header_name}:{api_key}",
            }
        
        if self.auth_type == AuthType.BEARER_TOKEN:
            token = self.auth_config.get("token", "")
            return {
                "http.request.headers": f"Authorization:Bearer {token}",
            }
        
        if self.auth_type == AuthType.BASIC_AUTH:
            username = self.auth_config.get("username", "")
            password = self.auth_config.get("password", "")
            return {
                "http.auth.type": "BASIC",
                "http.auth.user": username,
                "http.auth.password": password,
            }
        
        if self.auth_type == AuthType.API_KEY_QUERY:
            param_name = self.auth_config.get("param_name", "api_key")
            api_key = self.auth_config.get("api_key", "")
            # Append to URL
            return {
                "http.url.suffix": f"?{param_name}={api_key}",
            }
        
        return {}


class ConfluentMCPClient:
    """
    Client for Confluent MCP Server operations.
    
    Provides methods for:
    - Topic management (create, delete, list, describe)
    - Connector management (create, delete, list, status)
    - Schema Registry operations (register, get, list)
    - Flink SQL execution
    
    Falls back to REST API if MCP is unavailable.
    """
    
    def __init__(self, mcp_server_url: str, api_key: Optional[str] = None, api_secret: Optional[str] = None):
        """
        Initialize MCP client.
        
        Args:
            mcp_server_url: URL of the Confluent MCP server
            api_key: Optional API key for authentication
            api_secret: Optional API secret for authentication
        """
        self.mcp_server_url = mcp_server_url.rstrip("/")
        self.api_key = api_key
        self.api_secret = api_secret
        self.session = requests.Session()
        
        # Set up authentication
        if api_key and api_secret:
            self.session.auth = (api_key, api_secret)
        
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
        })
    
    # =========================================================================
    # MCP Tool Calling
    # =========================================================================
    
    def _call_tool(self, tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """
        Call an MCP tool.
        
        Args:
            tool_name: Name of the MCP tool to call
            arguments: Arguments for the tool
            
        Returns:
            Tool response
        """
        try:
            response = self.session.post(
                f"{self.mcp_server_url}/tools/call",
                json={
                    "name": tool_name,
                    "arguments": arguments,
                },
                timeout=30,
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"MCP tool call failed: {tool_name} - {e}")
            raise MCPError(f"Tool call failed: {tool_name}") from e
    
    def _list_tools(self) -> List[Dict[str, Any]]:
        """List available MCP tools."""
        try:
            response = self.session.get(
                f"{self.mcp_server_url}/tools/list",
                timeout=10,
            )
            response.raise_for_status()
            return response.json().get("tools", [])
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to list MCP tools: {e}")
            return []
    
    # =========================================================================
    # Topic Management
    # =========================================================================
    
    def create_topic(
        self,
        topic_name: str,
        partitions: int = 3,
        replication_factor: int = 3,
        config: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Create a Kafka topic.
        
        Args:
            topic_name: Name of the topic to create
            partitions: Number of partitions
            replication_factor: Replication factor
            config: Additional topic configuration
            
        Returns:
            Topic creation result
        """
        logger.info(f"Creating topic via MCP: {topic_name}")
        
        arguments = {
            "topic_name": topic_name,
            "partitions": partitions,
            "replication_factor": replication_factor,
        }
        
        if config:
            arguments["config"] = config
        
        result = self._call_tool("create_topic", arguments)
        logger.info(f"Topic created: {topic_name}")
        return result
    
    def delete_topic(self, topic_name: str) -> Dict[str, Any]:
        """Delete a Kafka topic."""
        logger.info(f"Deleting topic via MCP: {topic_name}")
        return self._call_tool("delete_topic", {"topic_name": topic_name})
    
    def list_topics(self) -> List[str]:
        """List all Kafka topics."""
        result = self._call_tool("list_topics", {})
        return result.get("topics", [])
    
    def describe_topic(self, topic_name: str) -> Dict[str, Any]:
        """Get detailed information about a topic."""
        return self._call_tool("describe_topic", {"topic_name": topic_name})
    
    # =========================================================================
    # Connector Management
    # =========================================================================
    
    def create_connector(self, config: ConnectorConfig) -> Dict[str, Any]:
        """
        Create a Kafka Connect connector.
        
        Args:
            config: Connector configuration
            
        Returns:
            Connector creation result
        """
        logger.info(f"Creating connector via MCP: {config.name}")
        
        connect_config = config.to_connect_config()
        
        result = self._call_tool("create_connector", {
            "connector_name": config.name,
            "config": connect_config,
        })
        
        logger.info(f"Connector created: {config.name}")
        return result
    
    def create_connector_from_dict(self, name: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a connector from a raw configuration dictionary.
        
        Args:
            name: Connector name
            config: Raw Kafka Connect configuration
            
        Returns:
            Connector creation result
        """
        logger.info(f"Creating connector via MCP: {name}")
        
        result = self._call_tool("create_connector", {
            "connector_name": name,
            "config": config,
        })
        
        logger.info(f"Connector created: {name}")
        return result
    
    def delete_connector(self, connector_name: str) -> Dict[str, Any]:
        """Delete a Kafka Connect connector."""
        logger.info(f"Deleting connector via MCP: {connector_name}")
        return self._call_tool("delete_connector", {"connector_name": connector_name})
    
    def list_connectors(self) -> List[str]:
        """List all Kafka Connect connectors."""
        result = self._call_tool("list_connectors", {})
        return result.get("connectors", [])
    
    def get_connector_status(self, connector_name: str) -> Dict[str, Any]:
        """Get the status of a connector."""
        return self._call_tool("get_connector_status", {"connector_name": connector_name})
    
    def pause_connector(self, connector_name: str) -> Dict[str, Any]:
        """Pause a connector."""
        return self._call_tool("pause_connector", {"connector_name": connector_name})
    
    def resume_connector(self, connector_name: str) -> Dict[str, Any]:
        """Resume a paused connector."""
        return self._call_tool("resume_connector", {"connector_name": connector_name})
    
    # =========================================================================
    # Schema Registry
    # =========================================================================
    
    def register_schema(
        self,
        subject: str,
        schema: Dict[str, Any],
        schema_type: str = "AVRO",
    ) -> Dict[str, Any]:
        """
        Register a schema in Schema Registry.
        
        Args:
            subject: Schema subject name
            schema: Schema definition
            schema_type: Schema type (AVRO, JSON, PROTOBUF)
            
        Returns:
            Schema registration result with schema_id
        """
        logger.info(f"Registering schema via MCP: {subject}")
        
        result = self._call_tool("register_schema", {
            "subject": subject,
            "schema": json.dumps(schema) if isinstance(schema, dict) else schema,
            "schema_type": schema_type,
        })
        
        logger.info(f"Schema registered: {subject} (ID: {result.get('schema_id')})")
        return result
    
    def get_schema(self, subject: str, version: str = "latest") -> Dict[str, Any]:
        """Get a schema from Schema Registry."""
        return self._call_tool("get_schema", {
            "subject": subject,
            "version": version,
        })
    
    def list_subjects(self) -> List[str]:
        """List all schema subjects."""
        result = self._call_tool("list_subjects", {})
        return result.get("subjects", [])
    
    # =========================================================================
    # Flink SQL
    # =========================================================================
    
    def execute_flink_sql(self, sql: str) -> Dict[str, Any]:
        """
        Execute a Flink SQL statement.
        
        Args:
            sql: Flink SQL statement
            
        Returns:
            Execution result
        """
        logger.info(f"Executing Flink SQL via MCP")
        logger.debug(f"SQL: {sql}")
        
        result = self._call_tool("execute_flink_sql", {"sql": sql})
        return result
    
    def create_flink_statement(self, name: str, sql: str) -> Dict[str, Any]:
        """Create a named Flink SQL statement."""
        return self._call_tool("create_flink_statement", {
            "statement_name": name,
            "sql": sql,
        })
    
    def list_flink_statements(self) -> List[Dict[str, Any]]:
        """List all Flink SQL statements."""
        result = self._call_tool("list_flink_statements", {})
        return result.get("statements", [])
    
    # =========================================================================
    # Producer/Consumer Operations
    # =========================================================================
    
    def produce_message(
        self,
        topic: str,
        value: Dict[str, Any],
        key: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Produce a message to a Kafka topic.
        
        Args:
            topic: Topic name
            value: Message value
            key: Optional message key
            
        Returns:
            Production result
        """
        arguments = {
            "topic": topic,
            "value": json.dumps(value) if isinstance(value, dict) else value,
        }
        if key:
            arguments["key"] = key
        
        return self._call_tool("produce_message", arguments)
    
    def consume_messages(
        self,
        topic: str,
        count: int = 10,
        from_beginning: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Consume messages from a Kafka topic.
        
        Args:
            topic: Topic name
            count: Number of messages to consume
            from_beginning: Whether to consume from the beginning
            
        Returns:
            List of consumed messages
        """
        result = self._call_tool("consume_messages", {
            "topic": topic,
            "count": count,
            "from_beginning": from_beginning,
        })
        return result.get("messages", [])
    
    # =========================================================================
    # Health & Status
    # =========================================================================
    
    def health_check(self) -> bool:
        """Check if MCP server is healthy."""
        try:
            response = self.session.get(
                f"{self.mcp_server_url}/health",
                timeout=5,
            )
            return response.status_code == 200
        except requests.exceptions.RequestException:
            return False
    
    def get_cluster_info(self) -> Dict[str, Any]:
        """Get Confluent cluster information."""
        return self._call_tool("get_cluster_info", {})


class MCPError(Exception):
    """Exception raised for MCP-related errors."""
    pass


# =============================================================================
# Connector Configuration Helpers
# =============================================================================

def create_http_source_connector(
    name: str,
    endpoint: str,
    topic: str,
    auth_type: AuthType = AuthType.NONE,
    auth_config: Optional[Dict[str, str]] = None,
    poll_interval_ms: int = 60000,
    transforms: Optional[List[Dict[str, Any]]] = None,
) -> ConnectorConfig:
    """
    Create an HTTP Source Connector configuration.
    
    Args:
        name: Connector name
        endpoint: HTTP endpoint URL
        topic: Target Kafka topic
        auth_type: Authentication type
        auth_config: Authentication configuration
        poll_interval_ms: Polling interval in milliseconds
        transforms: Optional SMT transforms
        
    Returns:
        ConnectorConfig for HTTP source
    """
    return ConnectorConfig(
        name=name,
        connector_type=ConnectorType.HTTP_SOURCE,
        topic=topic,
        endpoint=endpoint,
        auth_type=auth_type,
        auth_config=auth_config or {},
        poll_interval_ms=poll_interval_ms,
        transforms=transforms or [],
    )


def create_bigquery_sink_connector(
    name: str,
    topics: List[str],
    project: str,
    dataset: str,
    keyfile_path: Optional[str] = None,
) -> ConnectorConfig:
    """
    Create a BigQuery Sink Connector configuration.
    
    Args:
        name: Connector name
        topics: Source Kafka topics
        project: GCP project ID
        dataset: BigQuery dataset name
        keyfile_path: Path to service account key file
        
    Returns:
        ConnectorConfig for BigQuery sink
    """
    extra_config = {
        "project": project,
        "datasets": dataset,
        "autoCreateTables": "true",
        "autoUpdateSchemas": "true",
        "bufferSize": "100000",
        "maxWriteSize": "10000",
    }
    
    if keyfile_path:
        extra_config["keyfile"] = keyfile_path
    
    return ConnectorConfig(
        name=name,
        connector_type=ConnectorType.BIGQUERY_SINK,
        topic=",".join(topics),
        extra_config=extra_config,
    )


def connector_config_for_data_source(source: DataSource) -> Optional[ConnectorConfig]:
    """
    Generate a connector configuration for a data source.
    
    Args:
        source: DataSource to create connector for
        
    Returns:
        ConnectorConfig if applicable, None otherwise
    """
    if source.source_type != DataSourceType.API_REST:
        return None
    
    # Determine auth type from metadata
    auth_type_str = source.metadata.get("auth_type", "NONE")
    try:
        auth_type = AuthType(auth_type_str)
    except ValueError:
        auth_type = AuthType.NONE
    
    # Build auth config
    auth_config = {}
    if auth_type == AuthType.API_KEY_HEADER:
        auth_config["header_name"] = source.metadata.get("auth_header", "X-API-Key")
        auth_config["api_key"] = source.api_key or ""
    elif auth_type == AuthType.BEARER_TOKEN:
        auth_config["token"] = source.api_key or ""
    elif auth_type == AuthType.API_KEY_QUERY:
        auth_config["param_name"] = source.metadata.get("auth_param", "api_key")
        auth_config["api_key"] = source.api_key or ""
    
    topic_name = source.kafka_topic or f"agent.{source.name.lower().replace(' ', '_')}.raw"
    
    return create_http_source_connector(
        name=f"connector-{source.name.lower().replace(' ', '_')}",
        endpoint=source.endpoint,
        topic=topic_name,
        auth_type=auth_type,
        auth_config=auth_config,
        poll_interval_ms=source.metadata.get("poll_interval_ms", 60000),
    )


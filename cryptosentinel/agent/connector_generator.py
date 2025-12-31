"""
AI-Powered Connector Configuration Generator.

Uses Gemini to analyze API endpoints and generate Kafka Connect configurations
for diverse external data sources.
"""

import json
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field

from loguru import logger

from .models import DataSource, DataSourceType, Variable
from .mcp_client import ConnectorConfig, ConnectorType, AuthType
from config.settings import Settings


# Prompt template for connector generation
CONNECTOR_GENERATION_PROMPT = """
You are a Kafka Connect expert. Generate a connector configuration for the following API.

API Details:
- Name: {api_name}
- Endpoint: {endpoint}
- Description: {description}
- Authentication: {auth_info}
- Variables to Extract: {variables}
- Poll Interval: {poll_interval_ms} ms

Available Connector Types:
- HTTP_SOURCE: For REST APIs that need polling
- JDBC_SOURCE: For databases with JDBC drivers
- S3_SOURCE: For AWS S3 data sources
- GCS_SOURCE: For Google Cloud Storage

Based on the API details, generate a Kafka Connect HTTP Source Connector configuration.

Respond with ONLY valid JSON (no markdown):
{{
  "connector_name": "connector-{api_name_slug}",
  "connector_class": "io.confluent.connect.http.HttpSourceConnector",
  "config": {{
    "http.url": "{endpoint}",
    "http.request.method": "GET",
    "http.timer.interval.millis": "{poll_interval_ms}",
    "http.request.headers": "...",
    "http.response.parser": "...",
    "tasks.max": "1",
    "topic": "agent.{api_name_slug}.raw"
  }},
  "transforms": [
    {{
      "name": "extractField",
      "type": "org.apache.kafka.connect.transforms.ExtractField$Value",
      "field": "data"
    }}
  ],
  "auth_type": "API_KEY_HEADER|BEARER_TOKEN|BASIC_AUTH|API_KEY_QUERY|NONE",
  "auth_config": {{
    "header_name": "...",
    "param_name": "..."
  }},
  "notes": "Any important notes about this connector configuration"
}}

Rules:
1. Use appropriate authentication based on the API
2. Include transforms if the API response needs parsing
3. Set reasonable polling intervals (don't overwhelm the API)
4. Include all necessary headers for the API
5. If the API uses pagination, note it but use simple polling
"""

# Prompt for analyzing API documentation
API_ANALYSIS_PROMPT = """
Analyze this API documentation and extract key information for creating a Kafka Connect connector.

API Documentation:
{api_docs}

Extract and return ONLY valid JSON:
{{
  "base_url": "The base URL of the API",
  "endpoints": [
    {{
      "path": "/path/to/endpoint",
      "method": "GET|POST",
      "description": "What this endpoint does",
      "auth_required": true|false,
      "auth_type": "API_KEY_HEADER|BEARER_TOKEN|BASIC_AUTH|API_KEY_QUERY|NONE",
      "auth_header": "Header name if applicable",
      "rate_limit": "Rate limit info if available",
      "response_format": "Description of response structure",
      "variables_provided": ["list", "of", "variables"]
    }}
  ],
  "auth_info": {{
    "type": "API_KEY_HEADER|BEARER_TOKEN|BASIC_AUTH|API_KEY_QUERY|NONE",
    "header_name": "Header name if applicable",
    "query_param": "Query param name if applicable"
  }},
  "notes": "Any important notes about using this API"
}}
"""


@dataclass
class GeneratedConnectorConfig:
    """Result of AI-generated connector configuration."""
    connector_name: str
    config: Dict[str, Any]
    auth_type: AuthType
    auth_config: Dict[str, str]
    transforms: List[Dict[str, Any]]
    notes: str = ""
    success: bool = True
    error: Optional[str] = None


class ConnectorGenerator:
    """
    AI-powered connector configuration generator.
    
    Uses Gemini to analyze APIs and generate appropriate Kafka Connect configurations.
    """
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.client = None
        
        # Initialize Gemini client
        try:
            import google.genai as genai
            self.client = genai.Client(api_key=settings.gemini_api_key)
            logger.info("ConnectorGenerator initialized with Gemini")
        except Exception as e:
            logger.warning(f"Failed to initialize Gemini client: {e}")
    
    def generate_connector_config(
        self,
        source: DataSource,
        api_docs: Optional[str] = None,
    ) -> GeneratedConnectorConfig:
        """
        Generate a connector configuration for a data source.
        
        Args:
            source: DataSource to generate connector for
            api_docs: Optional API documentation to analyze
            
        Returns:
            GeneratedConnectorConfig with the generated configuration
        """
        if not self.client:
            return GeneratedConnectorConfig(
                connector_name=f"connector-{source.name}",
                config={},
                auth_type=AuthType.NONE,
                auth_config={},
                transforms=[],
                success=False,
                error="Gemini client not available",
            )
        
        try:
            # Build context from source
            api_name = source.name
            api_name_slug = source.name.lower().replace(" ", "_").replace("-", "_")
            endpoint = source.endpoint or ""
            description = source.metadata.get("description", "External API data source")
            
            # Build auth info
            auth_type_str = source.metadata.get("auth_type", "NONE")
            auth_header = source.metadata.get("auth_header", "")
            auth_info = f"Type: {auth_type_str}"
            if auth_header:
                auth_info += f", Header: {auth_header}"
            if source.api_key:
                auth_info += " (API key provided)"
            
            # Build variables list
            variables = [v.name for v in source.variables] if source.variables else ["data"]
            
            # Poll interval
            poll_interval_ms = source.metadata.get("poll_interval_ms", 60000)
            
            # Build prompt
            prompt = CONNECTOR_GENERATION_PROMPT.format(
                api_name=api_name,
                api_name_slug=api_name_slug,
                endpoint=endpoint,
                description=description,
                auth_info=auth_info,
                variables=", ".join(variables),
                poll_interval_ms=poll_interval_ms,
            )
            
            # Add API docs if provided
            if api_docs:
                prompt += f"\n\nAdditional API Documentation:\n{api_docs}"
            
            # Generate with Gemini
            response = self.client.models.generate_content(
                model="gemini-2.5-flash",
                contents=prompt,
            )
            
            text = response.text.strip()
            
            # Clean JSON
            if text.startswith("```"):
                lines = text.split("\n")
                text = "\n".join(lines[1:-1]) if lines[-1] == "```" else "\n".join(lines[1:])
                if text.startswith("json"):
                    text = text[4:].strip()
            
            data = json.loads(text)
            
            # Parse auth type
            auth_type_str = data.get("auth_type", "NONE")
            try:
                auth_type = AuthType(auth_type_str)
            except ValueError:
                auth_type = AuthType.NONE
            
            return GeneratedConnectorConfig(
                connector_name=data.get("connector_name", f"connector-{api_name_slug}"),
                config=data.get("config", {}),
                auth_type=auth_type,
                auth_config=data.get("auth_config", {}),
                transforms=data.get("transforms", []),
                notes=data.get("notes", ""),
                success=True,
            )
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse Gemini response: {e}")
            return GeneratedConnectorConfig(
                connector_name=f"connector-{source.name}",
                config={},
                auth_type=AuthType.NONE,
                auth_config={},
                transforms=[],
                success=False,
                error=f"JSON parse error: {e}",
            )
        except Exception as e:
            logger.error(f"Error generating connector config: {e}")
            return GeneratedConnectorConfig(
                connector_name=f"connector-{source.name}",
                config={},
                auth_type=AuthType.NONE,
                auth_config={},
                transforms=[],
                success=False,
                error=str(e),
            )
    
    def analyze_api_documentation(self, api_docs: str) -> Dict[str, Any]:
        """
        Analyze API documentation to extract connector-relevant information.
        
        Args:
            api_docs: API documentation text or URL content
            
        Returns:
            Dictionary with extracted API information
        """
        if not self.client:
            return {"error": "Gemini client not available"}
        
        try:
            prompt = API_ANALYSIS_PROMPT.format(api_docs=api_docs)
            
            response = self.client.models.generate_content(
                model="gemini-2.5-flash",
                contents=prompt,
            )
            
            text = response.text.strip()
            
            # Clean JSON
            if text.startswith("```"):
                lines = text.split("\n")
                text = "\n".join(lines[1:-1]) if lines[-1] == "```" else "\n".join(lines[1:])
                if text.startswith("json"):
                    text = text[4:].strip()
            
            return json.loads(text)
            
        except Exception as e:
            logger.error(f"Error analyzing API docs: {e}")
            return {"error": str(e)}
    
    def generate_connector_for_api_type(
        self,
        api_type: str,
        endpoint: str,
        topic: str,
        auth_type: AuthType = AuthType.NONE,
        auth_config: Optional[Dict[str, str]] = None,
        **kwargs,
    ) -> ConnectorConfig:
        """
        Generate a connector configuration for a known API type.
        
        Args:
            api_type: Type of API (e.g., "coingecko", "newsdata", "reddit")
            endpoint: API endpoint URL
            topic: Target Kafka topic
            auth_type: Authentication type
            auth_config: Authentication configuration
            **kwargs: Additional configuration options
            
        Returns:
            ConnectorConfig for the API
        """
        auth_config = auth_config or {}
        
        # Pre-defined configurations for known APIs
        api_configs = {
            "coingecko": {
                "connector_type": ConnectorType.HTTP_SOURCE,
                "poll_interval_ms": 60000,  # 1 minute
                "transforms": [
                    {
                        "name": "extractData",
                        "type": "org.apache.kafka.connect.transforms.ExtractField$Value",
                        "field": "data",
                    }
                ],
            },
            "newsdata": {
                "connector_type": ConnectorType.HTTP_SOURCE,
                "poll_interval_ms": 300000,  # 5 minutes (rate limited)
                "transforms": [
                    {
                        "name": "extractResults",
                        "type": "org.apache.kafka.connect.transforms.ExtractField$Value",
                        "field": "results",
                    }
                ],
            },
            "fear_greed": {
                "connector_type": ConnectorType.HTTP_SOURCE,
                "poll_interval_ms": 3600000,  # 1 hour (updates daily)
                "transforms": [
                    {
                        "name": "extractData",
                        "type": "org.apache.kafka.connect.transforms.ExtractField$Value",
                        "field": "data",
                    }
                ],
            },
            "reddit": {
                "connector_type": ConnectorType.HTTP_SOURCE,
                "poll_interval_ms": 120000,  # 2 minutes
                "transforms": [
                    {
                        "name": "extractChildren",
                        "type": "org.apache.kafka.connect.transforms.ExtractField$Value",
                        "field": "data.children",
                    }
                ],
            },
        }
        
        # Get config for API type, or use defaults
        api_config = api_configs.get(api_type.lower(), {
            "connector_type": ConnectorType.HTTP_SOURCE,
            "poll_interval_ms": 60000,
            "transforms": [],
        })
        
        # Override with kwargs
        api_config.update(kwargs)
        
        return ConnectorConfig(
            name=f"connector-{api_type.lower()}-{topic.replace('.', '-')}",
            connector_type=api_config["connector_type"],
            topic=topic,
            endpoint=endpoint,
            auth_type=auth_type,
            auth_config=auth_config,
            poll_interval_ms=api_config["poll_interval_ms"],
            transforms=api_config.get("transforms", []),
        )


# =============================================================================
# Pre-built Connector Templates for Common APIs
# =============================================================================

def create_coingecko_connector(
    topic: str = "agent.coingecko.raw",
    coins: List[str] = None,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create a CoinGecko API connector configuration.
    
    Args:
        topic: Target Kafka topic
        coins: List of coin IDs to track
        api_key: Optional API key for pro tier
        
    Returns:
        Kafka Connect configuration dictionary
    """
    coins = coins or ["bitcoin", "ethereum"]
    coin_ids = ",".join(coins)
    
    endpoint = f"https://api.coingecko.com/api/v3/simple/price?ids={coin_ids}&vs_currencies=usd&include_24hr_vol=true&include_24hr_change=true&include_market_cap=true"
    
    config = {
        "name": "connector-coingecko",
        "connector.class": "io.confluent.connect.http.HttpSourceConnector",
        "http.url": endpoint,
        "http.request.method": "GET",
        "http.timer.interval.millis": "60000",
        "http.response.parser": "io.confluent.connect.http.response.JsonPathResponseParser",
        "tasks.max": "1",
        "topic": topic,
        "value.converter": "io.confluent.connect.avro.AvroConverter",
    }
    
    if api_key:
        config["http.request.headers"] = f"x-cg-demo-api-key:{api_key}"
    
    return config


def create_newsdata_connector(
    topic: str = "agent.newsdata.raw",
    query: str = "cryptocurrency",
    api_key: str = "",
) -> Dict[str, Any]:
    """
    Create a NewsData.io API connector configuration.
    
    Args:
        topic: Target Kafka topic
        query: Search query
        api_key: NewsData.io API key
        
    Returns:
        Kafka Connect configuration dictionary
    """
    endpoint = f"https://newsdata.io/api/1/news?apikey={api_key}&q={query}&language=en"
    
    return {
        "name": "connector-newsdata",
        "connector.class": "io.confluent.connect.http.HttpSourceConnector",
        "http.url": endpoint,
        "http.request.method": "GET",
        "http.timer.interval.millis": "300000",  # 5 minutes
        "http.response.parser": "io.confluent.connect.http.response.JsonPathResponseParser",
        "http.response.parser.json.path": "$.results[*]",
        "tasks.max": "1",
        "topic": topic,
        "value.converter": "io.confluent.connect.avro.AvroConverter",
    }


def create_fear_greed_connector(
    topic: str = "agent.fear_greed.raw",
) -> Dict[str, Any]:
    """
    Create a Fear & Greed Index connector configuration.
    
    Args:
        topic: Target Kafka topic
        
    Returns:
        Kafka Connect configuration dictionary
    """
    return {
        "name": "connector-fear-greed",
        "connector.class": "io.confluent.connect.http.HttpSourceConnector",
        "http.url": "https://api.alternative.me/fng/?limit=1",
        "http.request.method": "GET",
        "http.timer.interval.millis": "3600000",  # 1 hour
        "http.response.parser": "io.confluent.connect.http.response.JsonPathResponseParser",
        "http.response.parser.json.path": "$.data[0]",
        "tasks.max": "1",
        "topic": topic,
        "value.converter": "io.confluent.connect.avro.AvroConverter",
    }


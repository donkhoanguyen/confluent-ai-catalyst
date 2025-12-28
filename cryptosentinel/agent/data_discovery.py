"""
Data source discovery - now grounded to existing assets in the registry.

Instead of inventing new APIs, this module maps variables to existing
Kafka topics and data sources that are already available.
"""

import json
from typing import List, Optional
import google.genai as genai
from loguru import logger

from .models import Variable, DataSource, DataSourceType
from .asset_registry import get_asset_registry
from config.settings import Settings


DATA_SOURCE_DISCOVERY_PROMPT = """
You are a data engineer helping to find data sources for variables.

IMPORTANT: You MUST only use data sources that are available in the system.

Variable: {variable_name}
Description: {variable_description}
Type: {variable_type}
Domain: {domain}

AVAILABLE DATA SOURCES:
{available_sources}

Your task:
1. Check if the variable "{variable_name}" is provided by any of the available sources above
2. If yes, select the appropriate source(s) from the list
3. If no exact match, suggest the closest available source that might contain related data
4. Do NOT invent new APIs or endpoints - only use sources from the list above

Respond with ONLY valid JSON (no markdown):
{{
  "sources": [
    {{
      "name": "source_name_from_available_list",
      "source_type": "KAFKA_TOPIC|DATABASE",
      "endpoint": "kafka topic name or database endpoint",
      "description": "How this source provides the variable",
      "variables_provided": ["variable_name"],
      "match_quality": "EXACT|CLOSE|PARTIAL"
    }}
  ],
  "unavailable": "If variable is not available, explain what would be needed"
}}

Rules:
- ONLY use source names from the available sources list
- Set match_quality: EXACT if the source directly provides this variable, CLOSE if related, PARTIAL if partially available
- If the variable is not available, set "unavailable" field
"""


class DataSourceDiscovery:
    """
    Discovers data sources for variables, grounded to existing assets.
    
    First checks the registry for exact matches, then uses Gemini to
    suggest the best available source if needed.
    """
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.client = genai.Client(api_key=settings.gemini_api_key)
        self.registry = get_asset_registry()
    
    def discover_sources(self, variable: Variable, domain: str) -> List[DataSource]:
        """
        Discover data sources for a variable from the registry.
        
        Args:
            variable: Variable to find sources for
            domain: Domain context
            
        Returns:
            List of DataSource objects (only from registry)
        """
        # First, check registry for exact matches
        registry_sources = self.registry.get_data_sources_for_variable(variable.name, domain)
        
        if registry_sources:
            logger.info(
                f"Found {len(registry_sources)} registry sources for {variable.name}: "
                f"{[s.name for s in registry_sources]}"
            )
            return registry_sources
        
        # If not found, try to find by similar name or use Gemini to suggest closest match
        logger.debug(f"Variable '{variable.name}' not found in registry, using Gemini to find closest match")
        
        # Get all available sources for context
        available_sources = self.registry.list_available_data_sources()
        source_list = []
        for source in available_sources:
            var_names = [v.name for v in source.variables]
            source_list.append(
                f"- {source.name} ({source.source_type.value}): {source.metadata.get('description', 'N/A')}"
                + f" [Provides: {', '.join(var_names)}]"
            )
        sources_text = "\n".join(source_list) if source_list else "No data sources available."
        
        prompt = DATA_SOURCE_DISCOVERY_PROMPT.format(
            variable_name=variable.name,
            variable_description=variable.description,
            variable_type=variable.variable_type.value,
            domain=domain,
            available_sources=sources_text,
        )
        
        try:
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
            
            # Check if variable is unavailable
            if data.get("unavailable"):
                logger.warning(f"Variable '{variable.name}' is not available: {data.get('unavailable')}")
                return []
            
            sources = []
            for source_data in data.get("sources", []):
                source_name = source_data["name"]
                match_quality = source_data.get("match_quality", "PARTIAL")
                
                # Get source from registry
                registry_source = self.registry.get_data_source(source_name)
                if registry_source:
                    # Create a copy with this variable added
                    source = DataSource(
                        name=registry_source.name,
                        source_type=registry_source.source_type,
                        endpoint=registry_source.endpoint,
                        kafka_topic=registry_source.kafka_topic,
                        variables=[variable] + registry_source.variables,
                        metadata={
                            **registry_source.metadata,
                            "match_quality": match_quality,
                            "discovered_for": variable.name,
                        }
                    )
                    sources.append(source)
                else:
                    logger.warning(f"Source '{source_name}' suggested by Gemini but not in registry")
            
            logger.info(f"Discovered {len(sources)} sources for {variable.name} via Gemini")
            return sources
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse Gemini response: {e}")
            return []
        except Exception as e:
            logger.error(f"Error discovering sources: {e}")
            return []
    
    def _parse_rate_limit(self, rate_limit_str: str) -> Optional[int]:
        """Parse rate limit string to integer."""
        if not rate_limit_str:
            return None
        
        try:
            # Try to extract number from strings like "60 requests/minute"
            import re
            numbers = re.findall(r'\d+', rate_limit_str)
            if numbers:
                return int(numbers[0])
        except Exception:
            pass
        
        return None


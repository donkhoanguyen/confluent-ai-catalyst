"""
Data source discovery using Gemini to find APIs and data sources.
"""

import json
from typing import List, Optional
import google.genai as genai
from loguru import logger

from .models import Variable, DataSource, DataSourceType
from config.settings import Settings


DATA_SOURCE_DISCOVERY_PROMPT = """
You are a data engineer helping to find data sources for variables.

Variable: {variable_name}
Description: {variable_description}
Type: {variable_type}
Domain: {domain}

Find public APIs, databases, or data sources that provide this variable.
Suggest specific APIs with:
- API name and endpoint
- Authentication requirements
- Rate limits
- Data format
- How to access the variable

Respond with ONLY valid JSON (no markdown):
{{
  "sources": [
    {{
      "name": "API or source name",
      "source_type": "API_REST|API_WEBSOCKET|KAFKA_TOPIC|DATABASE|FILE",
      "endpoint": "API endpoint URL",
      "api_key_required": true/false,
      "rate_limit": "requests per minute",
      "description": "How to access this variable",
      "variables_provided": ["variable_name"],
      "metadata": {{
        "documentation_url": "URL if available",
        "example_response": "Example API response structure"
      }}
    }}
  ]
}}

Rules:
- Suggest 2-3 concrete data sources
- Prefer free/public APIs when possible
- Include specific endpoints and parameters
- Note authentication requirements
"""


class DataSourceDiscovery:
    """Discovers data sources for variables using Gemini."""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.client = genai.Client(api_key=settings.gemini_api_key)
    
    def discover_sources(self, variable: Variable, domain: str) -> List[DataSource]:
        """
        Discover data sources for a variable.
        
        Args:
            variable: Variable to find sources for
            domain: Domain context
            
        Returns:
            List of DataSource objects
        """
        prompt = DATA_SOURCE_DISCOVERY_PROMPT.format(
            variable_name=variable.name,
            variable_description=variable.description,
            variable_type=variable.variable_type.value,
            domain=domain,
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
            sources = []
            
            for source_data in data.get("sources", []):
                try:
                    source = DataSource(
                        name=source_data["name"],
                        source_type=DataSourceType(source_data["source_type"]),
                        endpoint=source_data.get("endpoint"),
                        api_key=source_data.get("api_key") if source_data.get("api_key_required") else None,
                        rate_limit=self._parse_rate_limit(source_data.get("rate_limit", "")),
                        variables=[variable],
                        metadata=source_data.get("metadata", {}),
                    )
                    sources.append(source)
                except Exception as e:
                    logger.warning(f"Error parsing source: {e}")
                    continue
            
            logger.info(f"Discovered {len(sources)} sources for {variable.name}")
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


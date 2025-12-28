"""
Confounding factor discovery using Gemini.
"""

import json
from typing import List
import google.genai as genai
from loguru import logger

from .models import Hypothesis, Variable, VariableType
from config.settings import Settings


CONFOUNDER_DISCOVERY_PROMPT = """
You are a causal inference expert identifying confounding factors.

Hypothesis: {cause_name} causes {effect_name}
Mechanism: {mechanism}
Domain: {domain}

A confounder is a variable that:
1. Is associated with the cause
2. Is associated with the effect
3. Is not on the causal pathway (not a mediator)

Identify potential confounding factors for this hypothesis.
For each confounder, explain why it might confound the relationship.

Respond with ONLY valid JSON (no markdown):
{{
  "confounders": [
    {{
      "name": "confounder_name",
      "description": "Description of the confounder",
      "variable_type": "CONTINUOUS|DISCRETE|BINARY|CATEGORICAL|TIME_SERIES",
      "unit": "unit if applicable",
      "domain": "{domain}",
      "reasoning": "Why this might confound the relationship",
      "data_sources": [
        {{
          "name": "source_name",
          "endpoint": "API endpoint or data location",
          "source_type": "API_REST|API_WEBSOCKET|KAFKA_TOPIC|DATABASE|FILE"
        }}
      ]
    }}
  ]
}}

Rules:
- Identify 2-4 realistic confounders
- Explain the confounding mechanism
- Suggest data sources for each confounder
- Be specific about variable names and types
"""


class ConfounderDiscovery:
    """Discovers confounding factors using Gemini."""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.client = genai.Client(api_key=settings.gemini_api_key)
    
    def discover(self, hypothesis: Hypothesis, domain: str) -> List[Variable]:
        """
        Discover confounding factors for a hypothesis.
        
        Args:
            hypothesis: Hypothesis to find confounders for
            domain: Domain context
            
        Returns:
            List of Variable objects representing confounders
        """
        prompt = CONFOUNDER_DISCOVERY_PROMPT.format(
            cause_name=hypothesis.cause.name,
            effect_name=hypothesis.effect.name,
            mechanism=hypothesis.mechanism,
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
            confounders = []
            
            for conf_data in data.get("confounders", []):
                try:
                    confounder = Variable(
                        name=conf_data["name"],
                        description=conf_data["description"],
                        variable_type=VariableType(conf_data["variable_type"]),
                        unit=conf_data.get("unit"),
                        domain=conf_data.get("domain", domain),
                        metadata={
                            "reasoning": conf_data.get("reasoning", ""),
                            "data_sources": conf_data.get("data_sources", []),
                        },
                    )
                    confounders.append(confounder)
                except Exception as e:
                    logger.warning(f"Error parsing confounder: {e}")
                    continue
            
            logger.info(f"Discovered {len(confounders)} confounders for {hypothesis.cause.name} -> {hypothesis.effect.name}")
            return confounders
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse Gemini response: {e}")
            return []
        except Exception as e:
            logger.error(f"Error discovering confounders: {e}")
            return []


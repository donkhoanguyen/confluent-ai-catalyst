"""
Gemini-powered hypothesis generation for causal discovery.
"""

import json
from typing import List, Optional
from google import genai
from loguru import logger

from .models import Hypothesis, Variable, VariableType, CausalMethod, CausalResult
from config.settings import Settings


HYPOTHESIS_GENERATION_PROMPT = """
You are an expert causal inference researcher. Given a domain and a research question, 
generate causal hypotheses about potential cause-effect relationships.

Domain: {domain}
Research Question: {query}

For each hypothesis, provide:
1. The cause variable (what might cause the effect)
2. The effect variable (what is being affected)
3. A brief mechanism explaining how the cause might affect the effect
4. An initial confidence score (0.0 to 1.0) based on domain knowledge
5. Suggested causal inference methods (from: GRANGER, TRANSFER_ENTROPY, PROPENSITY_SCORE_MATCHING, INSTRUMENTAL_VARIABLES, PC_ALGORITHM)
6. Potential confounding factors that might affect the relationship
7. Required data sources to test the hypothesis

Respond with ONLY valid JSON (no markdown, no code blocks):
{{
  "hypotheses": [
    {{
      "cause": {{
        "name": "variable_name",
        "description": "Description of the cause variable",
        "variable_type": "CONTINUOUS|DISCRETE|BINARY|CATEGORICAL|TIME_SERIES",
        "unit": "unit if applicable",
        "domain": "{domain}"
      }},
      "effect": {{
        "name": "variable_name",
        "description": "Description of the effect variable",
        "variable_type": "CONTINUOUS|DISCRETE|BINARY|CATEGORICAL|TIME_SERIES",
        "unit": "unit if applicable",
        "domain": "{domain}"
      }},
      "mechanism": "Brief explanation of how cause affects effect",
      "confidence": 0.75,
      "suggested_methods": ["GRANGER", "TRANSFER_ENTROPY"],
      "potential_confounders": [
        {{
          "name": "confounder_name",
          "description": "Description",
          "variable_type": "CONTINUOUS",
          "domain": "{domain}"
        }}
      ],
      "required_data_sources": [
        {{
          "name": "source_name",
          "description": "What data source provides this variable",
          "source_type": "API_REST|API_WEBSOCKET|KAFKA_TOPIC|DATABASE|FILE"
        }}
      ]
    }}
  ]
}}

Rules:
- Generate 2-5 hypotheses
- Be specific about variable names and types
- Suggest appropriate causal methods based on data characteristics
- Identify realistic confounding factors
- Suggest concrete data sources (APIs, databases, etc.)
"""

REFINEMENT_PROMPT = """
You are analyzing causal inference results and need to refine hypotheses.

Domain: {domain}
Previous Hypotheses: {hypotheses_json}
Test Results: {results_json}

Based on the results:
1. Which hypotheses were supported? (is_significant = true)
2. Which hypotheses were rejected? (is_significant = false)
3. What confounding factors were significant?
4. What new hypotheses should be explored?
5. How should existing hypotheses be modified?

Respond with ONLY valid JSON (no markdown):
{{
  "refined_hypotheses": [
    // Same structure as hypothesis generation
  ],
  "insights": "Brief summary of what was learned"
}}
"""


class HypothesisGenerator:
    """Generates causal hypotheses using Gemini."""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.client = genai.Client(api_key=settings.gemini_api_key)
        # Use "gemini-2.5-flash" as it's more widely supported across API versions
        # The gemini-2.5-flash model may not be available in all API versions
        self.model_name = "gemini-2.5-flash"
    
    def generate(self, domain: str, query: str) -> List[Hypothesis]:
        """
        Generate causal hypotheses for a domain and query.
        
        Args:
            domain: Domain name (e.g., "cryptocurrency")
            query: Research question
            
        Returns:
            List of Hypothesis objects
        """
        prompt = HYPOTHESIS_GENERATION_PROMPT.format(
            domain=domain,
            query=query,
        )
        
        try:
            response = self.client.models.generate_content(
                model=self.model_name,
                contents=prompt
            )
            text = response.text.strip()
            
            # Clean JSON response
            if text.startswith("```"):
                lines = text.split("\n")
                text = "\n".join(lines[1:-1]) if lines[-1] == "```" else "\n".join(lines[1:])
                if text.startswith("json"):
                    text = text[4:].strip()
            
            data = json.loads(text)
            hypotheses = []
            
            for i, hyp_data in enumerate(data.get("hypotheses", [])):
                try:
                    cause_data = hyp_data["cause"]
                    effect_data = hyp_data["effect"]
                    
                    cause = Variable(
                        name=cause_data["name"],
                        description=cause_data["description"],
                        variable_type=VariableType(cause_data["variable_type"]),
                        unit=cause_data.get("unit"),
                        domain=cause_data.get("domain", domain),
                    )
                    
                    effect = Variable(
                        name=effect_data["name"],
                        description=effect_data["description"],
                        variable_type=VariableType(effect_data["variable_type"]),
                        unit=effect_data.get("unit"),
                        domain=effect_data.get("domain", domain),
                    )
                    
                    methods = [CausalMethod(m) for m in hyp_data.get("suggested_methods", [])]
                    
                    confounders = []
                    for conf_data in hyp_data.get("potential_confounders", []):
                        confounders.append(Variable(
                            name=conf_data["name"],
                            description=conf_data["description"],
                            variable_type=VariableType(conf_data["variable_type"]),
                            domain=conf_data.get("domain", domain),
                        ))
                    
                    hypothesis = Hypothesis(
                        cause=cause,
                        effect=effect,
                        mechanism=hyp_data["mechanism"],
                        confidence=hyp_data.get("confidence", 0.5),
                        suggested_methods=methods,
                        potential_confounders=confounders,
                        hypothesis_id=f"{domain}_{i}_{cause.name}_{effect.name}",
                    )
                    
                    hypotheses.append(hypothesis)
                    
                except Exception as e:
                    logger.warning(f"Error parsing hypothesis {i}: {e}")
                    continue
            
            logger.info(f"Generated {len(hypotheses)} hypotheses")
            return hypotheses
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse Gemini response: {e}")
            logger.debug(f"Response text: {text[:500]}")
            return []
        except Exception as e:
            logger.error(f"Error generating hypotheses: {e}")
            return []
    
    def refine(self, hypotheses: List[Hypothesis], results: List[CausalResult], 
               domain: str) -> List[Hypothesis]:
        """
        Refine hypotheses based on test results.
        
        Args:
            hypotheses: Previous hypotheses
            results: Causal test results
            domain: Domain name
            
        Returns:
            Refined list of hypotheses
        """
        # Convert to JSON for prompt
        hypotheses_json = json.dumps([
            {
                "cause": h.cause.name,
                "effect": h.effect.name,
                "mechanism": h.mechanism,
            }
            for h in hypotheses
        ], indent=2)
        
        results_json = json.dumps([
            {
                "hypothesis": f"{r.hypothesis.cause.name} -> {r.hypothesis.effect.name}",
                "method": r.method.value,
                "is_significant": r.is_significant,
                "confidence": r.confidence,
                "p_value": r.p_value,
                "confounders_significant": [v.name for v in r.confounders_significant],
            }
            for r in results
        ], indent=2)
        
        prompt = REFINEMENT_PROMPT.format(
            domain=domain,
            hypotheses_json=hypotheses_json,
            results_json=results_json,
        )
        
        try:
            response = self.client.models.generate_content(
                model=self.model_name,
                contents=prompt
            )
            text = response.text.strip()
            
            # Clean JSON
            if text.startswith("```"):
                lines = text.split("\n")
                text = "\n".join(lines[1:-1]) if lines[-1] == "```" else "\n".join(lines[1:])
                if text.startswith("json"):
                    text = text[4:].strip()
            
            data = json.loads(text)
            refined = []
            
            # Parse refined hypotheses (similar to generate method)
            for hyp_data in data.get("refined_hypotheses", []):
                try:
                    cause = Variable(
                        name=hyp_data["cause"]["name"],
                        description=hyp_data["cause"]["description"],
                        variable_type=VariableType(hyp_data["cause"]["variable_type"]),
                        domain=domain,
                    )
                    
                    effect = Variable(
                        name=hyp_data["effect"]["name"],
                        description=hyp_data["effect"]["description"],
                        variable_type=VariableType(hyp_data["effect"]["variable_type"]),
                        domain=domain,
                    )
                    
                    methods = [CausalMethod(m) for m in hyp_data.get("suggested_methods", [])]
                    
                    hypothesis = Hypothesis(
                        cause=cause,
                        effect=effect,
                        mechanism=hyp_data["mechanism"],
                        confidence=hyp_data.get("confidence", 0.5),
                        suggested_methods=methods,
                        hypothesis_id=f"{domain}_refined_{cause.name}_{effect.name}",
                    )
                    
                    refined.append(hypothesis)
                except Exception as e:
                    logger.warning(f"Error parsing refined hypothesis: {e}")
                    continue
            
            logger.info(f"Refined to {len(refined)} hypotheses")
            return refined if refined else hypotheses  # Fallback to original
            
        except Exception as e:
            logger.error(f"Error refining hypotheses: {e}")
            return hypotheses  # Fallback to original


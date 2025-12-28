"""
Gemini-powered hypothesis generation for causal discovery.

Now grounded to only use variables and data sources that exist in the system.
"""

import json
from typing import List, Optional, Tuple
from google import genai
from loguru import logger

from .models import Hypothesis, Variable, VariableType, CausalMethod, CausalResult
from .asset_registry import get_asset_registry
from config.settings import Settings


HYPOTHESIS_GENERATION_PROMPT = """
You are an expert causal inference researcher. Given a domain and a research question, 
generate causal hypotheses about potential cause-effect relationships.

IMPORTANT: You MUST only use variables that are available in the system. Do not invent new variables.

Domain: {domain}
Research Question: {query}

AVAILABLE VARIABLES (you can ONLY use these):
{available_variables}

AVAILABLE DATA SOURCES:
{available_sources}

For each hypothesis, provide:
1. The cause variable (MUST be from the available variables list above)
2. The effect variable (MUST be from the available variables list above)
3. A brief mechanism explaining how the cause might affect the effect
4. An initial confidence score (0.0 to 1.0) based on domain knowledge
5. Suggested causal inference methods (from: GRANGER, TRANSFER_ENTROPY, PROPENSITY_SCORE_MATCHING, INSTRUMENTAL_VARIABLES, PC_ALGORITHM)
6. Potential confounding factors (MUST be from available variables if they exist, or mark as "NOT_AVAILABLE")
7. Required data sources (MUST be from the available sources list above)

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
          "domain": "{domain}",
          "available": true|false
        }}
      ],
      "required_data_sources": [
        {{
          "name": "source_name",
          "description": "What data source provides this variable",
          "source_type": "KAFKA_TOPIC|DATABASE"
        }}
      ]
    }}
  ],
  "unavailable_variables": [
    "List any variables you wanted to use but are not available"
  ]
}}

Rules:
- Generate 2-5 hypotheses
- ONLY use variable names that exactly match the available variables list
- If you need a variable that's not available, note it in "unavailable_variables"
- Suggest appropriate causal methods based on data characteristics
- For confounders, set "available": true if the variable exists, false otherwise
- Use data sources from the available sources list
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
    """Generates causal hypotheses using Gemini, grounded to existing assets."""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.client = genai.Client(api_key=settings.gemini_api_key)
        # Use "gemini-2.5-flash" as it's more widely supported across API versions
        # The gemini-2.5-flash model may not be available in all API versions
        self.model_name = "gemini-2.5-flash"
        self.registry = get_asset_registry()
    
    def generate(self, domain: str, query: str, include_wish_list: bool = True) -> Tuple[List[Hypothesis], List[Hypothesis]]:
        """
        Generate causal hypotheses for a domain and query.
        Only uses variables and data sources that exist in the registry.
        
        Args:
            domain: Domain name (e.g., "cryptocurrency")
            query: Research question
            include_wish_list: If True, also return wish list hypotheses (not currently measurable)
            
        Returns:
            Tuple of (testable_hypotheses, wish_list_hypotheses)
            - testable_hypotheses: Hypotheses with all variables available
            - wish_list_hypotheses: Hypotheses with unavailable variables (marked as wish_list=True)
        """
        # Get available variables and sources for this domain
        available_vars = self.registry.get_variables_by_domain(domain)
        available_sources = self.registry.list_available_data_sources()
        
        # Format variables for prompt
        var_list = []
        for var in available_vars:
            var_list.append(
                f"- {var.name} ({var.variable_type.value}): {var.description}"
                + (f" [{var.unit}]" if var.unit else "")
            )
        variables_text = "\n".join(var_list) if var_list else "No variables available for this domain."
        
        # Format sources for prompt
        source_list = []
        for source in available_sources:
            var_names = [v.name for v in source.variables]
            source_list.append(
                f"- {source.name} ({source.source_type.value}): {source.metadata.get('description', 'N/A')}"
                + f" [Variables: {', '.join(var_names)}]"
            )
        sources_text = "\n".join(source_list) if source_list else "No data sources available."
        
        prompt = HYPOTHESIS_GENERATION_PROMPT.format(
            domain=domain,
            query=query,
            available_variables=variables_text,
            available_sources=sources_text,
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
            wish_list_hypotheses = []  # Hypotheses with unavailable variables
            unavailable_vars = data.get("unavailable_variables", [])
            
            if unavailable_vars:
                logger.info(f"LLM suggested unavailable variables: {unavailable_vars}")
            
            for i, hyp_data in enumerate(data.get("hypotheses", [])):
                try:
                    # Normalize hypothesis entry
                    if isinstance(hyp_data, str):
                        try:
                            hyp_data = json.loads(hyp_data)
                        except json.JSONDecodeError:
                            logger.warning(f"Hypothesis {i} is a string and not valid JSON: {hyp_data!r}")
                            continue
                    if not isinstance(hyp_data, dict):
                        logger.warning(f"Hypothesis {i} has unexpected type: {type(hyp_data)}")
                        continue

                    cause_data = hyp_data.get("cause")
                    effect_data = hyp_data.get("effect")
                    if not isinstance(cause_data, dict) or not isinstance(effect_data, dict):
                        logger.warning(f"Hypothesis {i} missing cause/effect details: {hyp_data}")
                        continue
                    
                    # Validate and get cause variable from registry
                    cause_registered = self.registry.get_variable(cause_data["name"], domain)
                    if not cause_registered:
                        # Create wish list hypothesis (not currently measurable)
                        cause_wish = Variable(
                            name=cause_data["name"],
                            description=cause_data.get("description", ""),
                            variable_type=VariableType(cause_data.get("variable_type", "CONTINUOUS")),
                            unit=cause_data.get("unit"),
                            domain=domain,
                            metadata={"available": False, "wish_list": True}
                        )
                        logger.info(
                            f"Hypothesis {i}: Cause variable '{cause_data['name']}' not available - "
                            f"creating wish list entry"
                        )
                        # Check if effect is available
                        effect_registered = self.registry.get_variable(effect_data["name"], domain)
                        if not effect_registered:
                            effect_wish = Variable(
                                name=effect_data["name"],
                                description=effect_data.get("description", ""),
                                variable_type=VariableType(effect_data.get("variable_type", "CONTINUOUS")),
                                unit=effect_data.get("unit"),
                                domain=domain,
                                metadata={"available": False, "wish_list": True}
                            )
                        else:
                            effect_wish = effect_registered
                        
                        # Create wish list hypothesis
                        wish_hyp = Hypothesis(
                            cause=cause_wish,
                            effect=effect_wish,
                            mechanism=hyp_data.get("mechanism", ""),
                            confidence=hyp_data.get("confidence", 0.5),
                            suggested_methods=[CausalMethod(m) for m in hyp_data.get("suggested_methods", [])],
                            hypothesis_id=f"{domain}_wish_{i}_{cause_data['name']}_{effect_data['name']}",
                            metadata={"wish_list": True, "unavailable_variables": [cause_data["name"]]}
                        )
                        wish_list_hypotheses.append(wish_hyp)
                        continue
                    cause = cause_registered
                    
                    # Validate and get effect variable from registry
                    effect_registered = self.registry.get_variable(effect_data["name"], domain)
                    if not effect_registered:
                        # Effect not available, but cause is - create wish list
                        effect_wish = Variable(
                            name=effect_data["name"],
                            description=effect_data.get("description", ""),
                            variable_type=VariableType(effect_data.get("variable_type", "CONTINUOUS")),
                            unit=effect_data.get("unit"),
                            domain=domain,
                            metadata={"available": False, "wish_list": True}
                        )
                        wish_hyp = Hypothesis(
                            cause=cause,
                            effect=effect_wish,
                            mechanism=hyp_data.get("mechanism", ""),
                            confidence=hyp_data.get("confidence", 0.5),
                            suggested_methods=[CausalMethod(m) for m in hyp_data.get("suggested_methods", [])],
                            hypothesis_id=f"{domain}_wish_{i}_{cause.name}_{effect_data['name']}",
                            metadata={"wish_list": True, "unavailable_variables": [effect_data["name"]]}
                        )
                        wish_list_hypotheses.append(wish_hyp)
                        continue
                    effect = effect_registered
                    
                    methods = [CausalMethod(m) for m in hyp_data.get("suggested_methods", [])]
                    
                    # Validate confounders against registry
                    confounders = []
                    for conf_data in hyp_data.get("potential_confounders", []):
                        if not isinstance(conf_data, dict):
                            logger.debug(f"Confounder entry has unexpected type for hypothesis {i}: {conf_data!r}")
                            continue
                        is_available = conf_data.get("available", True)
                        conf_name = conf_data["name"]
                        
                        if is_available:
                            conf_registered = self.registry.get_variable(conf_name, domain)
                            if conf_registered:
                                confounders.append(conf_registered)
                            else:
                                logger.debug(f"Confounder '{conf_name}' marked available but not in registry")
                        else:
                            logger.debug(f"Confounder '{conf_name}' not available in system")
                    
                    # Map to actual data sources from registry
                    required_sources = []
                    for source_entry in hyp_data.get("required_data_sources", []):
                        if isinstance(source_entry, str):
                            source_name = source_entry
                        elif isinstance(source_entry, dict):
                            source_name = source_entry.get("name")
                        else:
                            logger.debug(f"Source entry has unexpected type for hypothesis {i}: {source_entry!r}")
                            continue
                        if not source_name:
                            continue
                        source = self.registry.get_data_source(source_name)
                        if source:
                            required_sources.append(source)
                        else:
                            # Try to find source that provides the variables
                            cause_sources = self.registry.get_data_sources_for_variable(cause.name, domain)
                            effect_sources = self.registry.get_data_sources_for_variable(effect.name, domain)
                            required_sources.extend(cause_sources + effect_sources)
                    
                    # Remove duplicates
                    required_sources = list(dict.fromkeys(required_sources))
                    
                    hypothesis = Hypothesis(
                        cause=cause,
                        effect=effect,
                        mechanism=hyp_data["mechanism"],
                        confidence=hyp_data.get("confidence", 0.5),
                        suggested_methods=methods,
                        potential_confounders=confounders,
                        required_data_sources=required_sources,
                        hypothesis_id=f"{domain}_{i}_{cause.name}_{effect.name}",
                    )
                    
                    hypotheses.append(hypothesis)
                    
                except Exception as e:
                    logger.warning(f"Error parsing hypothesis {i}: {e}")
                    continue
            
            logger.info(
                f"Generated {len(hypotheses)} valid hypotheses (grounded to registry), "
                f"{len(wish_list_hypotheses)} wish list hypotheses (not currently measurable)"
            )
            
            # Store wish list in metadata for future reference
            if wish_list_hypotheses:
                logger.info(
                    f"Wish list hypotheses (require new data sources): "
                    f"{[f'{h.cause.name} -> {h.effect.name}' for h in wish_list_hypotheses]}"
                )
            
            if include_wish_list:
                return hypotheses, wish_list_hypotheses
            return hypotheses, []
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse Gemini response: {e}")
            logger.debug(f"Response text: {text[:500]}")
            return [], []
        except Exception as e:
            logger.error(f"Error generating hypotheses: {e}")
            return [], []
    
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


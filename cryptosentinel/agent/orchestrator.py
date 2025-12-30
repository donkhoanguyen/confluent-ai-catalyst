"""
LangGraph-based orchestrator for autonomous causal discovery agent.
"""

from typing import Annotated, Literal, TypedDict, Dict, Optional, List
from datetime import datetime, timezone
from operator import add
import json
import os

from loguru import logger
from dotenv import load_dotenv

# Load .env file to ensure LangSmith environment variables are available
# This is needed because LangChain/LangGraph reads from os.environ directly
load_dotenv(override=True)

# Ensure LANGCHAIN_API_KEY is set if LANGSMITH_API_KEY exists but LANGCHAIN_API_KEY doesn't
# LangChain uses LANGCHAIN_API_KEY to authenticate with LangSmith
if os.getenv("LANGSMITH_API_KEY") and not os.getenv("LANGCHAIN_API_KEY"):
    os.environ["LANGCHAIN_API_KEY"] = os.getenv("LANGSMITH_API_KEY")
    logger.debug("Set LANGCHAIN_API_KEY from LANGSMITH_API_KEY for LangSmith tracing")

try:
    from langgraph.graph import StateGraph, END
    from langgraph.graph.message import add_messages
    LANGGRAPH_AVAILABLE = True
except ImportError:
    LANGGRAPH_AVAILABLE = False
    logger.warning("langgraph not available - using simplified orchestrator")

from .models import (
    DiscoveryState,
    Hypothesis,
    CausalResult,
    Variable,
    DataSource,
    CausalMethod,
)
from .asset_registry import get_asset_registry
from .domain import DomainRegistry, VariableRegistry
from .dataframe_builder import CanonicalDataFrameBuilder
try:
    from .hypothesis_generator import HypothesisGenerator
    from .confounder_discovery import ConfounderDiscovery
    AGENT_MODULES_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Agent modules not fully available: {e}")
    AGENT_MODULES_AVAILABLE = False
from config.settings import Settings, get_settings


def merge_dicts(a: dict, b: dict) -> dict:
    """Merge two dictionaries, with b taking precedence."""
    return {**a, **b}


class AgentState(TypedDict):
    """State for the LangGraph agent."""
    domain: str
    query: str
    hypotheses: Annotated[list[Hypothesis], add]
    results: Annotated[list[CausalResult], add]
    active_data_sources: Annotated[list[DataSource], add]
    discovered_variables: Annotated[list[Variable], add]
    iteration: int
    status: str
    error: Optional[str]
    metadata: Annotated[dict, merge_dicts]
    messages: Annotated[list, add_messages]


class CausalDiscoveryAgent:
    """
    Autonomous causal discovery agent using LangGraph.
    
    The agent operates in a discovery loop (triggered after DataCurationAgent completes):
    1. Generate hypotheses using Gemini
    2. Run causal inference tests
    3. Discover and test confounders
    4. Refine hypotheses based on results
    5. Evaluate results and decide if more iterations needed
    
    Note: Data discovery, pipeline integration, and data collection are handled
    by the DataCurationAgent before this agent is triggered.
    """
    
    def __init__(self, settings: Settings = None):
        self.settings = settings or get_settings()
        self.offline_mode = getattr(self.settings, "offline_mode", False)
        
        # Only initialize LLM-dependent modules if not in offline mode
        # and if the modules are available
        # Note: Data discovery is handled by DataCurationAgent, not here
        if AGENT_MODULES_AVAILABLE and not self.offline_mode:
            try:
                self.hypothesis_generator = HypothesisGenerator(self.settings)
                self.confounder_discovery = ConfounderDiscovery(self.settings)
            except ValueError as e:
                # GCP_PROJECT_ID not set - fall back to None
                logger.warning(f"LLM modules unavailable: {e}")
                self.hypothesis_generator = None
                self.confounder_discovery = None
        else:
            self.hypothesis_generator = None
            self.confounder_discovery = None
            if self.offline_mode:
                logger.info("Offline mode: skipping LLM module initialization")
        self.variable_registry = VariableRegistry()
        self.domain_registry: Dict[str, DomainRegistry] = {}
        # Initialize DataFrame builder with data directory from settings if available
        data_dir = getattr(self.settings, 'canonical_data_dir', None) if self.settings else None
        self.dataframe_builder = CanonicalDataFrameBuilder(data_dir=data_dir)
        self.offline_df = self._load_offline_sample_df() if self.offline_mode else None
        
        # Build LangGraph
        self.graph = self._build_graph()
        if self.graph and LANGGRAPH_AVAILABLE:
            # LangGraph API handles persistence automatically, no need for custom checkpointer
            self.app = self.graph.compile()
        else:
            self.app = None
    
    def _load_offline_sample_df(self, domain: str = None):
        """Load a bundled offline demo DataFrame if available.
        
        This method is domain-agnostic. It attempts to load CSV files
        based on the domain name or common patterns.
        
        Args:
            domain: Optional domain name to use for CSV file lookup
        """
        try:
            # Try loading by domain name if provided
            if domain:
                df = self.dataframe_builder.load_from_csv(domain)
                if df is not None and len(df) > 0:
                    logger.info(f"Loaded {len(df)} rows from CSV for domain '{domain}' in offline mode")
                    return df
            
            # Try common unified data file names (domain-agnostic)
            common_names = ["unified_market_data", "unified_data", "sample_data"]
            for name in common_names:
                df = self.dataframe_builder.load_from_csv(name)
                if df is not None and len(df) > 0:
                    logger.info(f"Loaded {len(df)} rows from {name}.csv for offline mode")
                    return df
            
            # If no CSV found, that's okay - offline mode can work without it
            logger.debug("No offline demo CSV found; offline mode will run without pre-loaded data.")
            return None
        except Exception as e:
            logger.warning(f"Failed to load offline demo CSV: {e}")
            return None

    def _offline_hypotheses(self, domain: str, state: dict = None) -> tuple[list[Hypothesis], list[Hypothesis]]:
        """Provide hypotheses for offline/demo mode based on discovered variables.
        
        This is domain-agnostic - it uses whatever variables are available in state
        or the registry for the given domain.
        
        Args:
            domain: The domain name
            state: Optional agent state containing discovered_variables
        """
        registry = get_asset_registry()
        
        # Get variables from state (discovered by DataCurationAgent) or registry
        if state and state.get("discovered_variables"):
            available_vars = state["discovered_variables"]
            logger.info(f"Using {len(available_vars)} discovered variables for offline hypotheses")
        else:
            available_vars = registry.get_variables_by_domain(domain)
            logger.info(f"Using {len(available_vars)} registry variables for offline hypotheses")
        
        if not available_vars:
            logger.warning(f"No variables available for domain '{domain}' in offline mode")
            return [], []
        
        hypotheses: list[Hypothesis] = []
        
        # Create hypotheses by pairing available variables
        # Look for time-series or continuous variables that could have causal relationships
        time_series_vars = [v for v in available_vars if v.variable_type.value in ("TIME_SERIES", "CONTINUOUS")]
        
        if len(time_series_vars) >= 2:
            # Create hypothesis pairs from available variables
            for i, cause in enumerate(time_series_vars[:3]):  # Limit to first 3 as causes
                for effect in time_series_vars[i+1:i+3]:  # Pair with next 2 variables
                    if cause.name != effect.name:
                        hypotheses.append(
                            Hypothesis(
                                cause=cause,
                                effect=effect,
                                mechanism=f"Changes in {cause.name} may causally affect {effect.name}.",
                                confidence=0.5,
                                suggested_methods=[CausalMethod.GRANGER, CausalMethod.TRANSFER_ENTROPY],
                                hypothesis_id=f"{domain}_offline_{cause.name}_{effect.name}",
                                metadata={"offline_demo": True, "domain": domain},
                            )
                        )
                        if len(hypotheses) >= 3:  # Limit to 3 hypotheses in offline mode
                            break
                if len(hypotheses) >= 3:
                    break
        
        logger.info(f"Generated {len(hypotheses)} offline hypotheses for domain '{domain}'")
        return hypotheses, []
    
    def _build_graph(self):
        """Build the LangGraph state graph for causal discovery.
        
        The graph flow is:
        generate_hypotheses -> run_causal_tests -> discover_confounders -> 
        test_confounders -> refine_hypotheses -> evaluate_results -> (refine loop or END)
        
        Note: Data discovery, pipeline integration, and data collection are
        handled by the DataCurationAgent before this agent is triggered.
        """
        if not LANGGRAPH_AVAILABLE:
            # Fallback to simple sequential execution
            return None
        
        workflow = StateGraph(AgentState)
        
        # Add nodes - causal analysis only (no data discovery/collection)
        workflow.add_node("generate_hypotheses", self._generate_hypotheses_node)
        workflow.add_node("run_causal_tests", self._run_causal_tests_node)
        workflow.add_node("discover_confounders", self._discover_confounders_node)
        workflow.add_node("test_confounders", self._test_confounders_node)
        workflow.add_node("refine_hypotheses", self._refine_hypotheses_node)
        workflow.add_node("evaluate_results", self._evaluate_results_node)
        
        # Define edges - linear flow with refinement loop
        workflow.set_entry_point("generate_hypotheses")
        
        # Main analysis pipeline
        workflow.add_edge("generate_hypotheses", "run_causal_tests")
        workflow.add_edge("run_causal_tests", "discover_confounders")
        workflow.add_edge("discover_confounders", "test_confounders")
        workflow.add_edge("test_confounders", "refine_hypotheses")
        
        # Conditional edge: continue to evaluate or end early
        workflow.add_conditional_edges(
            "refine_hypotheses",
            self._should_continue,
            {
                "continue": "evaluate_results",
                "end": END,
            }
        )
        
        # Conditional edge: refine (loop back) or end
        workflow.add_conditional_edges(
            "evaluate_results",
            self._should_refine,
            {
                "refine": "generate_hypotheses",
                "end": END,
            }
        )
        
        return workflow
    
    def _generate_hypotheses_node(self, state: AgentState) -> dict:
        """Generate causal hypotheses using Gemini.
        
        Uses discovered variables and data sources from DataCurationAgent if available.
        This ensures hypotheses are generated based on what data is actually available.
        """
        logger.info(f"Generating hypotheses for domain: {state['domain']}")
        
        try:
            if self.offline_mode:
                hypotheses, wish_list = self._offline_hypotheses(state["domain"], state)
            else:
                if not self.hypothesis_generator:
                    raise RuntimeError("Hypothesis generator not available")
                
                # Use discovered variables and data sources from state if available
                # These come from DataCurationAgent's data discovery phase
                discovered_vars = state.get("discovered_variables", [])
                active_sources = state.get("active_data_sources", [])
                
                if discovered_vars:
                    logger.info(f"Using {len(discovered_vars)} discovered variables for hypothesis generation")
                if active_sources:
                    logger.info(f"Using {len(active_sources)} discovered data sources for hypothesis generation")
                
                result = self.hypothesis_generator.generate(
                    domain=state["domain"],
                    query=state["query"],
                    include_wish_list=True,
                    discovered_variables=discovered_vars if discovered_vars else None,
                    active_data_sources=active_sources if active_sources else None,
                )
                if isinstance(result, tuple) and len(result) == 2:
                    hypotheses, wish_list = result
                else:
                    hypotheses, wish_list = [], []
            
            # Register variables from hypotheses
            new_variables = []
            for hyp in hypotheses:
                self.variable_registry.register(hyp.cause)
                self.variable_registry.register(hyp.effect)
                new_variables.extend([hyp.cause, hyp.effect])
            
            logger.info(f"Generated {len(hypotheses)} testable hypotheses, {len(wish_list)} wish list entries")
            
            return {
                "hypotheses": hypotheses,
                "discovered_variables": new_variables,
                "status": "HYPOTHESIS_GENERATION",
                "metadata": {
                    "hypothesis_count": len(hypotheses),
                    "wish_list_count": len(wish_list),
                    "wish_list": [{"cause": h.cause.name, "effect": h.effect.name, "unavailable": h.metadata.get("unavailable_variables", [])} for h in wish_list],
                },
            }
            
        except Exception as e:
            logger.error(f"Error generating hypotheses: {e}")
            return {
                "error": str(e),
                "status": "ERROR",
            }
    
    def _run_causal_tests_node(self, state: AgentState) -> dict:
        """Run causal inference tests on hypotheses using canonical DataFrame.
        
        This method is domain-agnostic. It loads data based on the domain
        and uses discovered data sources when available.
        """
        logger.info(f"Running causal inference tests for domain: {state['domain']}")
        
        try:
            try:
                from causal.registry import CausalEngineRegistry
            except ImportError:
                from .causal.registry import CausalEngineRegistry
            
            registry = CausalEngineRegistry()
            results = []
            domain = state["domain"]
            
            # Use entity_id from metadata if available, otherwise use domain name
            entity_id = state.get("metadata", {}).get("entity_id", domain)
            
            # Try to load offline data for this domain
            df_from_csv = None
            if self.offline_mode:
                df_from_csv = self._load_offline_sample_df(domain)
            
            # Try to get data from in-memory DataStore if available
            # This is a fallback - in production, data would come from Kafka/discovered sources
            price_history = None
            sentiment_history = None
            
            try:
                from api.main import data_store
                if hasattr(data_store, 'price_history') and entity_id in data_store.price_history:
                    price_history = list(data_store.price_history[entity_id])
                if hasattr(data_store, 'sentiment_history') and entity_id in data_store.sentiment_history:
                    sentiment_history = list(data_store.sentiment_history[entity_id])
            except (ImportError, AttributeError):
                logger.debug("DataStore not available, will use CSV or placeholder data")
            
            for hypothesis in state["hypotheses"]:
                # Build canonical DataFrame for this hypothesis
                df = None
                
                # Try to use CSV data first (if we loaded it)
                if df_from_csv is not None:
                    # Use the full DataFrame and let build_for_hypothesis extract relevant columns
                    df = self.dataframe_builder.build_for_hypothesis(
                        hypothesis=hypothesis,
                        coin_id=entity_id,  # Generic entity identifier
                        price_history=None,  # Not needed if using CSV
                        sentiment_history=None,
                    )
                    # If that didn't work, build from the CSV DataFrame
                    if df is None:
                        df = df_from_csv.copy()
                
                # Fallback to building from DataStore
                if df is None and price_history and sentiment_history:
                    df = self.dataframe_builder.build_for_hypothesis(
                        hypothesis=hypothesis,
                        coin_id=entity_id,
                        price_history=price_history,
                        sentiment_history=sentiment_history,
                    )
                
                # Try each suggested method
                for method in hypothesis.suggested_methods:
                    try:
                        engine = registry.get_engine(method)
                        if not engine:
                            logger.warning(f"Engine not available for method: {method.value}")
                            continue
                        
                        # Pass pre-built DataFrame if available
                        result = engine.test(
                            cause=hypothesis.cause,
                            effect=hypothesis.effect,
                            data_sources=hypothesis.required_data_sources,
                            data=df,  # Use canonical DataFrame
                            confounders=hypothesis.potential_confounders,
                        )
                        results.append(result)
                        logger.info(
                            f"Tested {hypothesis.cause.name} -> {hypothesis.effect.name} "
                            f"using {method.value} (data: {'canonical' if df is not None else 'fetched'})"
                        )
                    except Exception as e:
                        logger.warning(f"Method {method.value} failed: {e}")
                        continue
            
            return {
                "results": results,
                "status": "CAUSAL_TESTING",
                "metadata": {"tests_run": len(results)},
            }
            
        except Exception as e:
            logger.error(f"Error running causal tests: {e}")
            return {
                "error": str(e),
                "status": "ERROR",
            }
    
    def _discover_confounders_node(self, state: AgentState) -> dict:
        """Discover potential confounding factors."""
        logger.info("Discovering confounding factors")
        
        try:
            if not self.confounder_discovery:
                raise RuntimeError("Confounder discovery not available")
            
            new_variables = []
            total_confounders = 0
            
            for hypothesis in state["hypotheses"]:
                confounders = self.confounder_discovery.discover(
                    hypothesis=hypothesis,
                    domain=state["domain"],
                )
                hypothesis.potential_confounders = confounders
                total_confounders += len(confounders)
                
                # Register confounder variables
                for conf in confounders:
                    self.variable_registry.register(conf)
                    new_variables.append(conf)
            
            return {
                "discovered_variables": new_variables,
                "status": "CONFOUNDER_DISCOVERY",
                "metadata": {"confounders_discovered": total_confounders},
            }
            
        except Exception as e:
            logger.error(f"Error discovering confounders: {e}")
            return {
                "error": str(e),
                "status": "ERROR",
            }
    
    def _test_confounders_node(self, state: AgentState) -> dict:
        """Test for confounding effects."""
        logger.info("Testing confounding factors")
        
        try:
            try:
                from causal.confounder_test import ConfounderTester
            except ImportError:
                from .causal.confounder_test import ConfounderTester
            
            tester = ConfounderTester()
            
            for result in state["results"]:
                hypothesis = result.hypothesis
                if hypothesis.potential_confounders:
                    confounders_tested = tester.test_confounders(
                        hypothesis=hypothesis,
                        confounders=hypothesis.potential_confounders,
                    )
                    result.confounders_tested = confounders_tested
                    result.confounders_significant = [
                        c for c in confounders_tested if tester.is_confounding(c)
                    ]
            
            return {"status": "CONFOUNDER_TESTING"}
            
        except Exception as e:
            logger.error(f"Error testing confounders: {e}")
            return {
                "error": str(e),
                "status": "ERROR",
            }
    
    def _refine_hypotheses_node(self, state: AgentState) -> dict:
        """Refine hypotheses based on results."""
        logger.info("Refining hypotheses")
        
        # In offline mode or if generator unavailable, skip refinement but increment iteration
        if self.offline_mode or not self.hypothesis_generator:
            logger.info("Skipping hypothesis refinement (offline mode or generator unavailable)")
            return {
                "hypotheses": state["hypotheses"],  # Keep existing hypotheses
                "iteration": state["iteration"] + 1,  # CRITICAL: Always increment iteration
                "status": "REFINING",
            }
        
        try:
            # Use Gemini to analyze results and suggest refinements
            refined = self.hypothesis_generator.refine(
                hypotheses=state["hypotheses"],
                results=state["results"],
                domain=state["domain"],
            )
            
            return {
                "hypotheses": refined,
                "iteration": state["iteration"] + 1,
                "status": "REFINING",
            }
            
        except Exception as e:
            logger.error(f"Error refining hypotheses: {e}")
            # CRITICAL: Even on error, increment iteration to prevent infinite loop
            return {
                "hypotheses": state["hypotheses"],  # Keep existing hypotheses
                "iteration": state["iteration"] + 1,  # Increment to prevent infinite loop
                "status": "REFINING",
                "metadata": {"refinement_error": str(e)},
            }
    
    def _evaluate_results_node(self, state: AgentState) -> dict:
        """Evaluate results and decide next steps."""
        logger.info("Evaluating results")
        
        # Check if we have significant results
        significant_results = [r for r in state["results"] if r.is_significant]
        
        if significant_results:
            return {
                "status": "COMPLETE",
                "metadata": {"significant_results": len(significant_results)},
            }
        else:
            return {
                "status": "EVALUATING",
                "metadata": {"no_significant_results": True},
            }
    
    def _should_continue(self, state: AgentState) -> Literal["continue", "end"]:
        """Decide if we should continue or end."""
        if state.get("error"):
            return "end"
        
        if state["iteration"] >= 3:  # Max iterations
            return "end"
        
        return "continue"
    
    def _should_refine(self, state: AgentState) -> Literal["refine", "end"]:
        """Decide if we should refine hypotheses or end."""
        if state.get("error"):
            return "end"
        
        if state["status"] == "COMPLETE":
            return "end"
        
        if state["iteration"] >= 3:
            return "end"
        
        # If no significant results, try refining
        significant = [r for r in state["results"] if r.is_significant]
        if not significant and state["iteration"] < 3:
            return "refine"
        
        return "end"
    
    def discover(self, domain: str, query: str, config: dict = None, 
                 discovered_variables: Optional[List[Variable]] = None,
                 active_data_sources: Optional[List[DataSource]] = None) -> DiscoveryState:
        """
        Start the causal discovery process.
        
        Args:
            domain: Domain name (e.g., "healthcare", "finance", "retail", "energy")
            query: User query about causal relationships
            config: Optional configuration for the discovery
            discovered_variables: Optional list of variables discovered by DataCurationAgent
            active_data_sources: Optional list of data sources discovered by DataCurationAgent
            
        Returns:
            DiscoveryState with results
        """
        # Use discovered variables and sources from DataCurationAgent if provided
        # This ensures hypothesis generation uses only available data
        initial_state: AgentState = {
            "domain": domain,
            "query": query,
            "hypotheses": [],
            "results": [],
            "active_data_sources": list(active_data_sources) if active_data_sources else [],
            "discovered_variables": list(discovered_variables) if discovered_variables else [],
            "iteration": 0,
            "status": "INITIALIZING",
            "error": None,
            "metadata": {},
            "messages": [],
        }
        
        if discovered_variables:
            logger.info(f"Starting causal discovery with {len(discovered_variables)} discovered variables from DataCurationAgent")
        if active_data_sources:
            logger.info(f"Starting causal discovery with {len(active_data_sources)} discovered data sources from DataCurationAgent")

        config = config or {}
        if config.get("offline_mode") is not None:
            self.offline_mode = bool(config["offline_mode"])
        if self.offline_mode and self.offline_df is None:
            # Try to load offline data if enabled later
            self.offline_df = self._load_offline_sample_df()

        def apply_update(current: dict, update: dict) -> dict:
            """Apply partial state updates while preserving initial keys."""
            result = dict(current)
            for key, value in update.items():
                if key == "metadata":
                    result[key] = {**result.get(key, {}), **value}
                elif key in ("hypotheses", "results", "active_data_sources", "discovered_variables"):
                    result[key] = result.get(key, []) + value
                else:
                    result[key] = value
            return result
        
        thread_id = config.get("thread_id", f"discovery_{datetime.now(timezone.utc).timestamp()}")
        recursion_limit = config.get("recursion_limit", 30)  # Default 30, was 25
        
        # Run the graph
        final_state = None
        merged_state = dict(initial_state)
        if self.app and LANGGRAPH_AVAILABLE:
            stream_config = {
                "configurable": {"thread_id": thread_id},
                "recursion_limit": recursion_limit
            }
            for update in self.app.stream(initial_state, config=stream_config):
                if isinstance(update, dict):
                    merged_state = apply_update(merged_state, update)
                else:
                    merged_state = update
                final_state = merged_state
                logger.debug(f"State update: {update}")
                
                # Safety check: if we've hit max iterations, break early to prevent infinite loop
                current_iteration = merged_state.get("iteration", 0)
                if current_iteration >= 3:
                    logger.warning(f"Reached max iterations ({current_iteration}), stopping graph execution")
                    break
        else:
            # Fallback: sequential execution
            final_state = self._run_sequential(initial_state)
        
        # Convert to DiscoveryState
        if final_state:
            last_state = final_state if isinstance(final_state, dict) else final_state
            return DiscoveryState(
                domain=last_state.get("domain", domain),
                query=last_state.get("query", query),
                hypotheses=last_state.get("hypotheses", []),
                results=last_state.get("results", []),
                active_data_sources=last_state.get("active_data_sources", []),
                discovered_variables=last_state.get("discovered_variables", []),
                iteration=last_state.get("iteration", 0),
                status=last_state.get("status", "ERROR"),
                error=last_state.get("error"),
                metadata=last_state.get("metadata", {}),
            )
        
        return DiscoveryState(domain=domain, query=query, status="ERROR", error="No state returned")
    
    def _run_sequential(self, initial_state: AgentState) -> AgentState:
        """Fallback sequential execution if LangGraph not available.
        
        Runs the causal analysis pipeline:
        generate_hypotheses -> run_causal_tests -> discover_confounders -> 
        test_confounders -> refine_hypotheses -> evaluate_results
        """
        state = dict(initial_state)
        
        def apply_update(current: dict, update: dict) -> dict:
            """Apply partial state update, handling list concatenation and dict merging."""
            result = dict(current)
            for key, value in update.items():
                if key == "metadata":
                    result[key] = {**result.get(key, {}), **value}
                elif key in ("hypotheses", "results", "active_data_sources", "discovered_variables"):
                    result[key] = result.get(key, []) + value
                else:
                    result[key] = value
            return result
        
        # Run causal analysis nodes sequentially
        update = self._generate_hypotheses_node(state)
        state = apply_update(state, update)
        if state.get("error"):
            return state
        
        update = self._run_causal_tests_node(state)
        state = apply_update(state, update)
        if state.get("error"):
            return state
        
        update = self._discover_confounders_node(state)
        state = apply_update(state, update)
        if state.get("error"):
            return state
        
        update = self._test_confounders_node(state)
        state = apply_update(state, update)
        if state.get("error"):
            return state
        
        update = self._refine_hypotheses_node(state)
        state = apply_update(state, update)
        
        update = self._evaluate_results_node(state)
        state = apply_update(state, update)
        
        return state


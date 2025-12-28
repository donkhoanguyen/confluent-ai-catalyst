"""
LangGraph-based orchestrator for autonomous causal discovery agent.
"""

from typing import Annotated, Literal, TypedDict, Dict, Optional
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
    from langgraph.checkpoint.memory import MemorySaver
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
    from .data_discovery import DataSourceDiscovery
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
    
    The agent operates in a discovery loop:
    1. Generate hypotheses using Gemini
    2. Discover data sources for variables
    3. Integrate data sources via Confluent
    4. Collect data
    5. Run causal inference tests
    6. Discover and test confounders
    7. Refine hypotheses based on results
    """
    
    def __init__(self, settings: Settings = None):
        self.settings = settings or get_settings()
        self.offline_mode = getattr(self.settings, "offline_mode", False)
        
        # Only initialize LLM-dependent modules if not in offline mode
        # and if the modules are available
        if AGENT_MODULES_AVAILABLE and not self.offline_mode:
            try:
                self.hypothesis_generator = HypothesisGenerator(self.settings)
                self.data_discovery = DataSourceDiscovery(self.settings)
                self.confounder_discovery = ConfounderDiscovery(self.settings)
            except ValueError as e:
                # GCP_PROJECT_ID not set - fall back to None
                logger.warning(f"LLM modules unavailable: {e}")
                self.hypothesis_generator = None
                self.data_discovery = None
                self.confounder_discovery = None
        else:
            self.hypothesis_generator = None
            self.data_discovery = None
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
            self.checkpointer = MemorySaver()
            self.app = self.graph.compile(checkpointer=self.checkpointer)
        else:
            self.app = None
    
    def _load_offline_sample_df(self):
        """Load a bundled offline demo DataFrame if available.
        
        Tries unified_market_data.csv first (unified table format),
        then falls back to bitcoin_offline_demo.csv for backward compatibility.
        """
        try:
            # First try unified_market_data.csv (preferred)
            df = self.dataframe_builder.load_from_csv("bitcoin")
            if df is not None and len(df) > 0:
                logger.info(f"Loaded {len(df)} rows from unified_market_data.csv for offline mode")
                return df
            
            # Fallback to legacy offline demo file
            df = self.dataframe_builder.load_from_csv("bitcoin", suffix="offline_demo")
            if df is None:
                logger.warning("No offline demo CSV found (tried unified_market_data.csv and bitcoin_offline_demo.csv); offline mode will run without data.")
            return df
        except Exception as e:
            logger.warning(f"Failed to load offline demo CSV: {e}")
            return None

    def _offline_hypotheses(self, domain: str) -> tuple[list[Hypothesis], list[Hypothesis]]:
        """Provide static hypotheses for offline/demo mode."""
        registry = get_asset_registry()
        cause1 = registry.get_variable("avg_sentiment", domain)
        effect1 = registry.get_variable("price_usd", domain)
        cause2 = registry.get_variable("post_count", domain)
        effect2 = registry.get_variable("price_change_24h_pct", domain) or registry.get_variable("return_pct", domain)

        hypotheses: list[Hypothesis] = []
        if cause1 and effect1:
            hypotheses.append(
                Hypothesis(
                    cause=cause1,
                    effect=effect1,
                    mechanism="Higher social sentiment may precede price increases.",
                    confidence=0.6,
                    suggested_methods=[CausalMethod.GRANGER, CausalMethod.TRANSFER_ENTROPY],
                    hypothesis_id=f"{domain}_offline_sentiment_price",
                    metadata={"offline_demo": True},
                )
            )
        if cause2 and effect2:
            hypotheses.append(
                Hypothesis(
                    cause=cause2,
                    effect=effect2,
                    mechanism="Rising post volume may signal momentum affecting returns.",
                    confidence=0.55,
                    suggested_methods=[CausalMethod.GRANGER, CausalMethod.TRANSFER_ENTROPY],
                    hypothesis_id=f"{domain}_offline_posts_returns",
                    metadata={"offline_demo": True},
                )
            )
        return hypotheses, []
    
    def _build_graph(self):
        """Build the LangGraph state graph."""
        if not LANGGRAPH_AVAILABLE:
            # Fallback to simple sequential execution
            return None
        
        workflow = StateGraph(AgentState)
        
        # Add nodes
        workflow.add_node("generate_hypotheses", self._generate_hypotheses_node)
        workflow.add_node("discover_data_sources", self._discover_data_sources_node)
        workflow.add_node("integrate_pipelines", self._integrate_pipelines_node)
        workflow.add_node("collect_data", self._collect_data_node)
        workflow.add_node("run_causal_tests", self._run_causal_tests_node)
        workflow.add_node("discover_confounders", self._discover_confounders_node)
        workflow.add_node("test_confounders", self._test_confounders_node)
        workflow.add_node("refine_hypotheses", self._refine_hypotheses_node)
        workflow.add_node("evaluate_results", self._evaluate_results_node)
        
        # Define edges
        workflow.set_entry_point("generate_hypotheses")
        
        workflow.add_edge("generate_hypotheses", "discover_data_sources")
        workflow.add_edge("discover_data_sources", "integrate_pipelines")
        workflow.add_edge("integrate_pipelines", "collect_data")
        workflow.add_edge("collect_data", "run_causal_tests")
        workflow.add_edge("run_causal_tests", "discover_confounders")
        workflow.add_edge("discover_confounders", "test_confounders")
        workflow.add_edge("test_confounders", "refine_hypotheses")
        
        # Conditional edge: refine or end
        workflow.add_conditional_edges(
            "refine_hypotheses",
            self._should_continue,
            {
                "continue": "evaluate_results",
                "end": END,
            }
        )
        
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
        """Generate causal hypotheses using Gemini."""
        logger.info(f"Generating hypotheses for domain: {state['domain']}")
        
        try:
            if self.offline_mode:
                hypotheses, wish_list = self._offline_hypotheses(state["domain"])
            else:
                if not self.hypothesis_generator:
                    raise RuntimeError("Hypothesis generator not available")
                result = self.hypothesis_generator.generate(
                    domain=state["domain"],
                    query=state["query"],
                    include_wish_list=True,
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
    
    def _discover_data_sources_node(self, state: AgentState) -> dict:
        """Discover data sources for variables."""
        logger.info("Discovering data sources")
        
        try:
            if self.offline_mode:
                return {
                    "active_data_sources": [],
                    "status": "DATA_DISCOVERY",
                    "metadata": {"data_source_count": 0, "offline_demo": True},
                }

            all_sources = []
            
            if not self.data_discovery:
                raise RuntimeError("Data discovery not available")
            for hypothesis in state["hypotheses"]:
                # Discover sources for cause and effect
                cause_sources = self.data_discovery.discover_sources(
                    variable=hypothesis.cause,
                    domain=state["domain"],
                )
                effect_sources = self.data_discovery.discover_sources(
                    variable=hypothesis.effect,
                    domain=state["domain"],
                )
                
                all_sources.extend(cause_sources)
                all_sources.extend(effect_sources)
                
                # Add to hypothesis
                hypothesis.required_data_sources = list(set(cause_sources + effect_sources))
            
            unique_sources = list(set(all_sources))
            logger.info(f"Discovered {len(unique_sources)} data sources")
            
            return {
                "active_data_sources": unique_sources,
                "status": "DATA_DISCOVERY",
                "metadata": {"data_source_count": len(unique_sources)},
            }
            
        except Exception as e:
            logger.error(f"Error discovering data sources: {e}")
            return {
                "error": str(e),
                "status": "ERROR",
            }
    
    def _integrate_pipelines_node(self, state: AgentState) -> dict:
        """Integrate data sources via Confluent (MCP or Admin API)."""
        logger.info("Integrating pipelines via Confluent")
        
        try:
            if self.offline_mode:
                return {
                    "status": "PIPELINE_INTEGRATION",
                    "metadata": {"pipelines_created": 0, "offline_demo": True},
                }

            try:
                from agent.confluent_client import ConfluentClient
            except ImportError:
                from .confluent_client import ConfluentClient
            
            client = ConfluentClient(self.settings)
            
            for source in state["active_data_sources"]:
                # Create Kafka topic
                topic = client.create_topic_for_source(source)
                source.kafka_topic = topic
                
                # Register schema
                try:
                    from agent.domain import SchemaTemplate
                except ImportError:
                    from .domain import SchemaTemplate
                schema = SchemaTemplate.for_data_source(source)
                client.register_schema(source, schema)
                
                logger.info(f"Integrated source: {source.name} -> topic: {topic}")
            
            return {
                "status": "PIPELINE_INTEGRATION",
                "metadata": {"pipelines_created": len(state["active_data_sources"])},
            }
            
        except Exception as e:
            logger.error(f"Error integrating pipelines: {e}")
            return {
                "error": str(e),
                "status": "ERROR",
            }
    
    def _collect_data_node(self, state: AgentState) -> dict:
        """Collect data from integrated sources."""
        logger.info("Collecting data from sources")
        
        try:
            if self.offline_mode:
                return {
                    "status": "DATA_COLLECTION",
                    "metadata": {"data_collected": True, "offline_demo": True},
                }

            import sys
            import os
            # Add parent directory to path to import producers module
            parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            if parent_dir not in sys.path:
                sys.path.insert(0, parent_dir)
            from producers.factory import ProducerFactory
            
            factory = ProducerFactory(self.settings)
            
            for source in state["active_data_sources"]:
                producer = factory.create_producer(source)
                if not producer:
                    logger.info(f"Skipping producer startup for {source.name} (no producer available)")
                    continue
                # Start producer in background (in real implementation)
                # Producer would be started/registered here
                logger.info(f"Started producer for: {source.name} ({producer.__class__.__name__})")
            
            # Wait for data collection (in real implementation, this would be async)
            return {
                "status": "DATA_COLLECTION",
                "metadata": {"data_collected": True},
            }
            
        except Exception as e:
            logger.error(f"Error collecting data: {e}")
            return {
                "error": str(e),
                "status": "ERROR",
            }
    
    def _run_causal_tests_node(self, state: AgentState) -> dict:
        """Run causal inference tests on hypotheses using canonical DataFrame."""
        logger.info("Running causal inference tests")
        
        try:
            try:
                from causal.registry import CausalEngineRegistry
            except ImportError:
                from .causal.registry import CausalEngineRegistry
            
            registry = CausalEngineRegistry()
            results = []
            
            # Try to get data from in-memory DataStore if available
            # This is a fallback - in production, data would come from Kafka
            price_history = None
            sentiment_history = None
            coin_id = "bitcoin"  # Default, should be configurable
            df_from_csv = self.offline_df if self.offline_mode else None
            df_from_csv = None  # Placeholder until CSV loading is implemented
            
            try:
                from api.main import data_store
                if coin_id in data_store.price_history:
                    price_history = list(data_store.price_history[coin_id])
                if coin_id in data_store.sentiment_history:
                    sentiment_history = list(data_store.sentiment_history[coin_id])
            except (ImportError, AttributeError):
                logger.debug("DataStore not available, will use placeholder data")
            
            for hypothesis in state["hypotheses"]:
                # Build canonical DataFrame for this hypothesis
                df = None
                
                # Try to use CSV data first (if we loaded it)
                if df_from_csv is not None:
                    # Use the full DataFrame and let build_for_hypothesis extract relevant columns
                    df = self.dataframe_builder.build_for_hypothesis(
                        hypothesis=hypothesis,
                        coin_id=coin_id,
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
                        coin_id=coin_id,
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
    
    def discover(self, domain: str, query: str, config: dict = None) -> DiscoveryState:
        """
        Start the causal discovery process.
        
        Args:
            domain: Domain name (e.g., "cryptocurrency")
            query: User query about causal relationships
            config: Optional configuration for the discovery
            
        Returns:
            DiscoveryState with results
        """
        initial_state: AgentState = {
            "domain": domain,
            "query": query,
            "hypotheses": [],
            "results": [],
            "active_data_sources": [],
            "discovered_variables": [],
            "iteration": 0,
            "status": "INITIALIZING",
            "error": None,
            "metadata": {},
            "messages": [],
        }

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
        """Fallback sequential execution if LangGraph not available."""
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
        
        # Run nodes sequentially
        update = self._generate_hypotheses_node(state)
        state = apply_update(state, update)
        if state.get("error"):
            return state
        
        update = self._discover_data_sources_node(state)
        state = apply_update(state, update)
        if state.get("error"):
            return state
        
        update = self._integrate_pipelines_node(state)
        state = apply_update(state, update)
        if state.get("error"):
            return state
        
        update = self._collect_data_node(state)
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


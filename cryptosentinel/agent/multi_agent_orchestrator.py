"""
Multi-Agent Orchestrator combining Data Curation and Causal Discovery agents.

This orchestrator creates a single LangGraph pipeline with two high-level nodes:
1. data_curation: Runs the DataCurationAgent to discover and prepare data
2. causal: Runs the CausalDiscoveryAgent to generate hypotheses and run causal tests

The pipeline flow: data_curation -> (if ready) -> causal -> END
"""

from typing import Annotated, Literal, TypedDict, Optional, List
from datetime import datetime, timezone
from operator import add
import os

from loguru import logger
from dotenv import load_dotenv

# Load .env file to ensure environment variables are available
load_dotenv(override=True)

# Ensure LANGCHAIN_API_KEY is set if LANGSMITH_API_KEY exists
if os.getenv("LANGSMITH_API_KEY") and not os.getenv("LANGCHAIN_API_KEY"):
    os.environ["LANGCHAIN_API_KEY"] = os.getenv("LANGSMITH_API_KEY")
    logger.debug("Set LANGCHAIN_API_KEY from LANGSMITH_API_KEY for LangSmith tracing")

try:
    from langgraph.graph import StateGraph, END
    from langgraph.graph.message import add_messages
    LANGGRAPH_AVAILABLE = True
except ImportError:
    LANGGRAPH_AVAILABLE = False
    logger.warning("langgraph not available - multi-agent orchestrator requires langgraph")

from .models import DataSource, Variable, Hypothesis, CausalResult
from .curation_orchestrator import DataCurationAgent, CurationState
from .orchestrator import CausalDiscoveryAgent, AgentState
from config.settings import Settings, get_settings


def merge_dicts(a: dict, b: dict) -> dict:
    """Merge two dictionaries, with b taking precedence."""
    return {**a, **b}


class MultiAgentState(TypedDict):
    """Unified state for the multi-agent pipeline.
    
    Combines state from both DataCurationAgent and CausalDiscoveryAgent.
    """
    # Domain and query
    domain: str
    query: str
    
    # Data curation state
    datasets: Annotated[list[dict], add]
    readiness_score: float
    is_ready: bool
    min_rows_required: int
    current_row_count: int
    
    # Shared discovery state
    active_data_sources: Annotated[list[DataSource], add]
    discovered_variables: Annotated[list[Variable], add]
    
    # Causal discovery state
    hypotheses: Annotated[list[Hypothesis], add]
    results: Annotated[list[CausalResult], add]
    
    # Control flow
    iteration: int
    status: str
    error: Optional[str]
    metadata: Annotated[dict, merge_dicts]
    messages: Annotated[list, add_messages]
    
    # Agent-specific state snapshots (for debugging)
    curation_state: Optional[dict]
    causal_state: Optional[dict]


class MultiAgentOrchestrator:
    """
    Multi-agent orchestrator that combines Data Curation and Causal Discovery.
    
    This creates a single LangGraph pipeline with two high-level nodes:
    - data_curation: Discovers data sources, integrates pipelines, collects data, checks readiness
    - causal: Generates hypotheses, runs causal tests, discovers confounders, refines
    
    Flow: data_curation -> (if ready) -> causal -> END
    """
    
    def __init__(self, settings: Settings = None):
        self.settings = settings or get_settings()
        self.offline_mode = getattr(self.settings, "offline_mode", False)
        
        # Initialize both agents
        self.curation_agent = DataCurationAgent(self.settings)
        self.causal_agent = CausalDiscoveryAgent(self.settings)
        
        # Build the multi-agent graph
        self.graph = self._build_graph()
        if self.graph and LANGGRAPH_AVAILABLE:
            self.app = self.graph.compile()
        else:
            self.app = None
            if not LANGGRAPH_AVAILABLE:
                logger.error("LangGraph not available - cannot create multi-agent orchestrator")
    
    def _build_graph(self):
        """Build the multi-agent LangGraph pipeline with nested subgraphs.
        
        Graph structure:
        - Entry: data_curation subgraph (shows internal workflow)
        - Conditional: if is_ready -> causal subgraph, else -> END
        - causal subgraph -> END
        
        This creates a single pipeline view in LangGraph Dev showing nested graphs:
        data_curation (with internal nodes visible) -> causal (with internal nodes visible) -> END
        
        We rebuild the agent subgraphs inline with the parent state schema (MultiAgentState)
        to ensure state compatibility while showing nested structure.
        """
        if not LANGGRAPH_AVAILABLE:
            return None
        
        workflow = StateGraph(MultiAgentState)
        
        # Build subgraphs with MultiAgentState schema so state flows correctly
        curation_subgraph = self._build_curation_subgraph()
        causal_subgraph = self._build_causal_subgraph()
        
        # Add compiled subgraphs as nodes - LangGraph will show their internal structure
        workflow.add_node("data_curation", curation_subgraph)
        workflow.add_node("causal", causal_subgraph)
        
        # Set entry point
        workflow.set_entry_point("data_curation")
        
        # Conditional edge: if data is ready, proceed to causal analysis
        workflow.add_conditional_edges(
            "data_curation",
            self._check_readiness_and_route,
            {
                "causal": "causal",
                END: END,
            }
        )
        
        # Causal node always ends
        workflow.add_edge("causal", END)
        
        return workflow
    
    def _build_curation_subgraph(self):
        """Build curation subgraph using MultiAgentState schema.
        
        This rebuilds the curation agent's workflow using the parent state schema
        so that state flows correctly between the parent and subgraph.
        """
        subgraph = StateGraph(MultiAgentState)
        
        # Add curation nodes - these delegate to the curation agent's node methods
        subgraph.add_node("discover_data_sources", self._curation_discover_node)
        subgraph.add_node("integrate_pipelines", self._curation_integrate_node)
        subgraph.add_node("collect_data", self._curation_collect_node)
        subgraph.add_node("compute_readiness", self._curation_readiness_node)
        
        # Set entry point
        subgraph.set_entry_point("discover_data_sources")
        
        # Define edges
        subgraph.add_edge("discover_data_sources", "integrate_pipelines")
        subgraph.add_edge("integrate_pipelines", "collect_data")
        subgraph.add_edge("collect_data", "compute_readiness")
        subgraph.add_edge("compute_readiness", END)
        
        return subgraph.compile()
    
    def _build_causal_subgraph(self):
        """Build causal subgraph using MultiAgentState schema.
        
        This rebuilds the causal agent's workflow using the parent state schema
        so that state flows correctly between the parent and subgraph.
        """
        subgraph = StateGraph(MultiAgentState)
        
        # Add causal nodes - these delegate to the causal agent's node methods
        subgraph.add_node("generate_hypotheses", self._causal_generate_node)
        subgraph.add_node("run_causal_tests", self._causal_test_node)
        subgraph.add_node("discover_confounders", self._causal_confounders_node)
        subgraph.add_node("test_confounders", self._causal_test_confounders_node)
        subgraph.add_node("refine_hypotheses", self._causal_refine_node)
        subgraph.add_node("evaluate_results", self._causal_evaluate_node)
        
        # Set entry point
        subgraph.set_entry_point("generate_hypotheses")
        
        # Define edges
        subgraph.add_edge("generate_hypotheses", "run_causal_tests")
        subgraph.add_edge("run_causal_tests", "discover_confounders")
        subgraph.add_edge("discover_confounders", "test_confounders")
        subgraph.add_edge("test_confounders", "refine_hypotheses")
        subgraph.add_edge("refine_hypotheses", "evaluate_results")
        subgraph.add_edge("evaluate_results", END)
        
        return subgraph.compile()
    
    # =========================================================================
    # Curation subgraph node wrappers
    # =========================================================================
    
    def _curation_discover_node(self, state: MultiAgentState) -> dict:
        """Discover data sources - delegates to curation agent."""
        curation_state = self._to_curation_state(state)
        result = self.curation_agent._discover_data_sources_node(curation_state)
        return self._merge_curation_result(result, state)
    
    def _curation_integrate_node(self, state: MultiAgentState) -> dict:
        """Integrate pipelines - delegates to curation agent."""
        curation_state = self._to_curation_state(state)
        result = self.curation_agent._integrate_pipelines_node(curation_state)
        return self._merge_curation_result(result, state)
    
    def _curation_collect_node(self, state: MultiAgentState) -> dict:
        """Collect data - delegates to curation agent."""
        curation_state = self._to_curation_state(state)
        result = self.curation_agent._collect_data_node(curation_state)
        return self._merge_curation_result(result, state)
    
    def _curation_readiness_node(self, state: MultiAgentState) -> dict:
        """Compute readiness - delegates to curation agent."""
        curation_state = self._to_curation_state(state)
        result = self.curation_agent._compute_readiness_node(curation_state)
        return self._merge_curation_result(result, state)
    
    def _merge_curation_result(self, result: dict, original_state: MultiAgentState) -> dict:
        """Merge curation node result back to MultiAgentState format."""
        merged = {}
        for key, value in result.items():
            if key in ("datasets", "active_data_sources", "discovered_variables"):
                merged[key] = value
            elif key == "metadata":
                merged[key] = {**original_state.get("metadata", {}), **value}
            elif key in ("readiness_score", "is_ready", "current_row_count", "status", "error", "iteration"):
                merged[key] = value
        return merged
    
    # =========================================================================
    # Causal subgraph node wrappers
    # =========================================================================
    
    def _causal_generate_node(self, state: MultiAgentState) -> dict:
        """Generate hypotheses - delegates to causal agent."""
        agent_state = self._to_agent_state(state)
        result = self.causal_agent._generate_hypotheses_node(agent_state)
        return self._merge_causal_result(result, state)
    
    def _causal_test_node(self, state: MultiAgentState) -> dict:
        """Run causal tests - delegates to causal agent."""
        agent_state = self._to_agent_state(state)
        result = self.causal_agent._run_causal_tests_node(agent_state)
        return self._merge_causal_result(result, state)
    
    def _causal_confounders_node(self, state: MultiAgentState) -> dict:
        """Discover confounders - delegates to causal agent."""
        agent_state = self._to_agent_state(state)
        result = self.causal_agent._discover_confounders_node(agent_state)
        return self._merge_causal_result(result, state)
    
    def _causal_test_confounders_node(self, state: MultiAgentState) -> dict:
        """Test confounders - delegates to causal agent."""
        agent_state = self._to_agent_state(state)
        result = self.causal_agent._test_confounders_node(agent_state)
        return self._merge_causal_result(result, state)
    
    def _causal_refine_node(self, state: MultiAgentState) -> dict:
        """Refine hypotheses - delegates to causal agent."""
        agent_state = self._to_agent_state(state)
        result = self.causal_agent._refine_hypotheses_node(agent_state)
        return self._merge_causal_result(result, state)
    
    def _causal_evaluate_node(self, state: MultiAgentState) -> dict:
        """Evaluate results - delegates to causal agent."""
        agent_state = self._to_agent_state(state)
        result = self.causal_agent._evaluate_results_node(agent_state)
        return self._merge_causal_result(result, state)
    
    def _merge_causal_result(self, result: dict, original_state: MultiAgentState) -> dict:
        """Merge causal node result back to MultiAgentState format."""
        merged = {}
        for key, value in result.items():
            if key in ("hypotheses", "results", "discovered_variables", "active_data_sources"):
                merged[key] = value
            elif key == "metadata":
                merged[key] = {**original_state.get("metadata", {}), **value}
            elif key in ("status", "error", "iteration"):
                merged[key] = value
        return merged
    
    def _to_curation_state(self, state: MultiAgentState) -> CurationState:
        """Convert MultiAgentState to CurationState for the curation agent."""
        return CurationState(
            domain=state["domain"],
            datasets=state.get("datasets", []),
            active_data_sources=state.get("active_data_sources", []),
            discovered_variables=state.get("discovered_variables", []),
            readiness_score=state.get("readiness_score", 0.0),
            is_ready=state.get("is_ready", False),
            min_rows_required=state.get("min_rows_required", 100),
            current_row_count=state.get("current_row_count", 0),
            pending_runs=[],
            completed_runs=[],
            iteration=0,
            status=state.get("status", "INITIALIZING"),
            error=state.get("error"),
            metadata=state.get("metadata", {}),
            messages=state.get("messages", []),
        )
    
    def _from_curation_state(self, curation_state: CurationState, original_state: MultiAgentState) -> dict:
        """Convert CurationState back to MultiAgentState updates."""
        return {
            "datasets": curation_state.get("datasets", []),
            "discovered_variables": curation_state.get("discovered_variables", []),
            "active_data_sources": curation_state.get("active_data_sources", []),
            "readiness_score": curation_state.get("readiness_score", 0.0),
            "is_ready": curation_state.get("is_ready", False),
            "current_row_count": curation_state.get("current_row_count", 0),
            "status": curation_state.get("status", "UNKNOWN"),
            "error": curation_state.get("error"),
            "curation_state": dict(curation_state),
            "metadata": {
                **original_state.get("metadata", {}),
                **curation_state.get("metadata", {}),
                "curation_status": curation_state.get("status"),
                "curation_iteration": curation_state.get("iteration", 0),
            },
        }
    
    def _to_agent_state(self, state: MultiAgentState) -> AgentState:
        """Convert MultiAgentState to AgentState for the causal agent."""
        return AgentState(
            domain=state["domain"],
            query=state["query"],
            hypotheses=state.get("hypotheses", []),
            results=state.get("results", []),
            active_data_sources=state.get("active_data_sources", []),
            discovered_variables=state.get("discovered_variables", []),
            iteration=state.get("iteration", 0),
            status=state.get("status", "INITIALIZING"),
            error=state.get("error"),
            metadata=state.get("metadata", {}),
            messages=state.get("messages", []),
        )
    
    def _from_agent_state(self, agent_state: AgentState, original_state: MultiAgentState) -> dict:
        """Convert AgentState back to MultiAgentState updates."""
        return {
            "hypotheses": agent_state.get("hypotheses", []),
            "results": agent_state.get("results", []),
            "discovered_variables": agent_state.get("discovered_variables", []),
            "active_data_sources": agent_state.get("active_data_sources", []),
            "iteration": agent_state.get("iteration", 0),
            "status": agent_state.get("status", "UNKNOWN"),
            "error": agent_state.get("error"),
            "causal_state": dict(agent_state),
            "metadata": {
                **original_state.get("metadata", {}),
                **agent_state.get("metadata", {}),
                "causal_status": agent_state.get("status"),
                "causal_iteration": agent_state.get("iteration", 0),
            },
        }
    
    def _data_curation_subgraph_wrapper(self, state: MultiAgentState) -> dict:
        """
        Wrapper that runs the curation agent's graph as a subgraph.
        
        This will show the internal curation workflow in LangGraph Dev:
        discover_data_sources -> integrate_pipelines -> collect_data -> 
        compute_readiness -> (loop or END)
        """
        logger.info(f"Running data curation subgraph for domain: {state['domain']}")
        
        if not self.curation_agent.app:
            # Fallback to direct method call if graph not available
            return self._data_curation_node_fallback(state)
        
        try:
            # Convert to curation state
            curation_state = self._to_curation_state(state)
            
            # Extract curation config
            curation_config = state.get("metadata", {}).get("curation_config", {})
            curation_config.setdefault("offline_mode", self.offline_mode)
            thread_id = curation_config.get("thread_id", f"curation_{state['domain']}")
            
            # Run the curation agent's graph
            stream_config = {
                "configurable": {"thread_id": thread_id},
                "recursion_limit": curation_config.get("recursion_limit", 50),
            }
            
            final_curation_state = None
            for update in self.curation_agent.app.stream(curation_state, config=stream_config):
                if isinstance(update, dict):
                    # Merge updates
                    for key, value in update.items():
                        if key == "metadata":
                            curation_state[key] = {**curation_state.get(key, {}), **value}
                        elif key in ("datasets", "active_data_sources", "discovered_variables", 
                                    "pending_runs", "completed_runs"):
                            curation_state[key] = curation_state.get(key, []) + value
                        else:
                            curation_state[key] = value
                    final_curation_state = curation_state
                else:
                    final_curation_state = update
                
                # Break if ready or error
                if curation_state.get("is_ready") or curation_state.get("error"):
                    break
            
            if final_curation_state is None:
                final_curation_state = curation_state
            
            # Convert back to multi-agent state
            return self._from_curation_state(final_curation_state, state)
            
        except Exception as e:
            logger.error(f"Error in data curation subgraph: {e}", exc_info=True)
            return {
                "error": str(e),
                "status": "ERROR",
                "is_ready": False,
                "metadata": {
                    **state.get("metadata", {}),
                    "curation_error": str(e),
                },
            }
    
    def _data_curation_node_fallback(self, state: MultiAgentState) -> dict:
        """Fallback: call curate method directly if graph not available."""
        return self._data_curation_node(state)
    
    def _data_curation_node(self, state: MultiAgentState) -> dict:
        """
        Wrapper node that runs the DataCurationAgent.
        
        This node executes the full curation pipeline:
        - Discover data sources
        - Integrate pipelines
        - Collect data
        - Compute readiness
        
        Returns state updates with curation results.
        """
        logger.info(f"Running data curation for domain: {state['domain']}")
        
        try:
            # Extract curation config from state metadata
            curation_config = state.get("metadata", {}).get("curation_config", {})
            curation_config.setdefault("offline_mode", self.offline_mode)
            
            # Run the curation agent
            curation_result = self.curation_agent.curate(
                domain=state["domain"],
                config=curation_config,
            )
            
            # Extract key fields from curation result
            # curation_result is a CurationState dict
            discovered_vars = curation_result.get("discovered_variables", [])
            active_sources = curation_result.get("active_data_sources", [])
            datasets = curation_result.get("datasets", [])
            readiness_score = curation_result.get("readiness_score", 0.0)
            is_ready = curation_result.get("is_ready", False)
            current_rows = curation_result.get("current_row_count", 0)
            
            logger.info(
                f"Curation complete: {len(discovered_vars)} variables, "
                f"{len(active_sources)} sources, readiness={readiness_score:.2%}, ready={is_ready}"
            )
            
            return {
                "datasets": datasets,
                "discovered_variables": discovered_vars,
                "active_data_sources": active_sources,
                "readiness_score": readiness_score,
                "is_ready": is_ready,
                "current_row_count": current_rows,
                "status": "DATA_CURATION_COMPLETE" if is_ready else "DATA_CURATION_IN_PROGRESS",
                "curation_state": curation_result,
                "metadata": {
                    **state.get("metadata", {}),
                    "curation_status": curation_result.get("status"),
                    "curation_iteration": curation_result.get("iteration", 0),
                    "sources_discovered": len(active_sources),
                    "variables_discovered": len(discovered_vars),
                },
            }
            
        except Exception as e:
            logger.error(f"Error in data curation node: {e}", exc_info=True)
            return {
                "error": str(e),
                "status": "ERROR",
                "is_ready": False,
                "metadata": {
                    **state.get("metadata", {}),
                    "curation_error": str(e),
                },
            }
    
    def _causal_subgraph_wrapper(self, state: MultiAgentState) -> dict:
        """
        Wrapper that runs the causal agent's graph as a subgraph.
        
        This will show the internal causal workflow in LangGraph Dev:
        generate_hypotheses -> run_causal_tests -> discover_confounders -> 
        test_confounders -> refine_hypotheses -> evaluate_results -> (loop or END)
        """
        logger.info(f"Running causal discovery subgraph for domain: {state['domain']}, query: {state['query']}")
        
        if not self.causal_agent.app:
            # Fallback to direct method call if graph not available
            return self._causal_node_fallback(state)
        
        try:
            # Convert to agent state
            agent_state = self._to_agent_state(state)
            
            # Extract causal config
            causal_config = state.get("metadata", {}).get("causal_config", {})
            causal_config.setdefault("offline_mode", self.offline_mode)
            thread_id = causal_config.get("thread_id", f"causal_{state['domain']}")
            
            # Run the causal agent's graph
            stream_config = {
                "configurable": {"thread_id": thread_id},
                "recursion_limit": causal_config.get("recursion_limit", 30),
            }
            
            final_agent_state = None
            for update in self.causal_agent.app.stream(agent_state, config=stream_config):
                if isinstance(update, dict):
                    # Merge updates
                    for key, value in update.items():
                        if key == "metadata":
                            agent_state[key] = {**agent_state.get(key, {}), **value}
                        elif key in ("hypotheses", "results", "active_data_sources", "discovered_variables"):
                            agent_state[key] = agent_state.get(key, []) + value
                        else:
                            agent_state[key] = value
                    final_agent_state = agent_state
                else:
                    final_agent_state = update
                
                # Break on error or completion
                if agent_state.get("error") or agent_state.get("status") == "COMPLETE":
                    break
            
            if final_agent_state is None:
                final_agent_state = agent_state
            
            # Convert back to multi-agent state
            return self._from_agent_state(final_agent_state, state)
            
        except Exception as e:
            logger.error(f"Error in causal subgraph: {e}", exc_info=True)
            return {
                "error": str(e),
                "status": "ERROR",
                "metadata": {
                    **state.get("metadata", {}),
                    "causal_error": str(e),
                },
            }
    
    def _causal_node_fallback(self, state: MultiAgentState) -> dict:
        """Fallback: call discover method directly if graph not available."""
        return self._causal_node(state)
    
    def _causal_node(self, state: MultiAgentState) -> dict:
        """
        Wrapper node that runs the CausalDiscoveryAgent.
        
        This node executes the full causal discovery pipeline:
        - Generate hypotheses
        - Run causal tests
        - Discover confounders
        - Test confounders
        - Refine hypotheses
        - Evaluate results
        
        Uses discovered variables and data sources from the curation node.
        """
        logger.info(f"Running causal discovery for domain: {state['domain']}, query: {state['query']}")
        
        try:
            # Extract causal config from state metadata
            causal_config = state.get("metadata", {}).get("causal_config", {})
            causal_config.setdefault("offline_mode", self.offline_mode)
            
            # Get discovered variables and sources from curation
            discovered_vars = state.get("discovered_variables", [])
            active_sources = state.get("active_data_sources", [])
            
            logger.info(
                f"Starting causal discovery with {len(discovered_vars)} variables "
                f"and {len(active_sources)} data sources"
            )
            
            # Run the causal agent
            causal_result = self.causal_agent.discover(
                domain=state["domain"],
                query=state["query"],
                config=causal_config,
                discovered_variables=discovered_vars if discovered_vars else None,
                active_data_sources=active_sources if active_sources else None,
            )
            
            # Extract key fields from causal result
            # causal_result is a DiscoveryState dataclass
            hypotheses = causal_result.hypotheses if causal_result.hypotheses else []
            results = causal_result.results if causal_result.results else []
            causal_status = causal_result.status if causal_result.status else "UNKNOWN"
            causal_iteration = causal_result.iteration if causal_result.iteration else 0
            
            # Merge any additional discovered variables
            additional_vars = []
            if causal_result.discovered_variables:
                existing_var_names = {v.name for v in discovered_vars}
                for var in causal_result.discovered_variables:
                    if var.name not in existing_var_names:
                        additional_vars.append(var)
            
            logger.info(
                f"Causal discovery complete: {len(hypotheses)} hypotheses, "
                f"{len(results)} results, status={causal_status}"
            )
            
            return {
                "hypotheses": hypotheses,
                "results": results,
                "discovered_variables": additional_vars,
                "iteration": causal_iteration,
                "status": causal_status,
                "causal_state": {
                    "hypotheses": [h.__dict__ if hasattr(h, '__dict__') else str(h) for h in hypotheses],
                    "results": [r.to_dict() if hasattr(r, 'to_dict') else str(r) for r in results],
                    "status": causal_status,
                    "iteration": causal_iteration,
                },
                "metadata": {
                    **state.get("metadata", {}),
                    "causal_status": causal_status,
                    "causal_iteration": causal_iteration,
                    "hypotheses_count": len(hypotheses),
                    "results_count": len(results),
                    "significant_results": len([r for r in results if getattr(r, 'is_significant', False)]),
                },
            }
            
        except Exception as e:
            logger.error(f"Error in causal node: {e}", exc_info=True)
            return {
                "error": str(e),
                "status": "ERROR",
                "metadata": {
                    **state.get("metadata", {}),
                    "causal_error": str(e),
                },
            }
    
    def _check_readiness_and_route(self, state: MultiAgentState) -> Literal["causal", "__end__"]:
        """
        Conditional routing function.
        
        Routes to causal node if data is ready, otherwise ends the pipeline.
        """
        if state.get("error"):
            logger.warning("Error detected, ending pipeline")
            return END
        
        is_ready = state.get("is_ready", False)
        
        if is_ready:
            logger.info("Data is ready, proceeding to causal discovery")
            return "causal"
        else:
            readiness_score = state.get("readiness_score", 0.0)
            logger.info(
                f"Data not ready (readiness={readiness_score:.2%}), ending pipeline. "
                f"Re-run when data is ready."
            )
            return END
    
    def run(self, domain: str, query: str, config: dict = None) -> MultiAgentState:
        """
        Run the complete multi-agent pipeline.
        
        Args:
            domain: Domain name (e.g., "cryptocurrency", "healthcare", "finance")
            query: User query about causal relationships
            config: Optional configuration dict with:
                - curation_config: Config for DataCurationAgent
                - causal_config: Config for CausalDiscoveryAgent
                - offline_mode: Override offline mode setting
                - thread_id: Thread ID for LangGraph persistence
                - recursion_limit: Recursion limit for LangGraph
                
        Returns:
            Final MultiAgentState with results from both agents
        """
        config = config or {}
        
        # Extract nested configs
        curation_config = config.get("curation_config", {})
        causal_config = config.get("causal_config", {})
        
        # Override offline mode if specified
        if config.get("offline_mode") is not None:
            self.offline_mode = bool(config["offline_mode"])
            curation_config["offline_mode"] = self.offline_mode
            causal_config["offline_mode"] = self.offline_mode
        
        # Build initial state
        initial_state: MultiAgentState = {
            "domain": domain,
            "query": query,
            "datasets": [],
            "readiness_score": 0.0,
            "is_ready": False,
            "min_rows_required": curation_config.get("min_rows", 100),
            "current_row_count": 0,
            "active_data_sources": [],
            "discovered_variables": [],
            "hypotheses": [],
            "results": [],
            "iteration": 0,
            "status": "INITIALIZING",
            "error": None,
            "metadata": {
                "curation_config": curation_config,
                "causal_config": causal_config,
                "pipeline_started": datetime.now(timezone.utc).isoformat(),
            },
            "messages": [],
            "curation_state": None,
            "causal_state": None,
        }
        
        if not self.app:
            logger.error("Multi-agent app not available")
            return {**initial_state, "error": "LangGraph app not available", "status": "ERROR"}
        
        # Run the graph
        thread_id = config.get("thread_id", f"multi_agent_{domain}_{datetime.now(timezone.utc).timestamp()}")
        recursion_limit = config.get("recursion_limit", 50)
        
        stream_config = {
            "configurable": {"thread_id": thread_id},
            "recursion_limit": recursion_limit,
        }
        
        final_state = None
        merged_state = dict(initial_state)
        
        try:
            for update in self.app.stream(initial_state, config=stream_config):
                if isinstance(update, dict):
                    # Merge update into state
                    for key, value in update.items():
                        if key == "metadata":
                            merged_state[key] = {**merged_state.get(key, {}), **value}
                        elif key in ("datasets", "active_data_sources", "discovered_variables", 
                                    "hypotheses", "results"):
                            # Handle list concatenation
                            existing = merged_state.get(key, [])
                            if isinstance(value, list):
                                merged_state[key] = existing + value
                            else:
                                merged_state[key] = existing
                        else:
                            merged_state[key] = value
                    final_state = merged_state
                else:
                    final_state = update
                
                # Log progress
                current_status = merged_state.get("status", "UNKNOWN")
                logger.debug(f"Pipeline status: {current_status}")
                
                # Break on error
                if merged_state.get("error"):
                    break
            
            if final_state:
                final_state["metadata"]["pipeline_completed"] = datetime.now(timezone.utc).isoformat()
            
            return final_state or merged_state
            
        except Exception as e:
            logger.error(f"Error running multi-agent pipeline: {e}", exc_info=True)
            return {
                **merged_state,
                "error": str(e),
                "status": "ERROR",
            }


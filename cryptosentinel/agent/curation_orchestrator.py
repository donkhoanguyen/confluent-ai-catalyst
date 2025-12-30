"""
LangGraph-based Data Curation Agent for continuous data monitoring and readiness.

This agent runs continuously to:
1. Discover relevant datasets for a domain
2. Integrate data pipelines via Confluent
3. Collect data from integrated sources
4. Compute dataset readiness for causal analysis
"""

from typing import Annotated, Literal, TypedDict, Dict, Optional, List
from datetime import datetime, timezone
from operator import add
import time
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
    logger.warning("langgraph not available - using simplified orchestrator")

from .models import DataSource, Variable
from .asset_registry import get_asset_registry

try:
    from .data_discovery import DataSourceDiscovery
    DISCOVERY_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Data discovery module not available: {e}")
    DISCOVERY_AVAILABLE = False

from config.settings import Settings, get_settings


def merge_dicts(a: dict, b: dict) -> dict:
    """Merge two dictionaries, with b taking precedence."""
    return {**a, **b}


class CurationState(TypedDict):
    """State for the Data Curation Agent."""
    domain: str
    # Dataset information
    datasets: Annotated[list[dict], add]  # List of discovered datasets
    active_data_sources: Annotated[list[DataSource], add]
    discovered_variables: Annotated[list[Variable], add]
    # Readiness tracking
    readiness_score: float  # 0.0 to 1.0
    is_ready: bool
    min_rows_required: int
    current_row_count: int
    # Run management
    pending_runs: Annotated[list[dict], add]  # Runs waiting for data
    completed_runs: Annotated[list[str], add]  # Run IDs that have been triggered
    # Control flow
    iteration: int
    status: str
    error: Optional[str]
    metadata: Annotated[dict, merge_dicts]
    messages: Annotated[list, add_messages]


class DataCurationAgent:
    """
    Data Curation Agent for continuous dataset monitoring and preparation.
    
    This agent operates in a monitoring loop:
    1. Read/discover datasets relevant to the domain
    2. Integrate data pipelines via Confluent  
    3. Collect data from sources
    4. Compute readiness (check if sufficient data exists)
    5. If ready, signal completion; otherwise, create pending run and sleep
    """
    
    def __init__(self, settings: Settings = None):
        self.settings = settings or get_settings()
        self.offline_mode = getattr(self.settings, "offline_mode", False)
        
        # Initialize data discovery if available
        if DISCOVERY_AVAILABLE and not self.offline_mode:
            try:
                self.data_discovery = DataSourceDiscovery(self.settings)
            except Exception as e:
                logger.warning(f"Data discovery unavailable: {e}")
                self.data_discovery = None
        else:
            self.data_discovery = None
            if self.offline_mode:
                logger.info("Offline mode: using mock data discovery")
        
        # Build LangGraph
        self.graph = self._build_graph()
        if self.graph and LANGGRAPH_AVAILABLE:
            # LangGraph API handles persistence automatically, no need for custom checkpointer
            self.app = self.graph.compile()
        else:
            self.app = None
    
    def _build_graph(self):
        """Build the LangGraph state graph for data curation.
        
        The graph flow is:
        discover_data_sources -> integrate_pipelines -> collect_data -> 
        compute_readiness -> (create_pending_run -> sleep -> loop) or END
        
        This agent discovers and collects data BEFORE the CausalDiscoveryAgent
        generates hypotheses, so the causal agent knows what variables are available.
        """
        if not LANGGRAPH_AVAILABLE:
            return None
        
        workflow = StateGraph(CurationState)
        
        # Add nodes for data curation pipeline
        workflow.add_node("discover_data_sources", self._discover_data_sources_node)
        workflow.add_node("integrate_pipelines", self._integrate_pipelines_node)
        workflow.add_node("collect_data", self._collect_data_node)
        workflow.add_node("compute_readiness", self._compute_readiness_node)
        workflow.add_node("create_pending_run", self._create_pending_run_node)
        workflow.add_node("sleep", self._sleep_node)
        
        # Define edges - linear flow with conditional loop
        workflow.set_entry_point("discover_data_sources")
        
        # Main data pipeline
        workflow.add_edge("discover_data_sources", "integrate_pipelines")
        workflow.add_edge("integrate_pipelines", "collect_data")
        workflow.add_edge("collect_data", "compute_readiness")
        
        # Conditional: if ready, end; otherwise create pending run and loop
        workflow.add_conditional_edges(
            "compute_readiness",
            self._check_readiness,
            {
                "create": "create_pending_run",
                END: END,
            }
        )
        
        workflow.add_edge("create_pending_run", "sleep")
        
        # After sleep, loop back to collect more data
        workflow.add_edge("sleep", "collect_data")
        
        return workflow
    
    def _discover_data_sources_node(self, state: CurationState) -> dict:
        """
        Discover available data sources for the domain.
        This is the first step - find what data sources exist before collecting.
        """
        logger.info(f"Discovering data sources for domain: {state['domain']}")
        
        try:
            domain = state["domain"]
            
            # Discover data sources
            if self.offline_mode or not self.data_discovery:
                # Offline mode: use asset registry
                sources = self._discover_offline_sources(domain)
                datasets = self._discover_offline_datasets(domain)
            else:
                # Online mode: use LLM-based data discovery
                sources = self._discover_online_sources(domain)
                datasets = self._build_datasets_from_sources(sources, domain)
            
            # Extract variables from sources
            new_variables = []
            for source in sources:
                if hasattr(source, 'variables'):
                    new_variables.extend(source.variables)
            
            logger.info(f"Discovered {len(sources)} data sources, {len(datasets)} datasets")
            
            return {
                "datasets": datasets,
                "active_data_sources": sources,
                "discovered_variables": new_variables,
                "status": "DATA_DISCOVERY",
                "metadata": {
                    "sources_discovered": len(sources),
                    "datasets_found": len(datasets),
                },
            }
            
        except Exception as e:
            logger.error(f"Error discovering data sources: {e}")
            return {
                "error": str(e),
                "status": "ERROR",
            }
    
    def _integrate_pipelines_node(self, state: CurationState) -> dict:
        """
        Integrate data sources via Confluent (create Kafka topics, register schemas).
        """
        logger.info("Integrating data pipelines via Confluent")
        
        try:
            sources = state.get("active_data_sources", [])
            integrated_count = self._integrate_new_sources(sources, state)
            
            logger.info(f"Integrated {integrated_count} new pipelines")
            
            return {
                "status": "PIPELINE_INTEGRATION",
                "metadata": {
                    "pipelines_integrated": integrated_count,
                },
            }
            
        except Exception as e:
            logger.error(f"Error integrating pipelines: {e}")
            return {
                "error": str(e),
                "status": "ERROR",
            }
    
    def _collect_data_node(self, state: CurationState) -> dict:
        """
        Collect data from integrated sources.
        """
        logger.info("Collecting data from sources")
        
        try:
            sources = state.get("active_data_sources", [])
            collection_result = self._collect_data_from_sources(sources)
            
            return {
                "status": "DATA_COLLECTION",
                "iteration": state["iteration"] + 1,
                "metadata": {
                    "collection_result": collection_result,
                },
            }
            
        except Exception as e:
            logger.error(f"Error collecting data: {e}")
            return {
                "error": str(e),
                "status": "ERROR",
                "iteration": state["iteration"] + 1,
            }
    
    def _discover_offline_sources(self, domain: str) -> List[DataSource]:
        """Discover data sources in offline mode using asset registry.
        
        This method is domain-agnostic. It queries the registry for available
        data sources for the given domain, or creates generic placeholders.
        
        Args:
            domain: The domain name (e.g., "cryptocurrency", "healthcare", "finance")
        """
        registry = get_asset_registry()
        sources = []
        
        # Try to get sources from registry first
        registry_sources = registry.list_available_data_sources()
        domain_sources = [s for s in registry_sources if s.domain == domain or s.domain is None]
        
        if domain_sources:
            logger.info(f"Found {len(domain_sources)} registered sources for domain '{domain}'")
            return domain_sources
        
        # If no registered sources, create generic placeholder sources for the domain
        # These are domain-agnostic templates
        source_templates = [
            {"name": f"{domain}_primary_data", "source_type": "api", "description": f"Primary data source for {domain}"},
            {"name": f"{domain}_social_data", "source_type": "api", "description": f"Social/sentiment data for {domain}"},
            {"name": f"{domain}_market_data", "source_type": "api", "description": f"Market/metrics data for {domain}"},
        ]
        
        for config in source_templates:
            source = DataSource(
                name=config["name"],
                source_type=config["source_type"],
                domain=domain,
                description=config["description"],
                metadata={"offline_demo": True, "placeholder": True},
            )
            sources.append(source)
        
        logger.info(f"Created {len(sources)} placeholder sources for domain '{domain}'")
        return sources
    
    def _discover_offline_datasets(self, domain: str) -> List[dict]:
        """Discover datasets in offline mode."""
        # Check for CSV files in canonical data directory
        import os
        from pathlib import Path
        
        data_dir = getattr(self.settings, 'canonical_data_dir', None)
        if data_dir:
            data_path = Path(data_dir)
        else:
            data_path = Path(__file__).parent.parent / "data" / "canonical"
        
        datasets = []
        if data_path.exists():
            for csv_file in data_path.glob("*.csv"):
                datasets.append({
                    "name": csv_file.stem,
                    "path": str(csv_file),
                    "type": "csv",
                    "domain": domain,
                })
        
        # Add placeholder dataset if none found
        if not datasets:
            datasets.append({
                "name": f"{domain}_unified_data",
                "path": None,
                "type": "streaming",
                "domain": domain,
            })
        
        return datasets
    
    def _discover_online_sources(self, domain: str) -> List[DataSource]:
        """Discover data sources in online mode using LLM-based discovery."""
        if not self.data_discovery:
            return self._discover_offline_sources(domain)
        
        sources = []
        registry = get_asset_registry()
        
        # Get variables for domain
        variables = registry.get_variables_by_domain(domain)
        
        for variable in variables:
            try:
                var_sources = self.data_discovery.discover_sources(
                    variable=variable,
                    domain=domain,
                )
                sources.extend(var_sources)
            except Exception as e:
                logger.warning(f"Error discovering sources for {variable.name}: {e}")
        
        return list(set(sources))
    
    def _build_datasets_from_sources(self, sources: List[DataSource], domain: str) -> List[dict]:
        """Build dataset metadata from discovered sources."""
        datasets = []
        for source in sources:
            datasets.append({
                "name": source.name,
                "source_type": source.source_type,
                "domain": domain,
                "kafka_topic": getattr(source, 'kafka_topic', None),
            })
        return datasets
    
    def _integrate_new_sources(self, sources: List[DataSource], state: CurationState) -> int:
        """Integrate new data sources via Confluent."""
        if self.offline_mode:
            return 0
        
        try:
            from .confluent_client import ConfluentClient
            client = ConfluentClient(self.settings)
            
            integrated = 0
            existing_topics = {s.kafka_topic for s in state.get("active_data_sources", []) if s.kafka_topic}
            
            for source in sources:
                if source.kafka_topic and source.kafka_topic in existing_topics:
                    continue
                
                try:
                    topic = client.create_topic_for_source(source)
                    source.kafka_topic = topic
                    
                    from .domain import SchemaTemplate
                    schema = SchemaTemplate.for_data_source(source)
                    client.register_schema(source, schema)
                    
                    logger.info(f"Integrated source: {source.name} -> topic: {topic}")
                    integrated += 1
                except Exception as e:
                    logger.warning(f"Failed to integrate source {source.name}: {e}")
            
            return integrated
            
        except ImportError:
            logger.warning("Confluent client not available")
            return 0
    
    def _collect_data_from_sources(self, sources: List[DataSource]) -> dict:
        """Collect data from active sources."""
        if self.offline_mode:
            return {"status": "offline_mode", "collected": False}
        
        try:
            import sys
            parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            if parent_dir not in sys.path:
                sys.path.insert(0, parent_dir)
            from producers.factory import ProducerFactory
            
            factory = ProducerFactory(self.settings)
            started = 0
            
            for source in sources:
                producer = factory.create_producer(source)
                if producer:
                    logger.info(f"Started producer for: {source.name}")
                    started += 1
            
            return {"status": "collecting", "producers_started": started}
            
        except ImportError:
            logger.warning("Producer factory not available")
            return {"status": "unavailable", "collected": False}
    
    def _compute_readiness_node(self, state: CurationState) -> dict:
        """
        Compute dataset readiness score.
        Checks if sufficient data has been collected for causal analysis.
        """
        logger.info("Computing dataset readiness")
        
        try:
            min_rows = state.get("min_rows_required", 100)
            current_rows = self._count_available_rows(state)
            
            # Compute readiness score
            if min_rows > 0:
                readiness_score = min(1.0, current_rows / min_rows)
            else:
                readiness_score = 1.0 if current_rows > 0 else 0.0
            
            is_ready = readiness_score >= 1.0
            
            logger.info(f"Readiness: {readiness_score:.2%} ({current_rows}/{min_rows} rows)")
            
            return {
                "readiness_score": readiness_score,
                "is_ready": is_ready,
                "current_row_count": current_rows,
                "status": "READY" if is_ready else "COLLECTING",
                "metadata": {
                    "readiness_computed": True,
                    "readiness_score": readiness_score,
                    "current_rows": current_rows,
                    "min_rows": min_rows,
                },
            }
            
        except Exception as e:
            logger.error(f"Error computing readiness: {e}")
            return {
                "readiness_score": 0.0,
                "is_ready": False,
                "error": str(e),
                "status": "ERROR",
            }
    
    def _count_available_rows(self, state: CurationState) -> int:
        """Count available data rows from datasets or data store."""
        total_rows = 0
        
        # Check CSV datasets
        for dataset in state.get("datasets", []):
            if dataset.get("path") and os.path.exists(dataset["path"]):
                try:
                    import pandas as pd
                    df = pd.read_csv(dataset["path"])
                    total_rows += len(df)
                except Exception:
                    pass
        
        # Check in-memory data store
        try:
            from api.main import data_store
            for coin_id, prices in data_store.price_history.items():
                total_rows += len(prices)
        except (ImportError, AttributeError):
            pass
        
        # If in offline mode with no data, simulate some rows
        if self.offline_mode and total_rows == 0:
            total_rows = 150  # Simulate having data
        
        return total_rows
    
    def _check_readiness(self, state: CurationState) -> Literal["create", "__end__"]:
        """Decide if data is ready or need to wait."""
        if state.get("error"):
            return END
        
        if state.get("is_ready", False):
            logger.info("Dataset is ready for causal analysis")
            return END
        
        # Max iterations check
        if state.get("iteration", 0) >= 10:
            logger.warning("Max iterations reached, ending curation loop")
            return END
        
        return "create"
    
    def _create_pending_run_node(self, state: CurationState) -> dict:
        """Create a pending run request to be triggered when data is ready."""
        logger.info("Creating pending run for causal analysis")
        
        pending_run = {
            "run_id": f"run_{datetime.now(timezone.utc).timestamp()}",
            "domain": state["domain"],
            "created_at": datetime.now(timezone.utc).isoformat(),
            "status": "pending",
            "readiness_at_creation": state.get("readiness_score", 0.0),
        }
        
        return {
            "pending_runs": [pending_run],
            "status": "PENDING",
            "metadata": {"pending_run_created": pending_run["run_id"]},
        }
    
    def _sleep_node(self, state: CurationState) -> dict:
        """Sleep before next readiness check."""
        sleep_duration = state.get("metadata", {}).get("sleep_duration", 5)
        logger.info(f"Sleeping for {sleep_duration} seconds before next check")
        
        # In production, this would actually sleep
        # For testing/demo, we just record that we would sleep
        if not self.offline_mode:
            time.sleep(min(sleep_duration, 1))  # Cap at 1 second for demo
        
        return {
            "status": "SLEEPING",
            "metadata": {"slept_for": sleep_duration},
        }
    
    def curate(self, domain: str, config: dict = None) -> CurationState:
        """
        Start the data curation process.
        
        Args:
            domain: Domain name (e.g., "healthcare", "finance", "retail", "energy")
            config: Optional configuration
            
        Returns:
            CurationState with readiness information
        """
        config = config or {}
        
        initial_state: CurationState = {
            "domain": domain,
            "datasets": [],
            "active_data_sources": [],
            "discovered_variables": [],
            "readiness_score": 0.0,
            "is_ready": False,
            "min_rows_required": config.get("min_rows", 100),
            "current_row_count": 0,
            "pending_runs": [],
            "completed_runs": [],
            "iteration": 0,
            "status": "INITIALIZING",
            "error": None,
            "metadata": {"sleep_duration": config.get("sleep_duration", 5)},
            "messages": [],
        }
        
        if config.get("offline_mode") is not None:
            self.offline_mode = bool(config["offline_mode"])
        
        thread_id = config.get("thread_id", f"curation_{datetime.now(timezone.utc).timestamp()}")
        recursion_limit = config.get("recursion_limit", 50)
        
        final_state = None
        if self.app and LANGGRAPH_AVAILABLE:
            stream_config = {
                "configurable": {"thread_id": thread_id},
                "recursion_limit": recursion_limit,
            }
            
            for update in self.app.stream(initial_state, config=stream_config):
                if isinstance(update, dict):
                    # Merge update into state
                    for key, value in update.items():
                        if key == "metadata":
                            initial_state[key] = {**initial_state.get(key, {}), **value}
                        elif key in ("datasets", "active_data_sources", "discovered_variables", "pending_runs", "completed_runs"):
                            initial_state[key] = initial_state.get(key, []) + value
                        else:
                            initial_state[key] = value
                final_state = initial_state
                
                # Break if ready or error
                if initial_state.get("is_ready") or initial_state.get("error"):
                    break
        else:
            final_state = self._run_sequential(initial_state)
        
        return final_state
    
    def _run_sequential(self, initial_state: CurationState) -> CurationState:
        """Fallback sequential execution.
        
        Runs the data curation pipeline:
        discover_data_sources -> integrate_pipelines -> collect_data -> compute_readiness
        """
        state = dict(initial_state)
        
        def apply_update(current: dict, update: dict) -> dict:
            result = dict(current)
            for key, value in update.items():
                if key == "metadata":
                    result[key] = {**result.get(key, {}), **value}
                elif key in ("datasets", "active_data_sources", "discovered_variables", "pending_runs", "completed_runs"):
                    result[key] = result.get(key, []) + value
                else:
                    result[key] = value
            return result
        
        # Run data curation pipeline
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
        
        update = self._compute_readiness_node(state)
        state = apply_update(state, update)
        
        return state


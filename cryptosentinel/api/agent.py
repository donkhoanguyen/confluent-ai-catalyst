"""
Agent API endpoints for causal discovery, data curation, and multi-agent orchestration.

This module provides REST API endpoints for three agent orchestrators:

1. **CausalDiscoveryAgent** (`/api/agent/discover`):
   - Generates causal hypotheses
   - Runs causal inference tests
   - Discovers and tests confounders
   - Refines hypotheses based on results

2. **DataCurationAgent** (`/api/agent/curate`):
   - Discovers data sources for a domain
   - Integrates data pipelines via Confluent
   - Collects data from sources
   - Computes dataset readiness for causal analysis

3. **MultiAgentOrchestrator** (`/api/agent/run`):
   - Runs both agents in sequence
   - Data curation -> Causal discovery (if data is ready)
   - Provides unified pipeline view

All endpoints support offline mode and can be configured via request config.
"""

from typing import List, Optional, Dict, Any
from datetime import datetime
from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect, Query
from pydantic import BaseModel, Field
from loguru import logger

from agent.orchestrator import CausalDiscoveryAgent
from agent.curation_orchestrator import DataCurationAgent
from agent.multi_agent_orchestrator import MultiAgentOrchestrator
from agent.models import (
    DiscoveryState,
    Hypothesis,
    CausalResult,
    Variable,
    DataSource,
    VariableType,
    DataSourceType,
    CausalMethod,
)
from config.settings import get_settings

router = APIRouter(prefix="/api/agent", tags=["agent"])

# Global agent instances
_causal_agent: Optional[CausalDiscoveryAgent] = None
_curation_agent: Optional[DataCurationAgent] = None
_multi_agent: Optional[MultiAgentOrchestrator] = None


def get_causal_agent() -> CausalDiscoveryAgent:
    """Get or create causal discovery agent instance."""
    global _causal_agent
    if _causal_agent is None:
        _causal_agent = CausalDiscoveryAgent(get_settings())
    return _causal_agent


def get_curation_agent() -> DataCurationAgent:
    """Get or create data curation agent instance."""
    global _curation_agent
    if _curation_agent is None:
        _curation_agent = DataCurationAgent(get_settings())
    return _curation_agent


def get_multi_agent() -> MultiAgentOrchestrator:
    """Get or create multi-agent orchestrator instance."""
    global _multi_agent
    if _multi_agent is None:
        _multi_agent = MultiAgentOrchestrator(get_settings())
    return _multi_agent


# =============================================================================
# Pydantic Request/Response Models
# =============================================================================


class DiscoveryRequest(BaseModel):
    """Request to start causal discovery."""
    domain: str
    query: str
    config: Optional[dict] = None


class CurationRequest(BaseModel):
    """Request to start data curation."""
    domain: str
    config: Optional[dict] = None


class MultiAgentRequest(BaseModel):
    """Request to run multi-agent pipeline."""
    domain: str
    query: str
    config: Optional[dict] = None


class VariableResponse(BaseModel):
    """Variable response model."""
    name: str
    description: str
    variable_type: str
    unit: Optional[str] = None
    domain: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class DataSourceResponse(BaseModel):
    """Data source response model."""
    name: str
    source_type: str
    endpoint: Optional[str] = None
    kafka_topic: Optional[str] = None
    variables: List[VariableResponse] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class HypothesisResponse(BaseModel):
    """Hypothesis response model."""
    hypothesis_id: Optional[str] = None
    cause: VariableResponse
    effect: VariableResponse
    mechanism: str
    confidence: float
    suggested_methods: List[str]
    potential_confounders: List[VariableResponse] = Field(default_factory=list)
    created_at: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class CausalResultResponse(BaseModel):
    """Causal result response model."""
    hypothesis_id: Optional[str] = None
    cause: str
    effect: str
    method: str
    is_significant: bool
    confidence: float
    p_value: Optional[float] = None
    effect_size: Optional[float] = None
    direction: Optional[str] = None
    lead_lag: Optional[int] = None
    sample_size: Optional[int] = None
    confounders_tested: List[str] = Field(default_factory=list)
    confounders_significant: List[str] = Field(default_factory=list)
    timestamp: Optional[str] = None


class DiscoveryResponse(BaseModel):
    """Response from causal discovery."""
    domain: str
    query: str
    hypotheses: List[HypothesisResponse]
    results: List[CausalResultResponse]
    active_data_sources: List[DataSourceResponse]
    discovered_variables: List[VariableResponse]
    iteration: int
    status: str
    error: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class CurationResponse(BaseModel):
    """Response from data curation."""
    domain: str
    datasets: List[Dict[str, Any]]
    active_data_sources: List[DataSourceResponse]
    discovered_variables: List[VariableResponse]
    readiness_score: float
    is_ready: bool
    min_rows_required: int
    current_row_count: int
    status: str
    error: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class MultiAgentResponse(BaseModel):
    """Response from multi-agent pipeline."""
    domain: str
    query: str
    datasets: List[Dict[str, Any]]
    readiness_score: float
    is_ready: bool
    active_data_sources: List[DataSourceResponse]
    discovered_variables: List[VariableResponse]
    hypotheses: List[HypothesisResponse]
    results: List[CausalResultResponse]
    iteration: int
    status: str
    error: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


# =============================================================================
# Helper Functions for Model Conversion
# =============================================================================


def variable_to_response(v: Variable) -> VariableResponse:
    """Convert Variable to VariableResponse."""
    return VariableResponse(
        name=v.name,
        description=v.description,
        variable_type=v.variable_type.value,
        unit=v.unit,
        domain=v.domain,
        metadata=v.metadata,
    )


def data_source_to_response(ds: DataSource) -> DataSourceResponse:
    """Convert DataSource to DataSourceResponse."""
    return DataSourceResponse(
        name=ds.name,
        source_type=ds.source_type.value if isinstance(ds.source_type, DataSourceType) else str(ds.source_type),
        endpoint=ds.endpoint,
        kafka_topic=ds.kafka_topic,
        variables=[variable_to_response(v) for v in ds.variables],
        metadata=ds.metadata,
    )


def hypothesis_to_response(h: Hypothesis) -> HypothesisResponse:
    """Convert Hypothesis to HypothesisResponse."""
    return HypothesisResponse(
        hypothesis_id=h.hypothesis_id,
        cause=variable_to_response(h.cause),
        effect=variable_to_response(h.effect),
        mechanism=h.mechanism,
        confidence=h.confidence,
        suggested_methods=[m.value if isinstance(m, CausalMethod) else str(m) for m in h.suggested_methods],
        potential_confounders=[variable_to_response(v) for v in h.potential_confounders],
        created_at=h.created_at.isoformat() if h.created_at else None,
        metadata=h.metadata,
    )


def causal_result_to_response(r: CausalResult) -> CausalResultResponse:
    """Convert CausalResult to CausalResultResponse."""
    if hasattr(r, "to_dict") and callable(getattr(r, "to_dict")):
        d = r.to_dict()
        return CausalResultResponse(**d)
    
    # Fallback manual conversion
    return CausalResultResponse(
        hypothesis_id=r.hypothesis.hypothesis_id if r.hypothesis else None,
        cause=r.hypothesis.cause.name if r.hypothesis else "unknown",
        effect=r.hypothesis.effect.name if r.hypothesis else "unknown",
        method=r.method.value if isinstance(r.method, CausalMethod) else str(r.method),
        is_significant=r.is_significant,
        confidence=r.confidence,
        p_value=r.p_value,
        effect_size=r.effect_size,
        direction=r.direction,
        lead_lag=r.lead_lag,
        sample_size=r.sample_size,
        confounders_tested=[v.name for v in r.confounders_tested],
        confounders_significant=[v.name for v in r.confounders_significant],
        timestamp=r.timestamp.isoformat() if r.timestamp else None,
    )


def discovery_state_to_response(state: DiscoveryState) -> DiscoveryResponse:
    """Convert DiscoveryState to DiscoveryResponse."""
    return DiscoveryResponse(
        domain=state.domain,
        query=state.query,
        hypotheses=[hypothesis_to_response(h) for h in state.hypotheses],
        results=[causal_result_to_response(r) for r in state.results],
        active_data_sources=[data_source_to_response(ds) for ds in state.active_data_sources],
        discovered_variables=[variable_to_response(v) for v in state.discovered_variables],
        iteration=state.iteration,
        status=state.status,
        error=state.error,
        metadata=state.metadata,
    )


# =============================================================================
# Causal Discovery Endpoints
# =============================================================================


@router.post("/discover", response_model=DiscoveryResponse)
async def start_discovery(request: DiscoveryRequest):
    """
    Start a causal discovery process.
    
    This endpoint runs the CausalDiscoveryAgent which:
    - Generates hypotheses
    - Runs causal inference tests
    - Discovers and tests confounders
    - Refines hypotheses based on results
    
    Args:
        request: Discovery request with domain and query
        
    Returns:
        Discovery state and results
    """
    try:
        agent = get_causal_agent()
        state = agent.discover(
            domain=request.domain,
            query=request.query,
            config=request.config or {},
        )
        
        return discovery_state_to_response(state)
    except Exception as e:
        logger.error(f"Discovery error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/discover/status/{domain}")
async def get_discovery_status(domain: str):
    """Get status of active discovery processes for a domain."""
    # In a real implementation, would track active discoveries
    return {"domain": domain, "active_discoveries": []}


# =============================================================================
# Data Curation Endpoints
# =============================================================================


@router.post("/curate", response_model=CurationResponse)
async def start_curation(request: CurationRequest):
    """
    Start a data curation process.
    
    This endpoint runs the DataCurationAgent which:
    - Discovers data sources for the domain
    - Integrates data pipelines via Confluent
    - Collects data from sources
    - Computes dataset readiness for causal analysis
    
    Args:
        request: Curation request with domain
        
    Returns:
        Curation state with readiness information
    """
    try:
        agent = get_curation_agent()
        state = agent.curate(
            domain=request.domain,
            config=request.config or {},
        )
        
        return CurationResponse(
            domain=state.get("domain", request.domain),
            datasets=state.get("datasets", []),
            active_data_sources=[data_source_to_response(ds) for ds in state.get("active_data_sources", [])],
            discovered_variables=[variable_to_response(v) for v in state.get("discovered_variables", [])],
            readiness_score=state.get("readiness_score", 0.0),
            is_ready=state.get("is_ready", False),
            min_rows_required=state.get("min_rows_required", 100),
            current_row_count=state.get("current_row_count", 0),
            status=state.get("status", "UNKNOWN"),
            error=state.get("error"),
            metadata=state.get("metadata", {}),
        )
    except Exception as e:
        logger.error(f"Curation error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/curate/status/{domain}")
async def get_curation_status(domain: str):
    """Get status of active curation processes for a domain."""
    # In a real implementation, would track active curations
    return {"domain": domain, "active_curations": []}


# =============================================================================
# Multi-Agent Orchestration Endpoints
# =============================================================================


@router.post("/run", response_model=MultiAgentResponse)
async def run_multi_agent(request: MultiAgentRequest):
    """
    Run the complete multi-agent pipeline.
    
    This endpoint runs both DataCurationAgent and CausalDiscoveryAgent in sequence:
    1. Data Curation: Discovers and prepares data
    2. Causal Discovery: Generates hypotheses and runs causal tests (if data is ready)
    
    Args:
        request: Multi-agent request with domain and query
        
    Returns:
        Complete pipeline state with curation and causal results
    """
    try:
        orchestrator = get_multi_agent()
        state = orchestrator.run(
            domain=request.domain,
            query=request.query,
            config=request.config or {},
        )
        
        # Convert MultiAgentState to response
        return MultiAgentResponse(
            domain=state.get("domain", request.domain),
            query=state.get("query", request.query),
            datasets=state.get("datasets", []),
            readiness_score=state.get("readiness_score", 0.0),
            is_ready=state.get("is_ready", False),
            active_data_sources=[data_source_to_response(ds) for ds in state.get("active_data_sources", [])],
            discovered_variables=[variable_to_response(v) for v in state.get("discovered_variables", [])],
            hypotheses=[hypothesis_to_response(h) for h in state.get("hypotheses", [])],
            results=[causal_result_to_response(r) for r in state.get("results", [])],
            iteration=state.get("iteration", 0),
            status=state.get("status", "UNKNOWN"),
            error=state.get("error"),
            metadata=state.get("metadata", {}),
        )
    except Exception as e:
        logger.error(f"Multi-agent error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Legacy Endpoints (for backward compatibility)
# =============================================================================


@router.get("/hypotheses")
async def list_hypotheses(domain: Optional[str] = None):
    """List active hypotheses (legacy endpoint)."""
    # In real implementation, would track active discoveries
    return {"hypotheses": []}


@router.get("/results")
async def get_results(domain: Optional[str] = None):
    """Get causal inference results (legacy endpoint)."""
    # In real implementation, would return results from active discoveries
    return {"results": []}


@router.post("/refine")
async def refine_hypotheses(domain: str, query: Optional[str] = None):
    """Trigger hypothesis refinement (legacy endpoint)."""
    # In real implementation, would refine existing hypotheses
    return {"status": "refined"}


# =============================================================================
# WebSocket Endpoints
# =============================================================================


@router.websocket("/stream")
async def stream_updates(websocket: WebSocket):
    """WebSocket endpoint for real-time discovery updates."""
    await websocket.accept()
    
    try:
        while True:
            # In real implementation, would stream state updates
            await websocket.send_json({"status": "connected"})
            
            import asyncio
            await asyncio.sleep(1)
            
    except WebSocketDisconnect:
        logger.info("WebSocket disconnected")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")

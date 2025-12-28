"""
Agent API endpoints for causal discovery.
"""

from typing import List, Optional
from datetime import datetime
from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
from loguru import logger

from agent.orchestrator import CausalDiscoveryAgent
from agent.models import DiscoveryState, Hypothesis, CausalResult
from config.settings import get_settings

router = APIRouter(prefix="/api/agent", tags=["agent"])

# Global agent instance
_agent: Optional[CausalDiscoveryAgent] = None


def get_agent() -> CausalDiscoveryAgent:
    """Get or create agent instance."""
    global _agent
    if _agent is None:
        _agent = CausalDiscoveryAgent(get_settings())
    return _agent


class DiscoveryRequest(BaseModel):
    """Request to start discovery."""
    domain: str
    query: str
    config: Optional[dict] = None


class DiscoveryResponse(BaseModel):
    """Response from discovery."""
    state: dict
    hypotheses: List[dict]
    results: List[dict]
    status: str


@router.post("/discover", response_model=DiscoveryResponse)
async def start_discovery(request: DiscoveryRequest):
    """
    Start a causal discovery process.
    
    Args:
        request: Discovery request with domain and query
        
    Returns:
        Discovery state and results
    """
    try:
        agent = get_agent()
        state = agent.discover(
            domain=request.domain,
            query=request.query,
            config=request.config or {},
        )
        
        return DiscoveryResponse(
            state=state.__dict__,
            hypotheses=[h.__dict__ for h in state.hypotheses],
            results=[r.to_dict() for r in state.results],
            status=state.status,
        )
    except Exception as e:
        logger.error(f"Discovery error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/hypotheses")
async def list_hypotheses(domain: Optional[str] = None):
    """List active hypotheses."""
    agent = get_agent()
    # In real implementation, would track active discoveries
    return {"hypotheses": []}


@router.get("/results")
async def get_results(domain: Optional[str] = None):
    """Get causal inference results."""
    agent = get_agent()
    # In real implementation, would return results from active discoveries
    return {"results": []}


@router.post("/refine")
async def refine_hypotheses(domain: str, query: Optional[str] = None):
    """Trigger hypothesis refinement."""
    agent = get_agent()
    # In real implementation, would refine existing hypotheses
    return {"status": "refined"}


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


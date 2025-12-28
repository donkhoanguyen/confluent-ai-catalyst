"""
LangGraph entrypoint for the causal discovery agent.
This exposes the compiled LangGraph app so `langgraph dev` can load it.
"""

from agent.orchestrator import CausalDiscoveryAgent


agent = CausalDiscoveryAgent()
if not agent.app:
    raise RuntimeError(
        "LangGraph app not available. Ensure `langgraph` is installed and "
        "Agent modules are importable."
    )

# Exported symbol used by langgraph.json
graph_app = agent.app
__all__ = ["graph_app", "agent"]


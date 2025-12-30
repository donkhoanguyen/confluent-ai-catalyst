"""
LangGraph entrypoint for the data curation agent.
This exposes the compiled LangGraph app so `langgraph dev` can load it.
"""

import os
from dotenv import load_dotenv

# Load .env file to ensure environment variables are available
load_dotenv(override=True)

# Ensure LANGCHAIN_API_KEY is set if LANGSMITH_API_KEY exists
if os.getenv("LANGSMITH_API_KEY") and not os.getenv("LANGCHAIN_API_KEY"):
    os.environ["LANGCHAIN_API_KEY"] = os.getenv("LANGSMITH_API_KEY")

from agent.curation_orchestrator import DataCurationAgent


agent = DataCurationAgent()
if not agent.app:
    raise RuntimeError(
        "LangGraph app not available. Ensure `langgraph` is installed and "
        "Agent modules are importable."
    )

# Exported symbol used by langgraph.json
graph_app = agent.app

# Validate graph is properly compiled
if hasattr(graph_app, 'get_graph'):
    try:
        graph_app.get_graph()
    except Exception as e:
        raise RuntimeError(f"Graph validation failed: {e}")

__all__ = ["graph_app", "agent"]


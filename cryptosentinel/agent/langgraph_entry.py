"""
LangGraph entrypoint for the causal discovery agent.
This exposes the compiled LangGraph app so `langgraph dev` can load it.
"""

import os
from dotenv import load_dotenv

# Load .env file to ensure LangSmith environment variables are available
load_dotenv(override=True)

# Ensure LANGCHAIN_API_KEY is set if LANGSMITH_API_KEY exists but LANGCHAIN_API_KEY doesn't
if os.getenv("LANGSMITH_API_KEY") and not os.getenv("LANGCHAIN_API_KEY"):
    os.environ["LANGCHAIN_API_KEY"] = os.getenv("LANGSMITH_API_KEY")

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


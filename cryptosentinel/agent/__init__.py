"""Autonomous Causal Discovery Agent Module."""

from .models import (
    Hypothesis,
    Variable,
    DataSource,
    DataSourceType,
    CausalResult,
    DiscoveryState,
)
from .domain import (
    DataSource as DataSourceBase,
    Variable as VariableBase,
    DomainRegistry,
)
from .orchestrator import CausalDiscoveryAgent
from .confluent_client import ConfluentClient
from .curation_orchestrator import DataCurationAgent

# Optional imports (may not be available in all environments)
try:
    from .mcp_client import (
        ConfluentMCPClient,
        ConnectorConfig,
        ConnectorType,
        AuthType,
    )
    MCP_AVAILABLE = True
except ImportError:
    MCP_AVAILABLE = False

try:
    from .connector_generator import ConnectorGenerator
    CONNECTOR_GENERATOR_AVAILABLE = True
except ImportError:
    CONNECTOR_GENERATOR_AVAILABLE = False

__all__ = [
    # Models
    "Hypothesis",
    "Variable",
    "DataSource",
    "DataSourceType",
    "CausalResult",
    "DiscoveryState",
    # Domain
    "DataSourceBase",
    "VariableBase",
    "DomainRegistry",
    # Agents
    "CausalDiscoveryAgent",
    "DataCurationAgent",
    # Confluent Integration
    "ConfluentClient",
    # MCP (optional)
    "ConfluentMCPClient",
    "ConnectorConfig",
    "ConnectorType",
    "AuthType",
    "ConnectorGenerator",
    # Feature flags
    "MCP_AVAILABLE",
    "CONNECTOR_GENERATOR_AVAILABLE",
]


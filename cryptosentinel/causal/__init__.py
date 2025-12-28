"""Causal inference module for CryptoSentinel."""

from .granger import (
    compute_granger_causality,
    bidirectional_granger_test,
    rolling_causal_analysis,
    generate_causal_alerts,
)

# Import new engines
try:
    from .registry import CausalEngineRegistry
    from .base import CausalEngine
    from .granger_engine import GrangerCausalityEngine
    from .transfer_entropy import TransferEntropyEngine
    from .psm import PropensityScoreMatchingEngine
    from .iv import InstrumentalVariablesEngine
    from .pc_algorithm import PCAlgorithmEngine
    from .confounder_test import ConfounderTester
except ImportError:
    # Modules may not be fully available
    pass

__all__ = [
    "compute_granger_causality",
    "bidirectional_granger_test",
    "rolling_causal_analysis",
    "generate_causal_alerts",
    "CausalEngineRegistry",
    "CausalEngine",
    "GrangerCausalityEngine",
    "TransferEntropyEngine",
    "PropensityScoreMatchingEngine",
    "InstrumentalVariablesEngine",
    "PCAlgorithmEngine",
    "ConfounderTester",
]


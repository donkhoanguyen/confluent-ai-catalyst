"""
Registry for causal inference engines.
"""

from typing import Dict, Optional
from loguru import logger

from .base import CausalEngine
from .granger_engine import GrangerCausalityEngine
from .transfer_entropy import TransferEntropyEngine
from .psm import PropensityScoreMatchingEngine
from .iv import InstrumentalVariablesEngine
from .pc_algorithm import PCAlgorithmEngine
from agent.models import CausalMethod


class CausalEngineRegistry:
    """Registry for managing causal inference engines."""
    
    def __init__(self):
        self._engines: Dict[CausalMethod, CausalEngine] = {}
        self._register_default_engines()
    
    def _register_default_engines(self):
        """Register default causal engines."""
        try:
            self.register(CausalMethod.GRANGER, GrangerCausalityEngine())
        except Exception as e:
            logger.warning(f"Failed to register Granger engine: {e}")
        
        try:
            self.register(CausalMethod.TRANSFER_ENTROPY, TransferEntropyEngine())
        except Exception as e:
            logger.warning(f"Failed to register Transfer Entropy engine: {e}")
        
        try:
            self.register(CausalMethod.PROPENSITY_SCORE_MATCHING, PropensityScoreMatchingEngine())
        except Exception as e:
            logger.warning(f"Failed to register PSM engine: {e}")
        
        try:
            self.register(CausalMethod.INSTRUMENTAL_VARIABLES, InstrumentalVariablesEngine())
        except Exception as e:
            logger.warning(f"Failed to register IV engine: {e}")
        
        try:
            self.register(CausalMethod.PC_ALGORITHM, PCAlgorithmEngine())
        except Exception as e:
            logger.warning(f"Failed to register PC Algorithm engine: {e}")
    
    def register(self, method: CausalMethod, engine: CausalEngine):
        """Register a causal engine."""
        self._engines[method] = engine
        logger.info(f"Registered causal engine: {method.value}")
    
    def get_engine(self, method: CausalMethod) -> Optional[CausalEngine]:
        """Get a causal engine by method."""
        return self._engines.get(method)
    
    def list_available_methods(self) -> list[CausalMethod]:
        """List all available causal methods."""
        return list(self._engines.keys())
    
    def get_engine_for_data(self, data_characteristics: dict) -> Optional[CausalEngine]:
        """
        Select appropriate engine based on data characteristics.
        
        Args:
            data_characteristics: Dict with data properties (time_series, min_samples, etc.)
            
        Returns:
            Recommended engine or None
        """
        # Simple heuristic-based selection
        if data_characteristics.get("time_series", False):
            # Prefer Granger or Transfer Entropy for time series
            if CausalMethod.GRANGER in self._engines:
                return self._engines[CausalMethod.GRANGER]
            elif CausalMethod.TRANSFER_ENTROPY in self._engines:
                return self._engines[CausalMethod.TRANSFER_ENTROPY]
        
        if data_characteristics.get("binary_treatment", False):
            # Prefer PSM for binary treatments
            if CausalMethod.PROPENSITY_SCORE_MATCHING in self._engines:
                return self._engines[CausalMethod.PROPENSITY_SCORE_MATCHING]
        
        if data_characteristics.get("instruments_available", False):
            # Prefer IV if instruments available
            if CausalMethod.INSTRUMENTAL_VARIABLES in self._engines:
                return self._engines[CausalMethod.INSTRUMENTAL_VARIABLES]
        
        # Default to PC Algorithm for general case
        if CausalMethod.PC_ALGORITHM in self._engines:
            return self._engines[CausalMethod.PC_ALGORITHM]
        
        # Fallback to first available
        if self._engines:
            return list(self._engines.values())[0]
        
        return None


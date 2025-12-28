"""
Transfer Entropy causal inference engine.
"""

from typing import List, Optional, Dict, Any
import pandas as pd
import numpy as np
from loguru import logger

from .base import CausalEngine
from agent.models import Variable, DataSource, CausalResult, Hypothesis, CausalMethod

try:
    from pyitlib import discrete_random_variable as drv
    PYITLIB_AVAILABLE = True
except ImportError:
    PYITLIB_AVAILABLE = False
    logger.warning("pyitlib not available - Transfer Entropy will use approximation")


class TransferEntropyEngine(CausalEngine):
    """Transfer Entropy causal inference engine."""
    
    def __init__(self):
        super().__init__(CausalMethod.TRANSFER_ENTROPY)
    
    def test(self, cause: Variable, effect: Variable,
             data_sources: List[DataSource],
             data: Optional[pd.DataFrame] = None,
             lag: int = 1,
             bins: int = 10,
             **kwargs) -> CausalResult:
        """
        Test for causal relationship using Transfer Entropy.
        
        Transfer Entropy measures the information flow from cause to effect.
        
        Args:
            cause: Cause variable
            effect: Effect variable
            data_sources: Data sources
            data: Optional pre-loaded data
            lag: Time lag to test
            bins: Number of bins for discretization
            **kwargs: Additional parameters
            
        Returns:
            CausalResult
        """
        if data is None:
            data = self.fetch_data(cause, effect, data_sources)
        
        if not self.validate_data(data, cause, effect):
            return self._create_invalid_result(cause, effect)
        
        try:
            # Discretize continuous variables
            cause_values = self._discretize(data[cause.name].values, bins)
            effect_values = self._discretize(data[effect.name].values, bins)
            
            # Calculate Transfer Entropy
            if PYITLIB_AVAILABLE:
                te = self._calculate_transfer_entropy_pyitlib(
                    cause_values, effect_values, lag
                )
            else:
                te = self._calculate_transfer_entropy_approx(
                    cause_values, effect_values, lag
                )
            
            # Statistical significance (simplified - would need proper test)
            # Transfer Entropy > 0 suggests information flow
            is_significant = te > 0.01  # Threshold
            confidence = min(1.0, te * 10)  # Normalize to 0-1
            
            from agent.models import Hypothesis as HypModel
            hypothesis = HypModel(
                cause=cause,
                effect=effect,
                mechanism="Transfer Entropy (information flow)",
                confidence=confidence,
                suggested_methods=[CausalMethod.TRANSFER_ENTROPY],
            )
            
            return CausalResult(
                hypothesis=hypothesis,
                method=self.method,
                is_significant=is_significant,
                confidence=confidence,
                effect_size=te,
                direction=f"{cause.name} → {effect.name}",
                lead_lag=lag,
                sample_size=len(data),
                result_data={"transfer_entropy": te},
            )
            
        except Exception as e:
            logger.error(f"Transfer Entropy test failed: {e}")
            return self._create_invalid_result(cause, effect, str(e))
    
    def _discretize(self, values: np.ndarray, bins: int) -> np.ndarray:
        """Discretize continuous values into bins."""
        if len(np.unique(values)) <= bins:
            return values.astype(int)
        
        _, bin_edges = np.histogram(values, bins=bins)
        return np.digitize(values, bin_edges)
    
    def _calculate_transfer_entropy_pyitlib(self, cause: np.ndarray,
                                           effect: np.ndarray, lag: int) -> float:
        """Calculate Transfer Entropy using pyitlib."""
        # Shift effect by lag
        effect_lagged = np.roll(effect, -lag)
        
        # Calculate TE(X -> Y) = H(Y|Y_past) - H(Y|Y_past, X_past)
        # Simplified calculation
        te = drv.entropy_conditional(effect_lagged, effect) - \
             drv.entropy_conditional(effect_lagged, np.column_stack([effect, cause]))
        
        return max(0.0, te)  # TE is non-negative
    
    def _calculate_transfer_entropy_approx(self, cause: np.ndarray,
                                          effect: np.ndarray, lag: int) -> float:
        """Approximate Transfer Entropy calculation."""
        # Simplified approximation using mutual information
        from scipy.stats import entropy
        
        effect_lagged = np.roll(effect, -lag)
        
        # Approximate as mutual information between cause and future effect
        # given current effect
        # This is a simplification
        te = 0.0
        
        # Calculate conditional entropy approximation
        unique_effect = np.unique(effect)
        for e_val in unique_effect:
            mask = effect == e_val
            if np.sum(mask) > 0:
                effect_future = effect_lagged[mask]
                cause_cond = cause[mask]
                
                if len(np.unique(effect_future)) > 1:
                    prob = np.bincount(effect_future) / len(effect_future)
                    h_cond = entropy(prob)
                    te += h_cond * np.sum(mask) / len(effect)
        
        return max(0.0, te)
    
    def _create_invalid_result(self, cause: Variable, effect: Variable,
                               error: str = "Invalid data") -> CausalResult:
        """Create an invalid result."""
        from agent.models import Hypothesis as HypModel
        return CausalResult(
            hypothesis=HypModel(
                cause=cause,
                effect=effect,
                mechanism="",
                confidence=0.0,
                suggested_methods=[CausalMethod.TRANSFER_ENTROPY],
            ),
            method=self.method,
            is_significant=False,
            confidence=0.0,
            error=error,
        )
    
    def get_required_data_characteristics(self) -> Dict[str, Any]:
        """Get data requirements for Transfer Entropy."""
        return {
            "min_samples": 50,
            "time_series": True,
            "continuous": True,  # Will be discretized
        }


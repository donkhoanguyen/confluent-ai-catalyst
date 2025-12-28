"""
PC Algorithm for constraint-based causal discovery.
"""

from typing import List, Optional, Dict, Any
import pandas as pd
import numpy as np
from loguru import logger

from .base import CausalEngine
from agent.models import Variable, DataSource, CausalResult, Hypothesis, CausalMethod

try:
    from causallearn.search.ConstraintBased.PC import pc
    from causallearn.utils.GraphUtils import GraphUtils
    CAUSALLEARN_AVAILABLE = True
except ImportError:
    CAUSALLEARN_AVAILABLE = False
    logger.warning("causal-learn not available - PC Algorithm will use simplified version")


class PCAlgorithmEngine(CausalEngine):
    """PC Algorithm causal discovery engine."""
    
    def __init__(self):
        super().__init__(CausalMethod.PC_ALGORITHM)
    
    def test(self, cause: Variable, effect: Variable,
             data_sources: List[DataSource],
             data: Optional[pd.DataFrame] = None,
             variables: Optional[List[Variable]] = None,
             **kwargs) -> CausalResult:
        """
        Test for causal relationship using PC Algorithm.
        
        PC Algorithm discovers causal structure from observational data.
        
        Args:
            cause: Cause variable
            effect: Effect variable
            data_sources: Data sources
            data: Optional pre-loaded data
            variables: All variables to include in graph
            **kwargs: Additional parameters
            
        Returns:
            CausalResult
        """
        if data is None:
            data = self.fetch_data(cause, effect, data_sources)
        
        if not self.validate_data(data, cause, effect):
            return self._create_invalid_result(cause, effect)
        
        try:
            # Include all variables if provided
            if variables:
                var_names = [v.name for v in variables if v.name in data.columns]
                if len(var_names) < 2:
                    var_names = [cause.name, effect.name]
            else:
                var_names = [cause.name, effect.name]
            
            # Prepare data matrix
            data_matrix = data[var_names].values
            
            if CAUSALLEARN_AVAILABLE:
                # Run PC algorithm
                cg = pc(data_matrix, alpha=0.05)
                
                # Check if cause -> effect edge exists
                graph = cg.G.graph
                cause_idx = var_names.index(cause.name)
                effect_idx = var_names.index(effect.name)
                
                has_edge = graph[cause_idx, effect_idx] != 0
                edge_type = graph[cause_idx, effect_idx]  # -1: ->, 1: <-, 2: <->, 0: no edge
                
                is_significant = has_edge and edge_type == -1  # Directed edge cause -> effect
                confidence = 0.8 if is_significant else 0.2
                
            else:
                # Simplified version using correlation and partial correlation
                is_significant, confidence = self._simplified_pc_test(
                    data, cause, effect, var_names
                )
            
            from agent.models import Hypothesis as HypModel
            hypothesis = HypModel(
                cause=cause,
                effect=effect,
                mechanism="PC Algorithm (constraint-based)",
                confidence=confidence,
                suggested_methods=[CausalMethod.PC_ALGORITHM],
            )
            
            return CausalResult(
                hypothesis=hypothesis,
                method=self.method,
                is_significant=is_significant,
                confidence=confidence,
                direction=f"{cause.name} → {effect.name}" if is_significant else "No edge",
                sample_size=len(data),
                result_data={"variables_tested": var_names},
            )
            
        except Exception as e:
            logger.error(f"PC Algorithm test failed: {e}")
            return self._create_invalid_result(cause, effect, str(e))
    
    def _simplified_pc_test(self, data: pd.DataFrame, cause: Variable,
                           effect: Variable, variables: List[str]) -> tuple[bool, float]:
        """Simplified PC test using partial correlation."""
        from scipy.stats import pearsonr
        from scipy.stats import linregress
        
        # Calculate partial correlation (controlling for other variables)
        if len(variables) > 2:
            # Control for other variables
            control_vars = [v for v in variables if v not in [cause.name, effect.name]]
            
            # Residualize cause and effect on control variables
            from sklearn.linear_model import LinearRegression
            
            if control_vars:
                control_data = data[control_vars].values
                
                # Residualize cause
                model_cause = LinearRegression()
                model_cause.fit(control_data, data[cause.name].values)
                cause_residual = data[cause.name].values - model_cause.predict(control_data)
                
                # Residualize effect
                model_effect = LinearRegression()
                model_effect.fit(control_data, data[effect.name].values)
                effect_residual = data[effect.name].values - model_effect.predict(control_data)
                
                # Test correlation of residuals
                corr, p_value = pearsonr(cause_residual, effect_residual)
            else:
                corr, p_value = pearsonr(data[cause.name].values, data[effect.name].values)
        else:
            corr, p_value = pearsonr(data[cause.name].values, data[effect.name].values)
        
        is_significant = p_value < 0.05 and corr > 0
        confidence = abs(corr) if is_significant else 0.0
        
        return is_significant, confidence
    
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
                suggested_methods=[CausalMethod.PC_ALGORITHM],
            ),
            method=self.method,
            is_significant=False,
            confidence=0.0,
            error=error,
        )
    
    def get_required_data_characteristics(self) -> Dict[str, Any]:
        """Get data requirements for PC Algorithm."""
        return {
            "min_samples": 50,
            "time_series": False,
            "multiple_variables": True,  # Works better with more variables
        }


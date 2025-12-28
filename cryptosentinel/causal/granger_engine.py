"""
Granger Causality engine implementing the CausalEngine interface.
"""

from typing import List, Optional, Dict, Any
import pandas as pd
from loguru import logger
from statsmodels.tsa.stattools import grangercausalitytests, adfuller

from .base import CausalEngine
from agent.models import Variable, DataSource, CausalResult, Hypothesis, CausalMethod
from .granger import check_stationarity, make_stationary, compute_granger_causality


class GrangerCausalityEngine(CausalEngine):
    """Granger Causality causal inference engine."""
    
    def __init__(self):
        super().__init__(CausalMethod.GRANGER)
    
    def test(self, cause: Variable, effect: Variable,
             data_sources: List[DataSource],
             data: Optional[pd.DataFrame] = None,
             max_lag: int = 10,
             significance: float = 0.05,
             **kwargs) -> CausalResult:
        """
        Test for Granger causality between cause and effect.
        
        Args:
            cause: Cause variable
            effect: Effect variable
            data_sources: Data sources
            data: Optional pre-loaded data
            max_lag: Maximum lag to test
            significance: Significance level
            **kwargs: Additional parameters
            
        Returns:
            CausalResult
        """
        # Fetch data if not provided
        if data is None:
            data = self.fetch_data(cause, effect, data_sources)
        
        # Validate data
        if not self.validate_data(data, cause, effect):
            from agent.models import Hypothesis as HypModel
            return CausalResult(
                hypothesis=HypModel(
                    cause=cause,
                    effect=effect,
                    mechanism="",
                    confidence=0.0,
                    suggested_methods=[CausalMethod.GRANGER],
                ),
                method=self.method,
                is_significant=False,
                confidence=0.0,
                error="Invalid data",
            )
        
        # Make data stationary if needed
        cause_series = make_stationary(data[cause.name])
        effect_series = make_stationary(data[effect.name])
        
        # Create DataFrame with stationary series
        df = pd.DataFrame({
            cause.name: cause_series,
            effect.name: effect_series,
        }).dropna()
        
        # Run Granger test
        granger_result = compute_granger_causality(
            df=df,
            x_col=cause.name,
            y_col=effect.name,
            max_lag=max_lag,
            significance=significance,
        )
        
        # Create hypothesis for result
        from agent.models import Hypothesis as HypModel
        hypothesis = HypModel(
            cause=cause,
            effect=effect,
            mechanism=f"Granger causality test",
            confidence=1.0 - granger_result.p_value,
            suggested_methods=[CausalMethod.GRANGER],
        )
        
        # Create CausalResult
        result = CausalResult(
            hypothesis=hypothesis,
            method=self.method,
            is_significant=granger_result.is_significant,
            confidence=1.0 - granger_result.p_value,
            p_value=granger_result.p_value,
            effect_size=granger_result.f_statistic,
            direction=f"{cause.name} → {effect.name}",
            lead_lag=granger_result.optimal_lag,
            sample_size=len(df),
            result_data={
                "f_statistic": granger_result.f_statistic,
                "optimal_lag": granger_result.optimal_lag,
            },
        )
        
        return result
    
    def get_required_data_characteristics(self) -> Dict[str, Any]:
        """Get data requirements for Granger causality."""
        return {
            "min_samples": 30,
            "time_series": True,
            "stationary": False,  # Will be made stationary
            "continuous": True,
        }


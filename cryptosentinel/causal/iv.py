"""
Instrumental Variables causal inference engine.
"""

from typing import List, Optional, Dict, Any
import pandas as pd
import numpy as np
from loguru import logger

from .base import CausalEngine
from agent.models import Variable, DataSource, CausalResult, Hypothesis, CausalMethod

try:
    from linearmodels.iv import IV2SLS
    LINEARMODELS_AVAILABLE = True
except ImportError:
    LINEARMODELS_AVAILABLE = False
    logger.warning("linearmodels not available - IV will use statsmodels")


class InstrumentalVariablesEngine(CausalEngine):
    """Instrumental Variables causal inference engine."""
    
    def __init__(self):
        super().__init__(CausalMethod.INSTRUMENTAL_VARIABLES)
    
    def test(self, cause: Variable, effect: Variable,
             data_sources: List[DataSource],
             data: Optional[pd.DataFrame] = None,
             instruments: Optional[List[Variable]] = None,
             **kwargs) -> CausalResult:
        """
        Test for causal relationship using Instrumental Variables.
        
        Args:
            cause: Cause variable (endogenous)
            effect: Effect variable
            data_sources: Data sources
            data: Optional pre-loaded data
            instruments: Instrumental variables
            **kwargs: Additional parameters
            
        Returns:
            CausalResult
        """
        if data is None:
            data = self.fetch_data(cause, effect, data_sources)
        
        if not self.validate_data(data, cause, effect):
            return self._create_invalid_result(cause, effect)
        
        try:
            # If no instruments provided, we can't do IV
            if not instruments or len(instruments) == 0:
                logger.warning("No instruments provided for IV estimation")
                return self._create_invalid_result(cause, effect, "No instruments")
            
            # Get instrument data
            instrument_data = data[[i.name for i in instruments if i.name in data.columns]]
            
            if instrument_data.empty:
                return self._create_invalid_result(cause, effect, "Instruments not in data")
            
            # Run IV regression
            if LINEARMODELS_AVAILABLE:
                result = self._estimate_iv_linearmodels(
                    data, cause, effect, instrument_data
                )
            else:
                result = self._estimate_iv_statsmodels(
                    data, cause, effect, instrument_data
                )
            
            from agent.models import Hypothesis as HypModel
            hypothesis = HypModel(
                cause=cause,
                effect=effect,
                mechanism="Instrumental Variables",
                confidence=1.0 - result.get("p_value", 0.5),
                suggested_methods=[CausalMethod.INSTRUMENTAL_VARIABLES],
            )
            
            return CausalResult(
                hypothesis=hypothesis,
                method=self.method,
                is_significant=result.get("is_significant", False),
                confidence=1.0 - result.get("p_value", 0.5),
                p_value=result.get("p_value"),
                effect_size=result.get("coefficient"),
                direction=f"{cause.name} → {effect.name}",
                sample_size=len(data),
                result_data=result,
            )
            
        except Exception as e:
            logger.error(f"IV test failed: {e}")
            return self._create_invalid_result(cause, effect, str(e))
    
    def _estimate_iv_linearmodels(self, data: pd.DataFrame, cause: Variable,
                                  effect: Variable,
                                  instruments: pd.DataFrame) -> Dict[str, Any]:
        """Estimate IV using linearmodels."""
        from linearmodels.iv import IV2SLS
        
        # Formula: effect ~ [cause ~ instruments]
        formula = f"{effect.name} ~ 1 + [{cause.name} ~ {'+'.join(instruments.columns)}]"
        
        mod = IV2SLS.from_formula(formula, data)
        res = mod.fit()
        
        return {
            "coefficient": res.params[cause.name],
            "p_value": res.pvalues[cause.name],
            "is_significant": res.pvalues[cause.name] < 0.05,
            "r_squared": res.rsquared,
        }
    
    def _estimate_iv_statsmodels(self, data: pd.DataFrame, cause: Variable,
                                 effect: Variable,
                                 instruments: pd.DataFrame) -> Dict[str, Any]:
        """Estimate IV using statsmodels (2SLS)."""
        from statsmodels.sandbox.regression.gmm import IV2SLS
        
        # First stage: regress cause on instruments
        from sklearn.linear_model import LinearRegression
        
        first_stage = LinearRegression()
        first_stage.fit(instruments.values, data[cause.name].values)
        cause_predicted = first_stage.predict(instruments.values)
        
        # Second stage: regress effect on predicted cause
        second_stage = LinearRegression()
        second_stage.fit(cause_predicted.reshape(-1, 1), data[effect.name].values)
        
        coefficient = second_stage.coef_[0]
        
        # Calculate p-value (simplified)
        residuals = data[effect.name].values - second_stage.predict(cause_predicted.reshape(-1, 1))
        mse = np.mean(residuals ** 2)
        se = np.sqrt(mse / len(data))
        t_stat = coefficient / se
        from scipy import stats
        p_value = 2 * (1 - stats.t.cdf(abs(t_stat), len(data) - 2))
        
        return {
            "coefficient": coefficient,
            "p_value": p_value,
            "is_significant": p_value < 0.05,
            "r_squared": 1 - mse / np.var(data[effect.name].values),
        }
    
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
                suggested_methods=[CausalMethod.INSTRUMENTAL_VARIABLES],
            ),
            method=self.method,
            is_significant=False,
            confidence=0.0,
            error=error,
        )
    
    def get_required_data_characteristics(self) -> Dict[str, Any]:
        """Get data requirements for IV."""
        return {
            "min_samples": 30,
            "time_series": False,
            "instruments_required": True,
        }


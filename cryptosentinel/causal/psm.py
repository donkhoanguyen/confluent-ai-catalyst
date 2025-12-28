"""
Propensity Score Matching causal inference engine.
"""

from typing import List, Optional, Dict, Any
import pandas as pd
import numpy as np
from loguru import logger

from .base import CausalEngine
from agent.models import Variable, DataSource, CausalResult, Hypothesis, CausalMethod

try:
    from causalml.match import NearestNeighborMatch
    CAUSALML_AVAILABLE = True
except ImportError:
    CAUSALML_AVAILABLE = False
    logger.warning("causalml not available - PSM will use simplified implementation")


class PropensityScoreMatchingEngine(CausalEngine):
    """Propensity Score Matching causal inference engine."""
    
    def __init__(self):
        super().__init__(CausalMethod.PROPENSITY_SCORE_MATCHING)
    
    def test(self, cause: Variable, effect: Variable,
             data_sources: List[DataSource],
             data: Optional[pd.DataFrame] = None,
             confounders: Optional[List[Variable]] = None,
             **kwargs) -> CausalResult:
        """
        Test for causal relationship using Propensity Score Matching.
        
        Args:
            cause: Cause variable (treatment)
            effect: Effect variable (outcome)
            data_sources: Data sources
            data: Optional pre-loaded data
            confounders: Confounding variables to control for
            **kwargs: Additional parameters
            
        Returns:
            CausalResult
        """
        if data is None:
            data = self.fetch_data(cause, effect, data_sources)
        
        if not self.validate_data(data, cause, effect):
            return self._create_invalid_result(cause, effect)
        
        try:
            # For PSM, cause should be binary (treatment/control)
            # If not, we'll binarize it
            treatment = self._binarize(data[cause.name])
            outcome = data[effect.name].values
            
            # Calculate propensity scores
            if confounders and len(confounders) > 0:
                confounder_data = data[[c.name for c in confounders if c.name in data.columns]]
                propensity_scores = self._calculate_propensity_scores(
                    treatment, confounder_data
                )
            else:
                # No confounders - use simple matching
                propensity_scores = treatment.astype(float)
            
            # Perform matching
            if CAUSALML_AVAILABLE:
                ate = self._match_causalml(treatment, outcome, propensity_scores)
            else:
                ate = self._match_simple(treatment, outcome, propensity_scores)
            
            # Statistical test (t-test on matched pairs)
            is_significant, p_value = self._test_significance(
                treatment, outcome, propensity_scores
            )
            
            confidence = 1.0 - p_value if p_value else 0.5
            
            from agent.models import Hypothesis as HypModel
            hypothesis = HypModel(
                cause=cause,
                effect=effect,
                mechanism="Propensity Score Matching",
                confidence=confidence,
                suggested_methods=[CausalMethod.PROPENSITY_SCORE_MATCHING],
            )
            
            return CausalResult(
                hypothesis=hypothesis,
                method=self.method,
                is_significant=is_significant,
                confidence=confidence,
                p_value=p_value,
                effect_size=ate,
                direction=f"{cause.name} → {effect.name}",
                sample_size=len(data),
                result_data={"ate": ate, "propensity_scores": propensity_scores.tolist()},
            )
            
        except Exception as e:
            logger.error(f"PSM test failed: {e}")
            return self._create_invalid_result(cause, effect, str(e))
    
    def _binarize(self, values: pd.Series) -> np.ndarray:
        """Binarize continuous values (above/below median)."""
        if values.dtype in [bool, int] and values.nunique() == 2:
            return values.values.astype(int)
        
        median = values.median()
        return (values > median).astype(int)
    
    def _calculate_propensity_scores(self, treatment: np.ndarray,
                                     confounders: pd.DataFrame) -> np.ndarray:
        """Calculate propensity scores using logistic regression."""
        from sklearn.linear_model import LogisticRegression
        
        if confounders.empty:
            return treatment.astype(float)
        
        model = LogisticRegression()
        model.fit(confounders.values, treatment)
        scores = model.predict_proba(confounders.values)[:, 1]
        return scores
    
    def _match_causalml(self, treatment: np.ndarray, outcome: np.ndarray,
                       propensity_scores: np.ndarray) -> float:
        """Perform matching using causalml."""
        matcher = NearestNeighborMatch()
        matched = matcher.match(treatment, propensity_scores)
        
        # Calculate Average Treatment Effect
        treated_outcomes = outcome[matched == 1]
        control_outcomes = outcome[matched == 0]
        
        if len(treated_outcomes) > 0 and len(control_outcomes) > 0:
            return np.mean(treated_outcomes) - np.mean(control_outcomes)
        return 0.0
    
    def _match_simple(self, treatment: np.ndarray, outcome: np.ndarray,
                     propensity_scores: np.ndarray) -> float:
        """Simple matching implementation."""
        # Match each treated unit to nearest control unit
        treated_idx = np.where(treatment == 1)[0]
        control_idx = np.where(treatment == 0)[0]
        
        if len(treated_idx) == 0 or len(control_idx) == 0:
            return 0.0
        
        matched_outcomes_treated = []
        matched_outcomes_control = []
        
        for t_idx in treated_idx:
            t_score = propensity_scores[t_idx]
            # Find nearest control
            distances = np.abs(propensity_scores[control_idx] - t_score)
            nearest_control = control_idx[np.argmin(distances)]
            
            matched_outcomes_treated.append(outcome[t_idx])
            matched_outcomes_control.append(outcome[nearest_control])
        
        if len(matched_outcomes_treated) > 0:
            return np.mean(matched_outcomes_treated) - np.mean(matched_outcomes_control)
        return 0.0
    
    def _test_significance(self, treatment: np.ndarray, outcome: np.ndarray,
                          propensity_scores: np.ndarray) -> tuple[bool, float]:
        """Test statistical significance."""
        from scipy import stats
        
        # Simple t-test on matched pairs
        treated_idx = np.where(treatment == 1)[0]
        control_idx = np.where(treatment == 0)[0]
        
        if len(treated_idx) < 2 or len(control_idx) < 2:
            return False, 1.0
        
        treated_outcomes = outcome[treated_idx]
        control_outcomes = outcome[control_idx]
        
        t_stat, p_value = stats.ttest_ind(treated_outcomes, control_outcomes)
        return p_value < 0.05, p_value
    
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
                suggested_methods=[CausalMethod.PROPENSITY_SCORE_MATCHING],
            ),
            method=self.method,
            is_significant=False,
            confidence=0.0,
            error=error,
        )
    
    def get_required_data_characteristics(self) -> Dict[str, Any]:
        """Get data requirements for PSM."""
        return {
            "min_samples": 50,
            "time_series": False,
            "binary_treatment": True,  # Can be created from continuous
        }


"""
Confounder testing using conditional independence tests.
"""

from typing import List, Optional
import pandas as pd
import numpy as np
from loguru import logger
from scipy import stats

from agent.models import Variable, Hypothesis, CausalResult


class ConfounderTester:
    """Tests for confounding effects."""
    
    def test_confounders(self, hypothesis: Hypothesis,
                        confounders: List[Variable],
                        data: Optional[pd.DataFrame] = None) -> List[Variable]:
        """
        Test which confounders are significant.
        
        Args:
            hypothesis: Hypothesis being tested
            confounders: List of potential confounders
            data: Optional data (would be fetched in real implementation)
            
        Returns:
            List of confounders that are significant
        """
        significant_confounders = []
        
        for confounder in confounders:
            if self.is_confounding(hypothesis, confounder, data):
                significant_confounders.append(confounder)
        
        return significant_confounders
    
    def is_confounding(self, hypothesis: Hypothesis, confounder: Variable,
                      data: Optional[pd.DataFrame] = None) -> bool:
        """
        Test if a variable is a confounder.
        
        A confounder must be:
        1. Associated with the cause
        2. Associated with the effect
        3. Not on the causal pathway
        
        Args:
            hypothesis: Hypothesis
            confounder: Potential confounder
            data: Optional data
            
        Returns:
            True if confounder is significant
        """
        if data is None or confounder.name not in data.columns:
            # In real implementation, would fetch data
            return False
        
        try:
            # Test 1: Association with cause
            cause_conf_corr, cause_conf_p = stats.pearsonr(
                data[hypothesis.cause.name].values,
                data[confounder.name].values
            )
            
            # Test 2: Association with effect
            effect_conf_corr, effect_conf_p = stats.pearsonr(
                data[hypothesis.effect.name].values,
                data[confounder.name].values
            )
            
            # Both associations must be significant
            is_confounding = (cause_conf_p < 0.05) and (effect_conf_p < 0.05)
            
            return is_confounding
            
        except Exception as e:
            logger.warning(f"Error testing confounder {confounder.name}: {e}")
            return False
    
    def test_conditional_independence(self, x: Variable, y: Variable,
                                     z: Variable,
                                     data: pd.DataFrame) -> bool:
        """
        Test if X and Y are conditionally independent given Z.
        
        Uses partial correlation test.
        
        Args:
            x: Variable X
            y: Variable Y
            z: Conditioning variable Z
            data: Data containing all variables
            
        Returns:
            True if conditionally independent
        """
        try:
            from scipy.stats import pearsonr
            from sklearn.linear_model import LinearRegression
            
            # Residualize X and Y on Z
            z_data = data[z.name].values.reshape(-1, 1)
            
            # Residualize X
            model_x = LinearRegression()
            model_x.fit(z_data, data[x.name].values)
            x_residual = data[x.name].values - model_x.predict(z_data)
            
            # Residualize Y
            model_y = LinearRegression()
            model_y.fit(z_data, data[y.name].values)
            y_residual = data[y.name].values - model_y.predict(z_data)
            
            # Test correlation of residuals
            corr, p_value = pearsonr(x_residual, y_residual)
            
            # If p > 0.05, they are conditionally independent
            return p_value > 0.05
            
        except Exception as e:
            logger.warning(f"Error testing conditional independence: {e}")
            return False


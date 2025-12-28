"""
Abstract base class for causal inference engines.
"""

from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any
import pandas as pd

from agent.models import Variable, DataSource, CausalResult, Hypothesis, CausalMethod


class CausalEngine(ABC):
    """Abstract base class for all causal inference engines."""
    
    def __init__(self, method: CausalMethod):
        self.method = method
    
    @abstractmethod
    def test(self, cause: Variable, effect: Variable, 
             data_sources: List[DataSource],
             data: Optional[pd.DataFrame] = None,
             **kwargs) -> CausalResult:
        """
        Test for causal relationship between cause and effect.
        
        Args:
            cause: Cause variable
            effect: Effect variable
            data_sources: Data sources providing the variables
            data: Optional pre-loaded data (if None, will fetch from sources)
            **kwargs: Method-specific parameters
            
        Returns:
            CausalResult with test results
        """
        pass
    
    @abstractmethod
    def get_required_data_characteristics(self) -> Dict[str, Any]:
        """
        Get data characteristics required for this method.
        
        Returns:
            Dict with requirements (e.g., min_samples, time_series, etc.)
        """
        pass
    
    def fetch_data(self, cause: Variable, effect: Variable,
                   data_sources: List[DataSource]) -> pd.DataFrame:
        """
        Fetch data for variables from data sources.
        
        Args:
            cause: Cause variable
            effect: Effect variable
            data_sources: Data sources
            
        Returns:
            DataFrame with cause and effect columns
        """
        # This is a placeholder - real implementation would fetch from Kafka/APIs
        # For now, return empty DataFrame
        # In production, this would:
        # 1. Connect to Kafka topics for each data source
        # 2. Consume messages and extract variables
        # 3. Join data by timestamp
        # 4. Return aligned DataFrame
        import pandas as pd
        return pd.DataFrame({
            cause.name: [],
            effect.name: [],
        })
    
    def validate_data(self, data: pd.DataFrame, cause: Variable, 
                     effect: Variable) -> bool:
        """
        Validate that data meets requirements for this method.
        
        Args:
            data: Data to validate
            cause: Cause variable
            effect: Effect variable
            
        Returns:
            True if data is valid
        """
        if data.empty:
            return False
        
        if cause.name not in data.columns or effect.name not in data.columns:
            return False
        
        if len(data) < 30:  # Minimum samples
            return False
        
        return True


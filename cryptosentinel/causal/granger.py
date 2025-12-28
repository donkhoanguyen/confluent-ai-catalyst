"""
Granger Causality Analysis Module

Implements causal inference between sentiment and price movements.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from loguru import logger
from scipy import stats
from statsmodels.tsa.stattools import grangercausalitytests, adfuller


class CausalDirection(str, Enum):
    """Direction of causal relationship."""
    SENTIMENT_LEADS = "SENTIMENT_LEADS"
    PRICE_LEADS = "PRICE_LEADS"
    BIDIRECTIONAL = "BIDIRECTIONAL"
    NO_CAUSALITY = "NO_CAUSALITY"


@dataclass
class GrangerResult:
    """Result of a Granger causality test."""
    optimal_lag: int
    f_statistic: float
    p_value: float
    is_significant: bool
    direction: str


@dataclass
class CausalAnalysisResult:
    """Result of bidirectional causal analysis."""
    direction: CausalDirection
    lead_lag_minutes: int
    confidence: float
    sentiment_to_price: GrangerResult
    price_to_sentiment: GrangerResult
    window_start: Optional[datetime] = None
    window_end: Optional[datetime] = None
    sample_size: int = 0


@dataclass
class CausalAlert:
    """Alert generated when causal dynamics change."""
    alert_type: str
    severity: str  # HIGH, MEDIUM, LOW
    message: str
    action: str
    timestamp: datetime


def check_stationarity(series: pd.Series, significance: float = 0.05) -> Tuple[bool, float]:
    """
    Check if a time series is stationary using Augmented Dickey-Fuller test.
    
    Args:
        series: Time series data
        significance: Significance level for the test
        
    Returns:
        Tuple of (is_stationary, p_value)
    """
    try:
        result = adfuller(series.dropna(), autolag="AIC")
        p_value = result[1]
        return p_value < significance, p_value
    except Exception as e:
        logger.warning(f"Stationarity test failed: {e}")
        return True, 0.0  # Assume stationary if test fails


def make_stationary(series: pd.Series) -> pd.Series:
    """
    Make a series stationary by differencing.
    
    Args:
        series: Original time series
        
    Returns:
        Differenced (stationary) series
    """
    is_stationary, _ = check_stationarity(series)
    
    if is_stationary:
        return series
    
    # First difference
    diff_series = series.diff().dropna()
    return diff_series


def compute_granger_causality(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    max_lag: int = 10,
    significance: float = 0.05,
) -> GrangerResult:
    """
    Test if x_col Granger-causes y_col.
    
    Granger causality tests whether past values of X help predict Y,
    beyond what past values of Y alone can predict.
    
    Args:
        df: DataFrame with time-indexed data
        x_col: Column name of potential cause
        y_col: Column name of potential effect
        max_lag: Maximum lag to test
        significance: Significance level (default 0.05)
        
    Returns:
        GrangerResult with optimal lag and test statistics
    """
    # Prepare data - Granger test expects [y, x] column order
    data = df[[y_col, x_col]].dropna()
    
    if len(data) < max_lag * 3:
        logger.warning(f"Insufficient data for Granger test: {len(data)} rows")
        return GrangerResult(
            optimal_lag=0,
            f_statistic=0.0,
            p_value=1.0,
            is_significant=False,
            direction=f"{x_col} → {y_col}",
        )
    
    try:
        # Run Granger test for multiple lags
        results = grangercausalitytests(data, maxlag=max_lag, verbose=False)
        
        # Find optimal lag (lowest p-value)
        best_lag = min(
            results.keys(),
            key=lambda k: results[k][0]["ssr_ftest"][1]
        )
        
        best_result = results[best_lag][0]["ssr_ftest"]
        f_stat = best_result[0]
        p_value = best_result[1]
        
        return GrangerResult(
            optimal_lag=best_lag,
            f_statistic=f_stat,
            p_value=p_value,
            is_significant=p_value < significance,
            direction=f"{x_col} → {y_col}",
        )
        
    except Exception as e:
        logger.error(f"Granger causality test failed: {e}")
        return GrangerResult(
            optimal_lag=0,
            f_statistic=0.0,
            p_value=1.0,
            is_significant=False,
            direction=f"{x_col} → {y_col}",
        )


def bidirectional_granger_test(
    df: pd.DataFrame,
    sentiment_col: str = "avg_sentiment",
    price_col: str = "return_pct",
    max_lag: int = 10,
    significance: float = 0.05,
) -> CausalAnalysisResult:
    """
    Test causality in both directions to determine dominant relationship.
    
    Args:
        df: DataFrame with time-indexed sentiment and price data
        sentiment_col: Column name for sentiment scores
        price_col: Column name for price returns
        max_lag: Maximum lag to test (in time units of the data)
        significance: Significance level
        
    Returns:
        CausalAnalysisResult with direction, lead-lag, and confidence
    """
    # Test: Sentiment → Price
    s_to_p = compute_granger_causality(
        df, sentiment_col, price_col, max_lag, significance
    )
    
    # Test: Price → Sentiment
    p_to_s = compute_granger_causality(
        df, price_col, sentiment_col, max_lag, significance
    )
    
    # Determine dominant direction
    if s_to_p.is_significant and not p_to_s.is_significant:
        direction = CausalDirection.SENTIMENT_LEADS
        lead_lag = s_to_p.optimal_lag
        confidence = 1 - s_to_p.p_value
        
    elif p_to_s.is_significant and not s_to_p.is_significant:
        direction = CausalDirection.PRICE_LEADS
        lead_lag = -p_to_s.optimal_lag  # Negative = price leads
        confidence = 1 - p_to_s.p_value
        
    elif s_to_p.is_significant and p_to_s.is_significant:
        direction = CausalDirection.BIDIRECTIONAL
        # Stronger direction wins for lead-lag
        if s_to_p.f_statistic > p_to_s.f_statistic:
            lead_lag = s_to_p.optimal_lag
            confidence = s_to_p.f_statistic / (s_to_p.f_statistic + p_to_s.f_statistic)
        else:
            lead_lag = -p_to_s.optimal_lag
            confidence = p_to_s.f_statistic / (s_to_p.f_statistic + p_to_s.f_statistic)
            
    else:
        direction = CausalDirection.NO_CAUSALITY
        lead_lag = 0
        confidence = 0.0
    
    return CausalAnalysisResult(
        direction=direction,
        lead_lag_minutes=lead_lag,
        confidence=confidence,
        sentiment_to_price=s_to_p,
        price_to_sentiment=p_to_s,
        sample_size=len(df),
    )


def rolling_causal_analysis(
    df: pd.DataFrame,
    window_size: int = 60,
    step_size: int = 5,
    sentiment_col: str = "avg_sentiment",
    price_col: str = "return_pct",
    max_lag: int = 10,
) -> List[CausalAnalysisResult]:
    """
    Perform causal analysis on rolling windows to track changes over time.
    
    Args:
        df: DataFrame with timestamp index
        window_size: Size of each analysis window (in rows/time units)
        step_size: Step between windows
        sentiment_col: Column name for sentiment
        price_col: Column name for price returns
        max_lag: Maximum lag for Granger test
        
    Returns:
        List of CausalAnalysisResult for each window
    """
    results = []
    
    for start in range(0, len(df) - window_size, step_size):
        window = df.iloc[start:start + window_size]
        
        if len(window) < window_size:
            continue
        
        result = bidirectional_granger_test(
            window,
            sentiment_col=sentiment_col,
            price_col=price_col,
            max_lag=max_lag,
        )
        
        # Add window timestamps
        result.window_start = window.index[0] if hasattr(window.index[0], 'isoformat') else None
        result.window_end = window.index[-1] if hasattr(window.index[-1], 'isoformat') else None
        result.sample_size = len(window)
        
        results.append(result)
    
    return results


def generate_causal_alerts(
    current: CausalAnalysisResult,
    previous: CausalAnalysisResult,
    confidence_threshold: float = 0.3,
    lag_change_threshold: int = 3,
) -> List[CausalAlert]:
    """
    Generate alerts when causal dynamics change significantly.
    
    Args:
        current: Current analysis result
        previous: Previous analysis result
        confidence_threshold: Minimum confidence change to trigger alert
        lag_change_threshold: Minimum lag change (in minutes) to trigger alert
        
    Returns:
        List of CausalAlert objects
    """
    alerts = []
    now = datetime.now(timezone.utc)
    
    # Direction change alert (highest priority)
    if current.direction != previous.direction:
        alerts.append(CausalAlert(
            alert_type="DIRECTION_CHANGE",
            severity="HIGH",
            message=f"Causal direction shifted from {previous.direction.value} to {current.direction.value}",
            action="Review trading strategy - market dynamics have changed",
            timestamp=now,
        ))
    
    # Lead-lag change alert
    lag_change = abs(current.lead_lag_minutes - previous.lead_lag_minutes)
    if lag_change >= lag_change_threshold:
        direction = "increased" if current.lead_lag_minutes > previous.lead_lag_minutes else "decreased"
        alerts.append(CausalAlert(
            alert_type="LAG_SHIFT",
            severity="MEDIUM",
            message=f"Lead-lag time {direction} by {lag_change} minutes (now: {current.lead_lag_minutes} min)",
            action="Adjust signal timing parameters",
            timestamp=now,
        ))
    
    # Confidence change alert
    conf_change = current.confidence - previous.confidence
    if abs(conf_change) >= confidence_threshold:
        direction = "increased" if conf_change > 0 else "decreased"
        alerts.append(CausalAlert(
            alert_type="CONFIDENCE_CHANGE",
            severity="LOW",
            message=f"Causal confidence {direction} by {abs(conf_change):.1%} (now: {current.confidence:.1%})",
            action="Monitor for stability before acting",
            timestamp=now,
        ))
    
    # New causality detected
    if previous.direction == CausalDirection.NO_CAUSALITY and current.direction != CausalDirection.NO_CAUSALITY:
        alerts.append(CausalAlert(
            alert_type="CAUSALITY_DETECTED",
            severity="HIGH",
            message=f"Causal relationship detected: {current.direction.value}",
            action="New predictive signal available - consider incorporating into strategy",
            timestamp=now,
        ))
    
    # Causality lost
    if previous.direction != CausalDirection.NO_CAUSALITY and current.direction == CausalDirection.NO_CAUSALITY:
        alerts.append(CausalAlert(
            alert_type="CAUSALITY_LOST",
            severity="MEDIUM",
            message="Causal relationship is no longer statistically significant",
            action="Reduce reliance on sentiment-based signals",
            timestamp=now,
        ))
    
    return alerts


def compute_cross_correlation(
    df: pd.DataFrame,
    sentiment_col: str = "avg_sentiment",
    price_col: str = "return_pct",
    max_lag: int = 30,
) -> Tuple[int, float]:
    """
    Compute cross-correlation to find optimal lag (simpler alternative to Granger).
    
    This is a faster, simpler method that finds the lag at which
    sentiment best correlates with future price movements.
    
    Args:
        df: DataFrame with sentiment and price columns
        sentiment_col: Column name for sentiment
        price_col: Column name for price returns
        max_lag: Maximum lag to test
        
    Returns:
        Tuple of (optimal_lag, max_correlation)
    """
    sentiment = df[sentiment_col].values
    price = df[price_col].values
    
    correlations = []
    
    for lag in range(1, max_lag + 1):
        if lag >= len(sentiment):
            break
        # Correlate sentiment[:-lag] with price[lag:]
        corr = np.corrcoef(sentiment[:-lag], price[lag:])[0, 1]
        correlations.append((lag, corr if not np.isnan(corr) else 0))
    
    if not correlations:
        return 0, 0.0
    
    # Find lag with maximum absolute correlation
    optimal_lag, max_corr = max(correlations, key=lambda x: abs(x[1]))
    
    return optimal_lag, max_corr


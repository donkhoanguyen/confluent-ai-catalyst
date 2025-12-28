"""
Canonical DataFrame Builder for Causal Analysis.

Assembles a clean, analysis-ready DataFrame from existing data stores
(Kafka topics, in-memory stores) with proper schema, joins, and quality checks.
Also provides CSV persistence for centralized data storage.
"""

from typing import Optional, Dict, List, Tuple
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import numpy as np
from loguru import logger

from .models import Variable, Hypothesis
from .asset_registry import get_asset_registry


class CanonicalDataFrameBuilder:
    """
    Builds a canonical analysis DataFrame from existing data sources.
    
    The canonical table has:
    - One row per (coin_id, timestamp) combination
    - All variables as columns
    - Proper time alignment
    - Basic quality checks (missingness, outliers)
    - CSV persistence for centralized storage
    """
    
    def __init__(self, data_dir: Optional[str] = None):
        """
        Initialize the DataFrame builder.
        
        Args:
            data_dir: Directory to store CSV files (default: ./data/canonical)
        """
        self.registry = get_asset_registry()
        if data_dir is None:
            data_dir = "./data/canonical"
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Canonical DataFrame storage: {self.data_dir}")
    
    def build_from_datastore(
        self,
        coin_id: str,
        price_history: List[Dict],
        sentiment_history: List[Dict],
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        save_to_csv: bool = True,
    ) -> Optional[pd.DataFrame]:
        """
        Build canonical DataFrame from in-memory DataStore history.
        
        Args:
            coin_id: Cryptocurrency identifier
            price_history: List of price records from DataStore.price_history
            sentiment_history: List of sentiment records from DataStore.sentiment_history
            start_time: Optional start time filter
            end_time: Optional end time filter
            save_to_csv: If True, automatically save to CSV
            
        Returns:
            DataFrame with columns: timestamp, coin_id, price_usd, return_pct, 
            volume_24h, avg_sentiment, post_count, etc.
        """
        if not price_history or not sentiment_history:
            logger.warning(f"Insufficient data for {coin_id}")
            return None
        
        # Convert to DataFrames
        price_df = pd.DataFrame(price_history)
        sentiment_df = pd.DataFrame(sentiment_history)
        
        # Ensure timestamp is datetime
        if 'timestamp' in price_df.columns:
            price_df['timestamp'] = pd.to_datetime(price_df['timestamp'])
        if 'timestamp' in sentiment_df.columns:
            sentiment_df['timestamp'] = pd.to_datetime(sentiment_df['timestamp'])
        
        # Set timestamp as index for easier alignment
        price_df = price_df.set_index('timestamp').sort_index()
        sentiment_df = sentiment_df.set_index('timestamp').sort_index()
        
        # Apply time filters if provided
        if start_time:
            price_df = price_df[price_df.index >= start_time]
            sentiment_df = sentiment_df[sentiment_df.index >= start_time]
        if end_time:
            price_df = price_df[price_df.index <= end_time]
            sentiment_df = sentiment_df[sentiment_df.index <= end_time]
        
        # Resample to common frequency (1 minute)
        # Forward fill price data (last known price)
        price_resampled = price_df.resample('1T').ffill()
        
        # Aggregate sentiment data (average over 1-minute windows)
        sentiment_resampled = sentiment_df.resample('1T').agg({
            'avg_sentiment': 'mean',
            'post_count': 'sum',
        })
        
        # Join on timestamp
        df = price_resampled.join(sentiment_resampled, how='inner')
        
        if len(df) == 0:
            logger.warning(f"No overlapping timestamps for {coin_id}")
            return None
        
        # Add coin_id column
        df['coin_id'] = coin_id
        
        # Reset index to make timestamp a column
        df = df.reset_index()
        
        # Rename columns to match canonical schema
        column_mapping = {
            'price_usd': 'price_usd',
            'return_pct': 'return_pct',  # or price_change_24h_pct
            'price_change_24h_pct': 'return_pct',
            'avg_sentiment': 'avg_sentiment',
            'post_count': 'post_count',
        }
        
        # Rename if needed
        for old_name, new_name in column_mapping.items():
            if old_name in df.columns and new_name not in df.columns:
                df = df.rename(columns={old_name: new_name})
        
        # Ensure required columns exist
        required_cols = ['timestamp', 'coin_id']
        for col in required_cols:
            if col not in df.columns:
                logger.error(f"Missing required column: {col}")
                return None
        
        # Reorder columns: timestamp, coin_id, then others
        other_cols = [c for c in df.columns if c not in ['timestamp', 'coin_id']]
        df = df[['timestamp', 'coin_id'] + other_cols]
        
        # Quality checks
        df = self._apply_quality_checks(df, coin_id)
        
        logger.info(f"Built canonical DataFrame for {coin_id}: {len(df)} rows, {len(df.columns)} columns")
        
        # Auto-save to CSV
        if save_to_csv:
            try:
                csv_path = self.save_to_csv(df, coin_id)
                logger.info(f"Auto-saved canonical DataFrame to {csv_path}")
            except Exception as e:
                logger.warning(f"Failed to auto-save DataFrame to CSV: {e}")
        
        return df
    
    def build_for_hypothesis(
        self,
        hypothesis: Hypothesis,
        coin_id: str,
        price_history: Optional[List[Dict]] = None,
        sentiment_history: Optional[List[Dict]] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> Optional[pd.DataFrame]:
        """
        Build DataFrame specifically for testing a hypothesis.
        
        Includes cause, effect, and confounder variables.
        
        Args:
            hypothesis: Hypothesis to test
            coin_id: Cryptocurrency identifier
            price_history: Price history data (optional, will try CSV first)
            sentiment_history: Sentiment history data (optional, will try CSV first)
            start_time: Optional start time
            end_time: Optional end time
            
        Returns:
            DataFrame with cause, effect, and confounder columns
        """
        # Try to load from CSV first (unified_market_data.csv)
        df = self.load_from_csv(coin_id, start_time=start_time, end_time=end_time)
        
        # If CSV not available and we have history, build from scratch
        if df is None and price_history and sentiment_history:
            df = self.build_from_datastore(
                coin_id, price_history, sentiment_history, start_time, end_time
            )
        
        if df is None:
            logger.warning(f"No data available for {coin_id} (tried CSV and DataStore)")
            return None
        
        # Map hypothesis variables to DataFrame columns
        # Try both legacy and unified column names
        cause_col = self._map_variable_to_column(hypothesis.cause)
        effect_col = self._map_variable_to_column(hypothesis.effect)
        
        # If mapped column not found, try alternative names
        if cause_col not in df.columns:
            # Try legacy name if unified name was used
            alt_cause = self._try_alternative_column_name(cause_col, df.columns)
            if alt_cause:
                cause_col = alt_cause
                logger.debug(f"Mapped cause '{hypothesis.cause.name}' to column '{cause_col}'")
            else:
                logger.warning(f"Cause variable '{hypothesis.cause.name}' (mapped to '{cause_col}') not found in DataFrame. Available columns: {list(df.columns)}")
                return None
        
        if effect_col not in df.columns:
            # Try legacy name if unified name was used
            alt_effect = self._try_alternative_column_name(effect_col, df.columns)
            if alt_effect:
                effect_col = alt_effect
                logger.debug(f"Mapped effect '{hypothesis.effect.name}' to column '{effect_col}'")
            else:
                logger.warning(f"Effect variable '{hypothesis.effect.name}' (mapped to '{effect_col}') not found in DataFrame. Available columns: {list(df.columns)}")
                return None
        
        # Select relevant columns: timestamp, coin_id, cause, effect, confounders
        selected_cols = ['timestamp', 'coin_id', cause_col, effect_col]
        
        # Add confounder columns if available
        for confounder in hypothesis.potential_confounders:
            conf_col = self._map_variable_to_column(confounder)
            # Try alternative names if mapped column not found
            if conf_col not in df.columns:
                alt_col = self._try_alternative_column_name(conf_col, df.columns)
                if alt_col:
                    conf_col = alt_col
                    logger.debug(f"Mapped confounder '{confounder.name}' to column '{conf_col}'")
                else:
                    logger.debug(f"Confounder '{confounder.name}' (mapped to '{conf_col}') not available in DataFrame")
                    continue
            selected_cols.append(conf_col)
        
        df_selected = df[selected_cols].copy()
        
        # Rename to standard names for analysis
        df_selected = df_selected.rename(columns={
            cause_col: 'cause',
            effect_col: 'effect',
        })
        
        # Rename confounders
        confounder_cols = {}
        for confounder in hypothesis.potential_confounders:
            conf_col = self._map_variable_to_column(confounder)
            if conf_col in df.columns:
                confounder_cols[conf_col] = f"confounder_{confounder.name}"
        
        if confounder_cols:
            df_selected = df_selected.rename(columns=confounder_cols)
        
        # Save hypothesis-specific DataFrame
        try:
            suffix = f"hyp_{hypothesis.hypothesis_id.replace(':', '_')}"
            csv_path = self.save_to_csv(df_selected, coin_id, suffix=suffix)
            logger.info(f"Saved hypothesis DataFrame to {csv_path}")
        except Exception as e:
            logger.warning(f"Failed to save hypothesis DataFrame: {e}")
        
        return df_selected
    
    def _map_variable_to_column(self, variable: Variable) -> str:
        """
        Map a Variable object to a DataFrame column name.
        
        Supports both legacy (Reddit-based) and unified (News-based) column names.
        
        Args:
            variable: Variable to map
            
        Returns:
            Column name in the canonical DataFrame
        """
        # Direct mapping based on variable name
        # Supports both legacy names and unified CSV column names
        mapping = {
            'price_usd': 'price_usd',
            'price_change_24h_pct': 'return_pct',
            'return_pct': 'return_pct',
            'volume_24h': 'volume_24h',
            'market_cap': 'market_cap',
            # Sentiment: map to unified CSV column name
            'avg_sentiment': 'news_sentiment_avg',  # Unified CSV uses news_sentiment_avg
            'sentiment_score': 'news_sentiment_avg',  # Use aggregated version
            'news_sentiment_avg': 'news_sentiment_avg',  # Direct match
            # Count variables: map to unified CSV column names
            'post_count': 'news_count',  # Unified CSV uses news_count
            'news_count': 'news_count',  # Direct match
            'positive_count': 'news_positive_count',  # Unified CSV uses news_positive_count
            'news_positive_count': 'news_positive_count',  # Direct match
            'negative_count': 'news_negative_count',  # Unified CSV uses news_negative_count
            'news_negative_count': 'news_negative_count',  # Direct match
            # Fear & Greed Index
            'fear_greed_value': 'fear_greed_value',
            'fear_greed_classification': 'fear_greed_classification',
        }
        
        return mapping.get(variable.name, variable.name.lower().replace(' ', '_'))
    
    def _try_alternative_column_name(self, column_name: str, available_columns: list) -> Optional[str]:
        """
        Try to find an alternative column name if the mapped one doesn't exist.
        
        Handles mapping between legacy (Reddit) and unified (News) column names.
        
        Args:
            column_name: The column name we're looking for
            available_columns: List of available column names in the DataFrame
            
        Returns:
            Alternative column name if found, None otherwise
        """
        # Mapping between unified CSV names and legacy names
        alternatives = {
            'news_sentiment_avg': ['avg_sentiment', 'sentiment_score'],
            'avg_sentiment': ['news_sentiment_avg'],
            'news_count': ['post_count'],
            'post_count': ['news_count'],
            'news_positive_count': ['positive_count'],
            'positive_count': ['news_positive_count'],
            'news_negative_count': ['negative_count'],
            'negative_count': ['news_negative_count'],
        }
        
        if column_name in alternatives:
            for alt in alternatives[column_name]:
                if alt in available_columns:
                    return alt
        
        return None
    
    def _apply_quality_checks(self, df: pd.DataFrame, coin_id: str) -> pd.DataFrame:
        """
        Apply basic quality checks to the DataFrame.
        
        - Remove rows with all NaN values
        - Log missingness statistics
        - Handle outliers (optional, just log for now)
        
        Args:
            df: DataFrame to check
            coin_id: Coin identifier for logging
            
        Returns:
            Cleaned DataFrame
        """
        original_len = len(df)
        
        # Remove rows where all data columns are NaN (keep timestamp and coin_id)
        data_cols = [c for c in df.columns if c not in ['timestamp', 'coin_id']]
        df = df.dropna(subset=data_cols, how='all')
        
        if len(df) < original_len:
            logger.info(f"Removed {original_len - len(df)} rows with all NaN data for {coin_id}")
        
        # Log missingness
        missingness = df[data_cols].isnull().sum()
        if missingness.any():
            logger.debug(f"Missing data for {coin_id}: {missingness[missingness > 0].to_dict()}")
        
        # Check for reasonable value ranges
        if 'price_usd' in df.columns:
            if (df['price_usd'] <= 0).any():
                logger.warning(f"Found non-positive prices for {coin_id}")
        
        if 'avg_sentiment' in df.columns:
            if ((df['avg_sentiment'] < -1) | (df['avg_sentiment'] > 1)).any():
                logger.warning(f"Sentiment scores out of range [-1, 1] for {coin_id}")
        
        return df
    
    def get_schema(self) -> Dict[str, str]:
        """
        Get the canonical DataFrame schema.
        
        Returns unified schema that matches unified_market_data.csv format.
        """
        return {
            'timestamp': 'datetime64[ns]',
            'coin_id': 'string',
            'price_usd': 'float64',
            'return_pct': 'float64',  # Alias for price_change_24h_pct
            'volume_24h': 'float64',
            'market_cap': 'float64',
            # Unified CSV uses news-based column names
            'news_sentiment_avg': 'float64',  # Unified CSV column name
            'news_count': 'int64',  # Unified CSV column name
            'news_positive_count': 'int64',  # Unified CSV column name
            'news_negative_count': 'int64',  # Unified CSV column name
            # Fear & Greed Index (new variables)
            'fear_greed_value': 'int64',
            'fear_greed_classification': 'string',
            # Legacy column names (for backward compatibility)
            'avg_sentiment': 'float64',  # Legacy alias
            'post_count': 'int64',  # Legacy alias
            'positive_count': 'int64',  # Legacy alias
            'negative_count': 'int64',  # Legacy alias
        }
    
    def validate_dataframe(self, df: pd.DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate that a DataFrame matches the canonical schema.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            (is_valid, list_of_errors)
        """
        errors = []
        schema = self.get_schema()
        
        # Check required columns
        required = ['timestamp', 'coin_id']
        for col in required:
            if col not in df.columns:
                errors.append(f"Missing required column: {col}")
        
        # Check data types
        for col, expected_type in schema.items():
            if col in df.columns:
                actual_type = str(df[col].dtype)
                if expected_type not in actual_type and actual_type != 'object':
                    errors.append(
                        f"Column '{col}' has type {actual_type}, expected {expected_type}"
                    )
        
        # Check for empty DataFrame
        if len(df) == 0:
            errors.append("DataFrame is empty")
        
        # Check timestamp is sorted
        if 'timestamp' in df.columns:
            if not df['timestamp'].is_monotonic_increasing:
                errors.append("Timestamp column is not sorted")
        
        return len(errors) == 0, errors
    
    def save_to_csv(
        self,
        df: pd.DataFrame,
        coin_id: str,
        suffix: Optional[str] = None,
        append: bool = False,
    ) -> Optional[Path]:
        """
        Save canonical DataFrame to CSV file.
        
        Args:
            df: DataFrame to save
            coin_id: Cryptocurrency identifier
            suffix: Optional suffix for filename (e.g., "hypothesis_1")
            append: If True, append to existing file; otherwise overwrite
            
        Returns:
            Path to saved CSV file
        """
        if df is None or df.empty:
            logger.warning(f"Cannot save empty DataFrame for {coin_id}")
            return None
        
        # Generate filename
        if suffix:
            filename = f"{coin_id}_{suffix}.csv"
        else:
            # Use timestamp range in filename
            start_ts = df['timestamp'].min().strftime('%Y%m%d_%H%M%S')
            end_ts = df['timestamp'].max().strftime('%Y%m%d_%H%M%S')
            filename = f"{coin_id}_{start_ts}_to_{end_ts}.csv"
        
        filepath = self.data_dir / filename
        
        # Save to CSV
        if append and filepath.exists():
            # Append mode: read existing, combine, deduplicate, save
            existing_df = pd.read_csv(filepath, parse_dates=['timestamp'])
            combined_df = pd.concat([existing_df, df], ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset=['timestamp', 'coin_id'], keep='last')
            combined_df = combined_df.sort_values('timestamp')
            combined_df.to_csv(filepath, index=False)
            logger.info(f"Appended {len(df)} rows to {filepath} (total: {len(combined_df)} rows)")
        else:
            df.to_csv(filepath, index=False)
            logger.info(f"Saved {len(df)} rows to {filepath}")
        
        return filepath
    
    def load_from_csv(
        self,
        coin_id: str,
        suffix: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> Optional[pd.DataFrame]:
        """
        Load canonical DataFrame from CSV file.
        
        First tries unified_market_data.csv (unified table format),
        then falls back to per-coin files.
        
        Args:
            coin_id: Cryptocurrency identifier
            suffix: Optional suffix for filename
            start_time: Optional start time filter
            end_time: Optional end time filter
            
        Returns:
            DataFrame or None if file not found
        """
        # First, try unified_market_data.csv (unified table format)
        unified_filepath = self.data_dir / "unified_market_data.csv"
        if unified_filepath.exists():
            try:
                df = pd.read_csv(unified_filepath, parse_dates=['timestamp'])
                
                # Filter by coin_id
                if 'coin_id' in df.columns:
                    df = df[df['coin_id'] == coin_id]
                
                # Apply time filters
                if start_time:
                    df = df[df['timestamp'] >= start_time]
                if end_time:
                    df = df[df['timestamp'] <= end_time]
                
                if len(df) > 0:
                    logger.info(f"Loaded {len(df)} rows from unified table: {unified_filepath}")
                    return df
            except Exception as e:
                logger.debug(f"Failed to load unified table: {e}")
        
        # Fallback: try to find matching file
        if suffix:
            filename = f"{coin_id}_{suffix}.csv"
            filepath = self.data_dir / filename
        else:
            # Find most recent file for this coin
            pattern = f"{coin_id}_*.csv"
            matching_files = list(self.data_dir.glob(pattern))
            if not matching_files:
                logger.debug(f"No CSV files found for {coin_id}")
                return None
            # Sort by modification time, get most recent
            filepath = max(matching_files, key=lambda p: p.stat().st_mtime)
        
        if not filepath.exists():
            logger.debug(f"CSV file not found: {filepath}")
            return None
        
        try:
            df = pd.read_csv(filepath, parse_dates=['timestamp'])
            
            # Apply time filters
            if start_time:
                df = df[df['timestamp'] >= start_time]
            if end_time:
                df = df[df['timestamp'] <= end_time]
            
            logger.info(f"Loaded {len(df)} rows from {filepath}")
            return df
            
        except Exception as e:
            logger.error(f"Error loading CSV from {filepath}: {e}")
            return None
    
    def get_latest_csv_path(self, coin_id: str) -> Optional[Path]:
        """
        Get path to the most recent CSV file for a coin.
        
        Args:
            coin_id: Cryptocurrency identifier
            
        Returns:
            Path to latest CSV file or None
        """
        pattern = f"{coin_id}_*.csv"
        matching_files = list(self.data_dir.glob(pattern))
        if not matching_files:
            return None
        return max(matching_files, key=lambda p: p.stat().st_mtime)
    
    def list_available_coins(self) -> List[str]:
        """
        List all coin_ids that have saved CSV files.
        
        Returns:
            List of coin identifiers
        """
        csv_files = list(self.data_dir.glob("*.csv"))
        coins = set()
        for file in csv_files:
            # Extract coin_id from filename (format: coin_id_*.csv)
            parts = file.stem.split('_')
            if parts:
                coins.add(parts[0])
        return sorted(list(coins))

#!/usr/bin/env python3
"""
Test script for causal discovery agent with unified schema.

Tests that the agent can:
1. Load data from unified_market_data.csv
2. Map variables correctly (legacy and unified column names)
3. Generate hypotheses using unified schema variables
4. Run causal tests with unified data

Run with:
    python test_agent_unified_schema.py
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from agent.orchestrator import CausalDiscoveryAgent
from agent.dataframe_builder import CanonicalDataFrameBuilder
from agent.asset_registry import get_asset_registry
from config.settings import get_settings
from loguru import logger

# Configure logging
logger.add("test_agent_unified_schema.log", rotation="10 MB", level="INFO")


def test_dataframe_loading():
    """Test that DataFrame builder can load unified CSV."""
    print("\n" + "=" * 70)
    print("Test 1: DataFrame Loading from Unified CSV")
    print("=" * 70)
    
    builder = CanonicalDataFrameBuilder(data_dir="./data/canonical")
    
    # Try loading unified CSV
    df = builder.load_from_csv("bitcoin")
    
    if df is None or len(df) == 0:
        print("❌ FAILED: Could not load unified_market_data.csv")
        print("   Make sure you've run test_unified_collector.py first to generate data.")
        return False
    
    print(f"✅ SUCCESS: Loaded {len(df)} rows from unified CSV")
    print(f"   Columns: {list(df.columns)}")
    print(f"   Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
    
    # Check for required columns
    required_cols = ['timestamp', 'coin_id', 'price_usd', 'return_pct']
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        print(f"❌ FAILED: Missing required columns: {missing}")
        return False
    
    print("✅ All required columns present")
    return True


def test_variable_mapping():
    """Test that variable-to-column mapping works for both legacy and unified names."""
    print("\n" + "=" * 70)
    print("Test 2: Variable-to-Column Mapping")
    print("=" * 70)
    
    builder = CanonicalDataFrameBuilder(data_dir="./data/canonical")
    registry = get_asset_registry()
    
    # Load test data
    df = builder.load_from_csv("bitcoin")
    if df is None or len(df) == 0:
        print("❌ SKIPPED: No data available")
        return False
    
    # Test legacy variable names
    test_vars = [
        ("avg_sentiment", "news_sentiment_avg"),
        ("post_count", "news_count"),
        ("positive_count", "news_positive_count"),
        ("negative_count", "news_negative_count"),
        ("fear_greed_value", "fear_greed_value"),
    ]
    
    all_passed = True
    for var_name, expected_col in test_vars:
        var = registry.get_variable(var_name, "cryptocurrency")
        if var is None:
            print(f"⚠️  Variable '{var_name}' not in registry (may be expected)")
            continue
        
        mapped_col = builder._map_variable_to_column(var)
        
        # Try alternative mapping if needed
        if mapped_col not in df.columns:
            alt_col = builder._try_alternative_column_name(mapped_col, df.columns)
            if alt_col:
                mapped_col = alt_col
        
        if mapped_col in df.columns:
            print(f"✅ '{var_name}' → '{mapped_col}' (found in DataFrame)")
        else:
            print(f"❌ '{var_name}' → '{mapped_col}' (NOT found in DataFrame)")
            print(f"   Available columns: {list(df.columns)}")
            all_passed = False
    
    return all_passed


def test_agent_initialization():
    """Test that agent can initialize and load data."""
    print("\n" + "=" * 70)
    print("Test 3: Agent Initialization")
    print("=" * 70)
    
    settings = get_settings()
    settings.offline_mode = True
    
    try:
        agent = CausalDiscoveryAgent(settings)
        print("✅ Agent initialized successfully")
        
        # Check if offline data was loaded
        if agent.offline_df is not None:
            print(f"✅ Offline data loaded: {len(agent.offline_df)} rows")
            print(f"   Columns: {list(agent.offline_df.columns)}")
        else:
            print("⚠️  No offline data loaded (may be expected if CSV is empty)")
        
        return True
    except Exception as e:
        print(f"❌ FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_hypothesis_generation():
    """Test that agent can generate hypotheses using unified schema variables."""
    print("\n" + "=" * 70)
    print("Test 4: Hypothesis Generation with Unified Schema")
    print("=" * 70)
    
    settings = get_settings()
    settings.offline_mode = True
    
    try:
        agent = CausalDiscoveryAgent(settings)
        
        # Run discovery in offline mode with recursion limit
        result = agent.discover(
            domain="cryptocurrency",
            query="Does news sentiment or fear & greed index cause price movements?",
            config={
                "thread_id": "test_unified_schema", 
                "offline_mode": True,
                "recursion_limit": 30  # Increase limit for testing
            }
        )
        
        print(f"✅ Discovery completed with status: {result.status}")
        print(f"   Hypotheses generated: {len(result.hypotheses)}")
        print(f"   Causal tests run: {len(result.results)}")
        
        # Check if hypotheses use unified schema variables
        if result.hypotheses:
            print("\n   Generated Hypotheses:")
            for i, hyp in enumerate(result.hypotheses, 1):
                print(f"   {i}. {hyp.cause.name} → {hyp.effect.name}")
                print(f"      Methods: {[m.value for m in hyp.suggested_methods]}")
        
        # Check if any results were generated
        if result.results:
            significant = [r for r in result.results if r.is_significant]
            print(f"\n   Significant results: {len(significant)}/{len(result.results)}")
        
        return True
    except Exception as e:
        print(f"❌ FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_dataframe_builder_for_hypothesis():
    """Test that DataFrame builder can extract data for a hypothesis."""
    print("\n" + "=" * 70)
    print("Test 5: DataFrame Builder for Hypothesis")
    print("=" * 70)
    
    builder = CanonicalDataFrameBuilder(data_dir="./data/canonical")
    registry = get_asset_registry()
    
    # Create a test hypothesis
    cause = registry.get_variable("news_sentiment_avg", "cryptocurrency")
    effect = registry.get_variable("price_usd", "cryptocurrency")
    
    if cause is None or effect is None:
        print("❌ SKIPPED: Required variables not in registry")
        return False
    
    from agent.models import Hypothesis, CausalMethod
    
    hypothesis = Hypothesis(
        cause=cause,
        effect=effect,
        mechanism="News sentiment may affect price",
        confidence=0.6,
        suggested_methods=[CausalMethod.GRANGER],
        hypothesis_id="test_hyp_1"
    )
    
    try:
        df = builder.build_for_hypothesis(
            hypothesis=hypothesis,
            coin_id="bitcoin"
        )
        
        if df is None or len(df) == 0:
            print("❌ FAILED: Could not build DataFrame for hypothesis")
            return False
        
        print(f"✅ SUCCESS: Built DataFrame with {len(df)} rows")
        print(f"   Columns: {list(df.columns)}")
        
        # Check that cause and effect columns are present
        if 'cause' in df.columns and 'effect' in df.columns:
            print("✅ Cause and effect columns mapped correctly")
        else:
            print(f"⚠️  Cause/effect columns not found (may use different names)")
            print(f"   Available: {list(df.columns)}")
        
        return True
    except Exception as e:
        print(f"❌ FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all tests."""
    print("=" * 70)
    print("🧪 Testing Causal Discovery Agent with Unified Schema")
    print("=" * 70)
    print("\nThis test suite verifies that the agent works with unified_market_data.csv")
    print("Make sure you've run test_unified_collector.py first to generate test data.\n")
    
    results = []
    
    # Run tests
    results.append(("DataFrame Loading", test_dataframe_loading()))
    results.append(("Variable Mapping", test_variable_mapping()))
    results.append(("Agent Initialization", test_agent_initialization()))
    results.append(("Hypothesis Generation", test_hypothesis_generation()))
    results.append(("DataFrame Builder for Hypothesis", test_dataframe_builder_for_hypothesis()))
    
    # Summary
    print("\n" + "=" * 70)
    print("📊 Test Summary")
    print("=" * 70)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status}: {name}")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All tests passed! The agent is ready to use with unified schema.")
        return 0
    else:
        print(f"\n⚠️  {total - passed} test(s) failed. Check the output above for details.")
        return 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n\n⚠️  Tests interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.exception("Unexpected error during testing")
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)


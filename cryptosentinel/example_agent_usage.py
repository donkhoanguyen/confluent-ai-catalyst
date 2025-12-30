#!/usr/bin/env python3
"""
Example script showing how to use the Autonomous Causal Discovery System.

This demonstrates the two-agent architecture:
1. DataCurationAgent - Discovers data sources and prepares data
2. CausalDiscoveryAgent - Generates and tests causal hypotheses

The system is domain-agnostic and works with any use case.

Run this after setting up your .env file:
    python example_agent_usage.py [--offline] [--domain DOMAIN] [--query QUERY]

Examples:
    # Cryptocurrency analysis (default)
    python example_agent_usage.py --offline
    
    # Healthcare analysis
    python example_agent_usage.py --offline --domain healthcare --query "What causes patient readmissions?"
    
    # Retail analysis
    python example_agent_usage.py --offline --domain retail --query "What drives customer churn?"
"""

import argparse

from agent.orchestrator import CausalDiscoveryAgent
from agent.curation_orchestrator import DataCurationAgent
from config.settings import get_settings
from loguru import logger

# Configure logging
logger.add("agent_discovery.log", rotation="10 MB")


def main():
    """Example usage of the causal discovery system with the new architecture."""
    parser = argparse.ArgumentParser(
        description="Run the causal discovery system demo.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python example_agent_usage.py --offline
  python example_agent_usage.py --domain healthcare --query "What causes patient readmissions?"
  python example_agent_usage.py --domain finance --query "What factors predict stock volatility?"
        """
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help="Run offline demo with bundled sample data (no external services)",
    )
    parser.add_argument(
        "--domain",
        type=str,
        default="cryptocurrency",
        help="Domain for analysis (e.g., healthcare, finance, retail, energy)",
    )
    parser.add_argument(
        "--query",
        type=str,
        default="What are the causal relationships in this domain?",
        help="Research question to investigate",
    )
    args = parser.parse_args()

    print("=" * 70)
    print("🤖 Autonomous Causal Discovery System")
    print("=" * 70)
    print()
    print(f"📁 Domain: {args.domain}")
    print(f"❓ Query: {args.query}")
    print()

    # Initialize settings
    settings = get_settings()
    if args.offline:
        settings.offline_mode = True
        print("🔒 Offline mode enabled")
    print()

    # =========================================================================
    # STEP 1: Data Curation Agent - Discover and prepare data sources
    # =========================================================================
    print("=" * 70)
    print("📊 STEP 1: Data Curation Agent")
    print("=" * 70)
    print("Discovering data sources and preparing data for analysis...")
    print()

    curation_agent = DataCurationAgent(settings)
    curation_result = curation_agent.curate(
        domain=args.domain,
        config={
            "offline_mode": settings.offline_mode,
            "min_rows": 50,  # Minimum rows needed for analysis
        }
    )

    print(f"Status: {curation_result.get('status', 'UNKNOWN')}")
    print(f"Data Sources Discovered: {len(curation_result.get('active_data_sources', []))}")
    print(f"Variables Discovered: {len(curation_result.get('discovered_variables', []))}")
    print(f"Datasets Found: {len(curation_result.get('datasets', []))}")
    print(f"Readiness Score: {curation_result.get('readiness_score', 0):.0%}")
    print()

    # Display discovered data sources
    data_sources = curation_result.get('active_data_sources', [])
    if data_sources:
        print("🔌 Discovered Data Sources:")
        for source in data_sources[:5]:
            print(f"  - {source.name} ({source.source_type})")
            if hasattr(source, 'description') and source.description:
                print(f"    {source.description[:60]}...")
        if len(data_sources) > 5:
            print(f"  ... and {len(data_sources) - 5} more")
        print()

    # Display discovered variables
    discovered_vars = curation_result.get('discovered_variables', [])
    if discovered_vars:
        print("📋 Discovered Variables:")
        for var in discovered_vars[:8]:
            var_type = var.variable_type.value if hasattr(var.variable_type, 'value') else str(var.variable_type)
            print(f"  - {var.name} ({var_type})")
        if len(discovered_vars) > 8:
            print(f"  ... and {len(discovered_vars) - 8} more")
        print()

    # =========================================================================
    # STEP 2: Causal Discovery Agent - Generate and test hypotheses
    # =========================================================================
    print("=" * 70)
    print("🔬 STEP 2: Causal Discovery Agent")
    print("=" * 70)
    print("Generating hypotheses based on discovered data...")
    print()

    causal_agent = CausalDiscoveryAgent(settings)
    
    # Pass discovered variables and data sources from curation agent
    result = causal_agent.discover(
        domain=args.domain,
        query=args.query,
        config={
            "thread_id": f"example_{args.domain}_1",
            "offline_mode": settings.offline_mode,
        },
        discovered_variables=curation_result.get('discovered_variables', []),
        active_data_sources=curation_result.get('active_data_sources', []),
    )

    print(f"Status: {result.status}")
    print(f"Hypotheses Generated: {len(result.hypotheses)}")
    print(f"Causal Tests Run: {len(result.results)}")
    print(f"Iterations: {result.iteration}")
    print()

    # Display hypotheses
    if result.hypotheses:
        print("📋 Generated Hypotheses:")
        for i, hyp in enumerate(result.hypotheses, 1):
            print(f"  {i}. {hyp.cause.name} → {hyp.effect.name}")
            mechanism = hyp.mechanism[:60] + "..." if len(hyp.mechanism) > 60 else hyp.mechanism
            print(f"     Mechanism: {mechanism}")
            print(f"     Confidence: {hyp.confidence:.0%}")
            methods = [m.value for m in hyp.suggested_methods] if hyp.suggested_methods else []
            print(f"     Methods: {methods}")
            print()

    # Display test results
    if result.results:
        print("🧪 Causal Test Results:")
        for i, res in enumerate(result.results, 1):
            status = "✅ Significant" if res.is_significant else "❌ Not significant"
            print(f"  {i}. {res.hypothesis.cause.name} → {res.hypothesis.effect.name}")
            print(f"     {status}")
            print(f"     Method: {res.method.value}")
            if res.p_value is not None:
                print(f"     P-value: {res.p_value:.4f}")
            print(f"     Confidence: {res.confidence:.0%}")
            if res.lead_lag:
                print(f"     Lead-lag: {res.lead_lag} time units")
            print()

    # Display significant results summary
    significant = [r for r in result.results if r.is_significant]
    if significant:
        print("=" * 70)
        print("✅ SIGNIFICANT CAUSAL RELATIONSHIPS FOUND")
        print("=" * 70)
        for i, res in enumerate(significant, 1):
            print(f"  {i}. {res.hypothesis.cause.name} → {res.hypothesis.effect.name}")
            print(f"     {res.hypothesis.mechanism}")
        print()
    else:
        print("=" * 70)
        print("ℹ️  No significant causal relationships found in this run.")
        print("   Possible reasons:")
        print("   - Insufficient data for the domain")
        print("   - Variables may not have causal relationships")
        print("   - More domain-specific data sources may be needed")
        print("=" * 70)
        print()

    # Display metadata/insights
    if result.metadata:
        wish_list = result.metadata.get('wish_list', [])
        if wish_list:
            print("💡 Wish List (variables that would improve analysis):")
            for item in wish_list[:3]:
                unavailable = item.get('unavailable', [])
                if unavailable:
                    print(f"  - Missing: {', '.join(unavailable)}")
            print()

    print("=" * 70)
    print("✨ Discovery complete!")
    print(f"   Domain: {args.domain}")
    print(f"   Data sources used: {len(data_sources)}")
    print(f"   Variables analyzed: {len(discovered_vars)}")
    print(f"   Hypotheses tested: {len(result.hypotheses)}")
    print(f"   Significant findings: {len(significant)}")
    print()
    print("📝 Check agent_discovery.log for detailed logs.")
    print("=" * 70)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Discovery interrupted by user")
    except Exception as e:
        logger.exception("Error during discovery")
        print(f"\n❌ Error: {e}")
        print("Check agent_discovery.log for full details")

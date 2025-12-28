#!/usr/bin/env python3
"""
Example script showing how to use the Autonomous Causal Discovery Agent.

Run this after setting up your .env file:
    python example_agent_usage.py [--offline]
"""

import argparse

from agent.orchestrator import CausalDiscoveryAgent
from config.settings import get_settings
from loguru import logger

# Configure logging
logger.add("agent_discovery.log", rotation="10 MB")

def main():
    """Example usage of the causal discovery agent."""
    parser = argparse.ArgumentParser(description="Run the causal discovery agent demo.")
    parser.add_argument(
        "--offline",
        action="store_true",
        help="Run offline demo with bundled sample data (no external services)",
    )
    args = parser.parse_args()

    print("=" * 70)
    print("🤖 Autonomous Causal Discovery Agent - Example Usage")
    print("=" * 70)
    print()

    # Initialize agent
    print("📦 Initializing agent...")
    settings = get_settings()
    if args.offline:
        settings.offline_mode = True
        print("🔒 Offline mode enabled (using bundled sample data).")
    agent = CausalDiscoveryAgent(settings)
    print("✅ Agent initialized!")
    print()

    # Example 1: Crypto market analysis
    print("🔍 Example 1: Crypto Market Analysis")
    print("-" * 70)
    print("Domain: cryptocurrency")
    print("Query: Does social sentiment cause price movements?")
    print()

    result = agent.discover(
        domain="cryptocurrency",
        query="Does social sentiment cause price movements?",
        config={"thread_id": "example_crypto_1", "offline_mode": settings.offline_mode}
    )

    print(f"Status: {result.status}")
    print(f"Hypotheses generated: {len(result.hypotheses)}")
    print(f"Causal tests run: {len(result.results)}")
    print()

    # Display hypotheses
    if result.hypotheses:
        print("📋 Generated Hypotheses:")
        for i, hyp in enumerate(result.hypotheses, 1):
            print(f"  {i}. {hyp.cause.name} → {hyp.effect.name}")
            print(f"     Mechanism: {hyp.mechanism[:60]}...")
            print(f"     Confidence: {hyp.confidence:.2%}")
            print(f"     Methods: {[m.value for m in hyp.suggested_methods]}")
            print()

    # Display significant results
    significant = [r for r in result.results if r.is_significant]
    if significant:
        print("✅ Significant Causal Relationships Found:")
        for i, res in enumerate(significant, 1):
            print(f"  {i}. {res.hypothesis.cause.name} → {res.hypothesis.effect.name}")
            print(f"     Method: {res.method.value}")
            print(f"     P-value: {res.p_value:.4f}")
            print(f"     Confidence: {res.confidence:.2%}")
            if res.lead_lag:
                print(f"     Lead-lag: {res.lead_lag} time units")
            print()
    else:
        print("ℹ️  No significant causal relationships found in this run.")
        print("   The agent may need more data or refinement.")
        print()

    # Display discovered variables
    if result.discovered_variables:
        print(f"📊 Discovered Variables ({len(result.discovered_variables)}):")
        for var in result.discovered_variables[:5]:  # Show first 5
            print(f"  - {var.name} ({var.variable_type.value})")
        if len(result.discovered_variables) > 5:
            print(f"  ... and {len(result.discovered_variables) - 5} more")
        print()

    # Display data sources
    if result.active_data_sources:
        print(f"🔌 Integrated Data Sources ({len(result.active_data_sources)}):")
        for source in result.active_data_sources[:3]:  # Show first 3
            print(f"  - {source.name} ({source.source_type.value})")
            if source.kafka_topic:
                print(f"    Topic: {source.kafka_topic}")
        if len(result.active_data_sources) > 3:
            print(f"  ... and {len(result.active_data_sources) - 3} more")
        print()

    print("=" * 70)
    print("✨ Discovery complete! Check agent_discovery.log for details.")
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


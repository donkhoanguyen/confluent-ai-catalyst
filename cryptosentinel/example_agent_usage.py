#!/usr/bin/env python3
"""
Example script showing how to use the Autonomous Causal Discovery Agent.

Run this after setting up your .env file:
    python example_agent_usage.py
"""

from agent.orchestrator import CausalDiscoveryAgent
from config.settings import get_settings
from loguru import logger

# Configure logging
logger.add("agent_discovery.log", rotation="10 MB")

def show_agent_mermaid_ipython(agent):
    """
    Display the agent's orchestration flow as a rendered image using IPython.display,
    if run in a notebook or environment with IPython.
    """
    try:
        from IPython.display import Image, display
        import requests
        import base64
        mermaid = None

        # Try to fetch the Mermaid source from the agent
        if hasattr(agent, "mermaid"):
            mermaid = agent.mermaid() if callable(agent.mermaid) else agent.mermaid
        elif hasattr(agent, "as_mermaid"):
            mermaid = agent.as_mermaid() if callable(agent.as_mermaid) else agent.as_mermaid
        elif hasattr(agent, "graph") and hasattr(agent.graph, "to_mermaid"):
            mermaid = agent.graph.to_mermaid()
        if not mermaid:
            print("(No agent-generated Mermaid diagram available.)")
            return

        # Render via a public Mermaid live renderer API (for local/dev use only)
        # See: https://github.com/mermaid-js/mermaid.live
        url = "https://mermaid.ink/img/" + base64.urlsafe_b64encode(mermaid.encode("utf-8")).decode("utf-8")
        response = requests.get(url)
        if response.ok:
            display(Image(response.content))
        else:
            print("Couldn't render Mermaid diagram with online API.")
            print(mermaid)
    except ImportError:
        print("(IPython is not available. Install 'ipython' to view Mermaid graph inline.)")
        # Just print the Mermaid source
        # Try to print the diagram source for visual context
        if hasattr(agent, "mermaid"):
            mermaid = agent.mermaid() if callable(agent.mermaid) else agent.mermaid
            if mermaid:
                print("\nAgent Orchestration Flow (Mermaid source):\n")
                print(mermaid)
    except Exception as e:
        print("Could not display Mermaid image:", str(e))
        if hasattr(agent, "mermaid"):
            mermaid = agent.mermaid() if callable(agent.mermaid) else agent.mermaid
            if mermaid:
                print("\nAgent Orchestration Flow (Mermaid source):\n")
                print(mermaid)

def main():
    """Example usage of the causal discovery agent."""
    print("=" * 70)
    print("🤖 Autonomous Causal Discovery Agent - Example Usage")
    print("=" * 70)
    print()

    # Initialize agent
    print("📦 Initializing agent...")
    settings = get_settings()
    agent = CausalDiscoveryAgent(settings)
    print("✅ Agent initialized!")
    print()

    # Display the agent's internal mermaid graph as an image if possible
    show_agent_mermaid_ipython(agent)

    # Example 1: Crypto market analysis
    print("🔍 Example 1: Crypto Market Analysis")
    print("-" * 70)
    print("Domain: cryptocurrency")
    print("Query: Does social sentiment cause price movements?")
    print()

    result = agent.discover(
        domain="cryptocurrency",
        query="Does social sentiment cause price movements?",
        config={"thread_id": "example_crypto_1"}
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


"""
Agent Discovery Dashboard View
"""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from typing import List, Dict, Any
import httpx
from loguru import logger


def render_agent_view():
    """Render the agent discovery view."""
    # Main header
    st.markdown("""
    <div style="text-align: center; padding: 20px 0;">
        <h1>🤖 Autonomous Causal Discovery Agent</h1>
        <p style="color: #a0a0b0; font-size: 1.1rem;">
            Discover causal relationships automatically using AI-powered hypothesis generation
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    # Discovery Configuration - Now in the main page
    with st.expander("⚙️ Agent Configuration", expanded=True):
        col_c1, col_c2 = st.columns([1, 2])
        with col_c1:
            domain = st.text_input(
                "Domain", 
                value="cryptocurrency", 
                help="Domain to explore (e.g., cryptocurrency, healthcare, finance)"
            )
        with col_c2:
            query = st.text_area(
                "Research Question",
                value="Does social sentiment cause price movements?",
                help="What causal relationship do you want to discover?",
                height=68
            )
        
        if st.button("🚀 Start Discovery", type="primary", use_container_width=True):
            with st.spinner("Starting discovery... This may take a few minutes."):
                start_discovery(domain, query)
    
    st.markdown("---")
    
    # Main content
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("Discovery Status")
        status_placeholder = st.empty()
        
        st.subheader("Active Hypotheses")
        hypotheses_placeholder = st.empty()
        
        st.subheader("Causal Results")
        results_placeholder = st.empty()
    
    with col2:
        st.subheader("Discovery Metrics")
        metrics_placeholder = st.empty()
        
        st.subheader("Data Sources")
        sources_placeholder = st.empty()
    
    # Load and display data
    display_discovery_status(status_placeholder, metrics_placeholder)
    display_hypotheses(hypotheses_placeholder)
    display_results(results_placeholder)
    display_data_sources(sources_placeholder)


def start_discovery(domain: str, query: str):
    """Start a discovery process."""
    try:
        with httpx.Client(timeout=300.0, follow_redirects=True) as client:
            response = client.post(
                "http://localhost:8000/api/agent/discover",
                json={"domain": domain, "query": query},
            )
            response.raise_for_status()
            result = response.json()
            
            st.success(f"Discovery started! Status: {result['status']}")
            st.session_state['discovery_result'] = result
            
    except httpx.ConnectError as e:
        error_msg = f"Cannot connect to API server at http://localhost:8000. Is the API server running?"
        st.error(error_msg)
        logger.error(f"Connection error: {e}")
    except httpx.TimeoutException as e:
        error_msg = f"Request timed out. The discovery process may be taking longer than expected."
        st.error(error_msg)
        logger.error(f"Timeout error: {e}")
    except httpx.HTTPStatusError as e:
        error_msg = f"HTTP error {e.response.status_code}: {e.response.text}"
        st.error(error_msg)
        logger.error(f"HTTP error: {e}")
    except Exception as e:
        error_msg = f"Error starting discovery: {type(e).__name__}: {e}"
        st.error(error_msg)
        logger.error(f"Discovery error: {e}")


def display_discovery_status(status_placeholder, metrics_placeholder):
    """Display discovery status."""
    if 'discovery_result' not in st.session_state:
        status_placeholder.info("No active discovery. Start one from the sidebar.")
        metrics_placeholder.info("No metrics available.")
        return
    
    result = st.session_state['discovery_result']
    state = result.get('state', {})
    
    # Status
    status = state.get('status', 'UNKNOWN')
    status_color = {
        'COMPLETE': '🟢',
        'INITIALIZING': '🟡',
        'HYPOTHESIS_GENERATION': '🟡',
        'DATA_COLLECTION': '🟡',
        'CAUSAL_TESTING': '🟡',
        'ERROR': '🔴',
    }.get(status, '⚪')
    
    status_placeholder.markdown(f"""
    **Status:** {status_color} {status}
    
    **Iteration:** {state.get('iteration', 0)}
    
    **Domain:** {state.get('domain', 'N/A')}
    """)
    
    # Metrics
    metrics_placeholder.metric("Hypotheses", len(result.get('hypotheses', [])))
    metrics_placeholder.metric("Results", len(result.get('results', [])))
    metrics_placeholder.metric("Variables", len(state.get('discovered_variables', [])))


def display_hypotheses(hypotheses_placeholder):
    """Display active hypotheses."""
    if 'discovery_result' not in st.session_state:
        hypotheses_placeholder.info("No hypotheses yet.")
        return
    
    hypotheses = st.session_state['discovery_result'].get('hypotheses', [])
    
    if not hypotheses:
        hypotheses_placeholder.info("No hypotheses generated yet.")
        return
    
    for i, hyp in enumerate(hypotheses):
        with st.expander(f"Hypothesis {i+1}: {hyp.get('cause', {}).get('name', '?')} → {hyp.get('effect', {}).get('name', '?')}"):
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("**Cause:**", hyp.get('cause', {}).get('name', 'N/A'))
                st.write("**Effect:**", hyp.get('effect', {}).get('name', 'N/A'))
                st.write("**Mechanism:**", hyp.get('mechanism', 'N/A'))
            
            with col2:
                st.write("**Confidence:**", f"{hyp.get('confidence', 0):.2%}")
                st.write("**Methods:**", ", ".join([m for m in hyp.get('suggested_methods', [])]))
                st.write("**Confounders:**", len(hyp.get('potential_confounders', [])))


def display_results(results_placeholder):
    """Display causal inference results."""
    if 'discovery_result' not in st.session_state:
        results_placeholder.info("No results yet.")
        return
    
    results = st.session_state['discovery_result'].get('results', [])
    
    if not results:
        results_placeholder.info("No results yet. Discovery may still be running.")
        return
    
    # Create results DataFrame
    df = pd.DataFrame([
        {
            "Hypothesis": f"{r.get('cause', '?')} → {r.get('effect', '?')}",
            "Method": r.get('method', 'N/A'),
            "Significant": "✅" if r.get('is_significant') else "❌",
            "Confidence": f"{r.get('confidence', 0):.2%}",
            "P-value": f"{r.get('p_value', 1.0):.4f}" if r.get('p_value') else "N/A",
        }
        for r in results
    ])
    
    results_placeholder.dataframe(df, use_container_width=True)
    
    # Visualization
    if len(results) > 0:
        fig = px.bar(
            df,
            x="Hypothesis",
            y="Confidence",
            color="Significant",
            title="Causal Inference Results",
        )
        st.plotly_chart(fig, use_container_width=True)


def display_data_sources(sources_placeholder):
    """Display active data sources."""
    if 'discovery_result' not in st.session_state:
        sources_placeholder.info("No data sources yet.")
        return
    
    state = st.session_state['discovery_result'].get('state', {})
    sources = state.get('active_data_sources', [])
    
    if not sources:
        sources_placeholder.info("No data sources integrated yet.")
        return
    
    for source in sources:
        st.write(f"**{source.get('name', 'N/A')}**")
        st.write(f"Type: {source.get('source_type', 'N/A')}")
        if source.get('kafka_topic'):
            st.write(f"Topic: `{source['kafka_topic']}`")
        st.divider()


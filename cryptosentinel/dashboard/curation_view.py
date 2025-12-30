"""
Data Curation Dashboard View

Handles data discovery, pipeline integration, data collection, and readiness assessment.
"""

import streamlit as st
import plotly.graph_objects as go
import pandas as pd
from typing import List, Dict, Any, Optional
import httpx
from loguru import logger


def render_curation_view():
    """Render the data curation view."""
    # Main header
    st.markdown("""
    <div style="text-align: center; padding: 20px 0;">
        <h1>📊 Data Curation</h1>
        <p style="color: #a0a0b0; font-size: 1.1rem;">
            Discover data sources, integrate pipelines, and prepare datasets for causal analysis
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    # Curation Configuration - In the main page
    with st.expander("⚙️ Curation Configuration", expanded=True):
        col_c1, col_c2 = st.columns([1, 2])
        with col_c1:
            domain = st.text_input(
                "Domain", 
                value="cryptocurrency", 
                help="Domain to curate data for (e.g., cryptocurrency, healthcare, finance)"
            )
        with col_c2:
            curation_config = st.text_area(
                "Configuration (JSON)",
                value="{}",
                help="Optional curation configuration (JSON format)",
                height=68
            )
        
        col_btn1, col_btn2 = st.columns([1, 1])
        with col_btn1:
            if st.button("🚀 Start Curation", type="primary", use_container_width=True):
                with st.spinner("Starting curation... This may take a few minutes."):
                    start_curation(domain, curation_config)
        with col_btn2:
            if st.button("🔄 Refresh Status", use_container_width=True):
                refresh_curation_status(domain)
    
    st.markdown("---")
    
    # Main content in two columns
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("📋 Curation Status")
        status_placeholder = st.empty()
        
        st.subheader("📁 Discovered Data Sources")
        sources_placeholder = st.empty()
        
        st.subheader("📈 Dataset Readiness")
        readiness_placeholder = st.empty()
        
        st.subheader("🔍 Discovered Variables")
        variables_placeholder = st.empty()
    
    with col2:
        st.subheader("📊 Curation Metrics")
        metrics_placeholder = st.empty()
        
        st.subheader("💾 Datasets")
        datasets_placeholder = st.empty()
    
    # Load and display data
    display_curation_status(status_placeholder, metrics_placeholder)
    display_data_sources(sources_placeholder)
    display_readiness(readiness_placeholder)
    display_variables(variables_placeholder)
    display_datasets(datasets_placeholder)


def start_curation(domain: str, config_str: str):
    """Start a curation process."""
    try:
        # Parse config JSON
        import json
        config = {}
        if config_str.strip():
            try:
                config = json.loads(config_str)
            except json.JSONDecodeError as e:
                st.error(f"Invalid JSON configuration: {e}")
                return
        
        with httpx.Client(timeout=300.0, follow_redirects=True) as client:
            response = client.post(
                "http://localhost:8000/api/agent/curate",
                json={"domain": domain, "config": config},
            )
            response.raise_for_status()
            result = response.json()
            
            st.success(f"Curation started! Status: {result.get('status', 'UNKNOWN')}")
            st.session_state['curation_result'] = result
            
    except httpx.ConnectError as e:
        error_msg = f"Cannot connect to API server at http://localhost:8000. Is the API server running?"
        st.error(error_msg)
        logger.error(f"Connection error: {e}")
    except httpx.TimeoutException as e:
        error_msg = f"Request timed out. The curation process may be taking longer than expected."
        st.error(error_msg)
        logger.error(f"Timeout error: {e}")
    except httpx.HTTPStatusError as e:
        error_msg = f"HTTP error {e.response.status_code}: {e.response.text}"
        st.error(error_msg)
        logger.error(f"HTTP error: {e}")
    except Exception as e:
        error_msg = f"Error starting curation: {type(e).__name__}: {e}"
        st.error(error_msg)
        logger.error(f"Curation error: {e}")


def refresh_curation_status(domain: str):
    """Refresh curation status from API."""
    try:
        with httpx.Client(timeout=30.0, follow_redirects=True) as client:
            response = client.get(
                f"http://localhost:8000/api/agent/curate/status/{domain}",
            )
            response.raise_for_status()
            result = response.json()
            st.info(f"Status refreshed for domain: {domain}")
            # Update session state if we have active curations
            if result.get("active_curations"):
                st.session_state['curation_result'] = result.get("active_curations")[0]
    except Exception as e:
        st.warning(f"Could not refresh status: {e}")


def display_curation_status(status_placeholder, metrics_placeholder):
    """Display curation status."""
    if 'curation_result' not in st.session_state:
        status_placeholder.info("No active curation. Start one from the configuration above.")
        metrics_placeholder.info("No metrics available.")
        return
    
    result = st.session_state['curation_result']
    
    # Status
    status = result.get('status', 'UNKNOWN')
    status_color = {
        'COMPLETE': '🟢',
        'READY': '🟢',
        'INITIALIZING': '🟡',
        'DISCOVERING': '🟡',
        'INTEGRATING': '🟡',
        'COLLECTING': '🟡',
        'COMPUTING_READINESS': '🟡',
        'ERROR': '🔴',
    }.get(status, '⚪')
    
    status_placeholder.markdown(f"""
    **Status:** {status_color} {status}
    
    **Domain:** {result.get('domain', 'N/A')}
    
    **Readiness:** {result.get('readiness_score', 0.0):.1%}
    
    **Ready for Analysis:** {'✅ Yes' if result.get('is_ready', False) else '❌ No'}
    """)
    
    if result.get('error'):
        status_placeholder.error(f"**Error:** {result['error']}")
    
    # Metrics
    metrics_placeholder.metric("Data Sources", len(result.get('active_data_sources', [])))
    metrics_placeholder.metric("Variables", len(result.get('discovered_variables', [])))
    metrics_placeholder.metric("Datasets", len(result.get('datasets', [])))
    metrics_placeholder.metric("Row Count", f"{result.get('current_row_count', 0):,}")


def display_data_sources(sources_placeholder):
    """Display discovered data sources."""
    if 'curation_result' not in st.session_state:
        sources_placeholder.info("No data sources discovered yet.")
        return
    
    sources = st.session_state['curation_result'].get('active_data_sources', [])
    
    if not sources:
        sources_placeholder.info("No data sources discovered yet.")
        return
    
    for source in sources:
        with st.expander(f"📡 {source.get('name', 'Unknown Source')}"):
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("**Type:**", source.get('source_type', 'N/A'))
                st.write("**Status:**", source.get('status', 'N/A'))
                if source.get('kafka_topic'):
                    st.write("**Kafka Topic:**", f"`{source['kafka_topic']}`")
            
            with col2:
                if source.get('description'):
                    st.write("**Description:**", source.get('description'))
                if source.get('last_updated'):
                    st.write("**Last Updated:**", source.get('last_updated'))


def display_readiness(readiness_placeholder):
    """Display dataset readiness information."""
    if 'curation_result' not in st.session_state:
        readiness_placeholder.info("No readiness data available.")
        return
    
    result = st.session_state['curation_result']
    readiness_score = result.get('readiness_score', 0.0)
    is_ready = result.get('is_ready', False)
    current_rows = result.get('current_row_count', 0)
    min_rows = result.get('min_rows_required', 100)
    
    # Create readiness gauge
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=readiness_score * 100,
        number={"font": {"size": 36, "family": "Orbitron"}, "suffix": "%"},
        gauge={
            "axis": {"range": [0, 100], "tickcolor": "#a0a0b0"},
            "bar": {"color": "#00f5d4" if is_ready else "#fee440"},
            "bgcolor": "#1a1a2e",
            "bordercolor": "#2a2a3e",
            "steps": [
                {"range": [0, 50], "color": "rgba(247, 37, 133, 0.2)"},
                {"range": [50, 80], "color": "rgba(254, 228, 64, 0.2)"},
                {"range": [80, 100], "color": "rgba(0, 245, 212, 0.2)"},
            ],
            "threshold": {
                "line": {"color": "white", "width": 4},
                "thickness": 0.75,
                "value": 80
            }
        },
        title={"text": f"Readiness: {'✅ Ready' if is_ready else '⏳ Not Ready'}", 
               "font": {"size": 18, "color": "#a0a0b0"}},
    ))
    
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        height=250,
        margin=dict(l=20, r=20, t=50, b=20),
    )
    
    readiness_placeholder.plotly_chart(fig, use_container_width=True)
    
    # Row count info
    readiness_placeholder.markdown(f"""
    <div style="text-align: center; padding: 10px;">
        <p style="color: #a0a0b0;">
            <strong style="color: #00f5d4;">{current_rows:,}</strong> / {min_rows:,} rows required
        </p>
    </div>
    """, unsafe_allow_html=True)


def display_variables(variables_placeholder):
    """Display discovered variables."""
    if 'curation_result' not in st.session_state:
        variables_placeholder.info("No variables discovered yet.")
        return
    
    variables = st.session_state['curation_result'].get('discovered_variables', [])
    
    if not variables:
        variables_placeholder.info("No variables discovered yet.")
        return
    
    # Create DataFrame
    df = pd.DataFrame([
        {
            "Variable": v.get('name', 'N/A'),
            "Type": v.get('variable_type', 'N/A'),
            "Source": v.get('source', 'N/A'),
            "Description": v.get('description', 'N/A')[:50] + "..." if v.get('description') else 'N/A',
        }
        for v in variables
    ])
    
    variables_placeholder.dataframe(df, use_container_width=True, hide_index=True)


def display_datasets(datasets_placeholder):
    """Display available datasets."""
    if 'curation_result' not in st.session_state:
        datasets_placeholder.info("No datasets available.")
        return
    
    datasets = st.session_state['curation_result'].get('datasets', [])
    
    if not datasets:
        datasets_placeholder.info("No datasets available yet.")
        return
    
    for dataset in datasets:
        st.write(f"**{dataset.get('name', 'Unknown Dataset')}**")
        if dataset.get('path'):
            st.write(f"Path: `{dataset['path']}`")
        if dataset.get('row_count'):
            st.write(f"Rows: {dataset['row_count']:,}")
        st.divider()


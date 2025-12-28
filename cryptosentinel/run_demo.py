#!/usr/bin/env python3
"""
CryptoSentinel Demo Runner

Starts all services for the demo in a single process.
For development/demo purposes only - use separate processes in production.
"""

import asyncio
import subprocess
import sys
import time
from threading import Thread

from loguru import logger


def run_api():
    """Run the FastAPI server."""
    logger.info("Starting API server...")
    subprocess.run([
        sys.executable, "-m", "uvicorn",
        "api.main:app",
        "--host", "0.0.0.0",
        "--port", "8000",
        "--reload"
    ])


def run_streamlit():
    """Run the Streamlit dashboard."""
    logger.info("Starting Streamlit dashboard...")
    time.sleep(2)  # Wait for API to start
    subprocess.run([
        sys.executable, "-m", "streamlit",
        "run", "dashboard/app.py",
        "--server.port", "8501",
        "--server.headless", "true"
    ])


def main():
    """Run all services."""
    print("""
    ╔═══════════════════════════════════════════════════════════════╗
    ║                                                               ║
    ║   🔮 CryptoSentinel - Real-Time Causal Intelligence          ║
    ║                                                               ║
    ║   Starting services...                                        ║
    ║                                                               ║
    ║   • API Server:  http://localhost:8000                       ║
    ║   • Dashboard:   http://localhost:8501                       ║
    ║   • API Docs:    http://localhost:8000/docs                  ║
    ║                                                               ║
    ║   Press Ctrl+C to stop all services                          ║
    ║                                                               ║
    ╚═══════════════════════════════════════════════════════════════╝
    """)

    # Start services in threads
    api_thread = Thread(target=run_api, daemon=True)
    streamlit_thread = Thread(target=run_streamlit, daemon=True)

    try:
        api_thread.start()
        streamlit_thread.start()

        # Keep main thread alive
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        logger.info("Shutting down...")
        sys.exit(0)


if __name__ == "__main__":
    main()


#!/usr/bin/env python3
"""Test if agent view can be imported correctly."""

import sys
import os

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

print("Testing imports...")

try:
    from dashboard.agent_view import render_agent_view
    print("✅ SUCCESS: agent_view imported")
    print(f"   Function: {render_agent_view}")
except ImportError as e:
    print(f"❌ FAILED: ImportError - {e}")
    import traceback
    traceback.print_exc()
except Exception as e:
    print(f"❌ FAILED: {type(e).__name__} - {e}")
    import traceback
    traceback.print_exc()

print("\nTesting dashboard app import...")
try:
    import dashboard.app
    print("✅ SUCCESS: dashboard.app imported")
    print(f"   AGENT_VIEW_AVAILABLE: {dashboard.app.AGENT_VIEW_AVAILABLE}")
except Exception as e:
    print(f"❌ FAILED: {type(e).__name__} - {e}")
    import traceback
    traceback.print_exc()


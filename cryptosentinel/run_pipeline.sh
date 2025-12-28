#!/bin/bash
# CryptoSentinel Pipeline Runner
# Starts all services for the full pipeline

set -e

echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║                                                               ║"
echo "║   🔮 CryptoSentinel - Full Pipeline                          ║"
echo "║                                                               ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo ""

# Check if .env exists
if [ ! -f ".env" ]; then
    echo "❌ Error: .env file not found!"
    echo "   Copy env.example to .env and add your credentials."
    exit 1
fi

# Check Python
if ! command -v python3 &> /dev/null; then
    echo "❌ Error: Python 3 not found!"
    exit 1
fi

# Activate virtual environment if exists
if [ -d "venv" ]; then
    echo "📦 Activating virtual environment..."
    source venv/bin/activate
fi

# Function to cleanup background processes
cleanup() {
    echo ""
    echo "🛑 Shutting down services..."
    kill $(jobs -p) 2>/dev/null
    exit 0
}

trap cleanup SIGINT SIGTERM

echo "🚀 Starting services..."
echo ""

# Start API server
echo "  → Starting API server (port 8000)..."
python -m uvicorn api.main:app --host 0.0.0.0 --port 8000 &
sleep 2

# Start price producer
echo "  → Starting price producer..."
python -m producers.price_producer &
sleep 1

# Start Reddit producer
echo "  → Starting Reddit producer..."
python -m producers.reddit_producer &
sleep 1

# Start enrichment consumer
echo "  → Starting enrichment consumer..."
python -m consumers.enrichment_consumer &
sleep 1

# Start Streamlit dashboard
echo "  → Starting dashboard (port 8501)..."
python -m streamlit run dashboard/app.py --server.port 8501 &

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo ""
echo "✅ All services started!"
echo ""
echo "   📊 Dashboard:  http://localhost:8501"
echo "   🔌 API:        http://localhost:8000"
echo "   📚 API Docs:   http://localhost:8000/docs"
echo ""
echo "   Press Ctrl+C to stop all services"
echo ""
echo "═══════════════════════════════════════════════════════════════"

# Wait for all background processes
wait


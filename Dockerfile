FROM python:3.11-slim

WORKDIR /app

# Install build dependencies for faster compilation
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
# Use deploy-optimized requirements (excludes heavy causalml package)
COPY cryptosentinel/requirements-deploy.txt ./requirements.txt

# Install dependencies with optimizations for speed
# --prefer-binary: use pre-built wheels when available
# --no-cache-dir: don't cache to save space
RUN pip install --no-cache-dir --prefer-binary -r requirements.txt

# Copy application code
COPY cryptosentinel/ .

# Expose port (Railway will set PORT env var)
EXPOSE 8000

# Use PORT from environment variable
ENV PORT=8000

# Run the application
CMD python -m uvicorn api.main:app --host 0.0.0.0 --port ${PORT:-8000}


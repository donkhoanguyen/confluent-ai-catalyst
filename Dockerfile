FROM python:3.10-slim

WORKDIR /app

# Copy requirements first for better caching
COPY cryptosentinel/requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY cryptosentinel/ .

# Expose port (Railway will set PORT env var)
EXPOSE 8000

# Use PORT from environment variable
ENV PORT=8000

# Run the application
CMD python -m uvicorn api.main:app --host 0.0.0.0 --port ${PORT:-8000}


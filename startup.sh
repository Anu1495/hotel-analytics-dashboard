#!/bin/bash

# Initialize the virtual environment
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# Install dependencies from requirements.txt
if [ -f "requirements.txt" ]; then
    pip install --no-cache-dir -r requirements.txt
fi

# Set default port if not specified
PORT=${PORT:-8000}

# Start Streamlit server with explicit settings
streamlit run server.py \
    --server.port=$PORT \
    --server.address=0.0.0.0 \
    --server.headless=true \
    --server.enableCORS=false \
    --server.enableXsrfProtection=false

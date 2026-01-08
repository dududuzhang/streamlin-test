FROM python:3.11-slim

# Basic environment
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    STREAMLIT_SERVER_HEADLESS=true

WORKDIR /app

# Install system dependencies needed to build some Python packages
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        gcc \
        libpq-dev \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy application code
COPY . /app

# Streamlit default port
EXPOSE 8501

# Run the Streamlit app
CMD ["streamlit", "run", "uber_pickups.py", "--server.port", "8501", "--server.address", "0.0.0.0"]

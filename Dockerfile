# Use Python 3.11 slim image
FROM python:3.11-slim

# Base working directory
WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install system dependencies (for websockets, cryptography, etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libssl-dev \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# 👉 IMPORTANT: switch into src so `kalshifolder` is importable
WORKDIR /app/src

# Default command: use the logged streamer
CMD ["python", "-m", "kalshifolder.websocket.streamer_logged"]

FROM python:3.11-slim

WORKDIR /app

# Install system dependencies required for SQLite and RabbitMQ
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application files
COPY . ./app

# Default command
# CMD ["python", "main_consumer.py"]

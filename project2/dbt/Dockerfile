FROM python:3.11-slim

# Install system dependencies for dbt + ClickHouse
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt /tmp/requirements.txt

# Install dbt and dependencies
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Set working directory
WORKDIR /usr/app

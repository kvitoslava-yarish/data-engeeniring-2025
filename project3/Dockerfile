
FROM python:3.11-slim

RUN apt-get update && apt-get install -y curl iputils-ping wget && rm -rf /var/lib/apt/lists/*


# Install dependencies
RUN pip install duckdb pyarrow pyiceberg pandas

WORKDIR /lab
COPY data/ ./data/


FROM apache/superset:3.1.1

USER root
RUN pip install --no-cache-dir \
    clickhouse-connect==0.7.18 \
    clickhouse-sqlalchemy==0.3.2
USER superset

FROM python:3.11-slim

RUN apt-get update && apt-get install -y curl iputils-ping wget && rm -rf /var/lib/apt/lists/*


# Install dependencies
RUN pip install duckdb pyarrow pyiceberg pandas

WORKDIR /lab
COPY data/ ./data/



FROM apache/superset:3.1.1

USER root

RUN pip uninstall -y sqlalchemy alembic

RUN pip install --no-cache-dir \
    psycopg2-binary \
    clickhouse-connect \
    "sqlalchemy<2.0" \
    "alembic==1.13.1"

USER superset
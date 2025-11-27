FROM apache/airflow:2.9.3-python3.11

# Optional: install OS packages
USER root
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
RUN apt-get update && apt-get install -y curl iputils-ping wget && rm -rf /var/lib/apt/lists/*


# Switch back to airflow for Python package installs
USER airflow



# Install dbt + adapter under airflow user as well
RUN pip install --no-cache-dir dbt-core dbt-clickhouse
RUN pip install pyiceberg duckdb pyarrow

# Copy and install your Python deps
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
USER airflow

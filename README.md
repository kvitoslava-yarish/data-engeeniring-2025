# data-engineering-2025

## Objective
Fetch Estonian Youtube data to understand current trends for videomaking and channel popularity for advertising.

## Explanation

Our data dictionary is "data dictionary.pdf".

Our data sources are initial csv files for seeds (found inside data.zip) and YouTube API.

The scripts for sourcing our data is in /scripts.

Our Data Architecture diagram is inside /workflow_schema

Our star schema is "Star Schema.jpeg".

![Star Schema](images/StarSchema.jpeg)

Our demo SQL queries are in /sql.

## Architecture Overview

| Component | Role | Access/Port |
| :--- | :--- | :--- |
| **Airflow** | Orchestration & ETL (Data Ingestion) | `http://localhost:8080` |
| **ClickHouse** | OLAP Data Warehouse | HTTP: `http://localhost:8123` |
| **dbt** | Transformation (T in ELT) & Data Modeling (Silver/Gold layers) | Run-time container |
| **Tabix** | Web client for querying ClickHouse | `http://localhost:8124` |

---

## Project Launch Instructions

These instructions assume you have **Docker** and **Docker Compose** installed.

### 1. Prerequisites and Setup

1.  **Obtain a YouTube API Key:** The Airflow DAG (`channels_to_clickhouse.py` and `videos_to_clickhouse.py`) fetches data from the YouTube Data API. You must obtain an API key from Google Cloud Console.
2.  **Update the API Key:** Replace the placeholder key in `project2/airflow/dags/channels_to_clickhouse.py` and `project2/airflow/dags/videos_to_clickhouse.py` with your actual key.
    ```python
    API_KEY = "AIzaSyA3DBoFW0B6sFeF4JMtRkTWZ2Wd_LsrLXo" # 👈 REPLACE THIS VALUE
    ```
3.  **Prepare Directories:** Ensure all necessary local directories exist for volume mounting:
    ```bash
    mkdir -p project2/airflow/{dags,logs,plugins,include}
    mkdir -p project2/clickhouse/init
    ```
4.  **Check Configuration:** Review `project2/.env` to ensure the necessary environment variables for Airflow, ClickHouse, and dbt connections are correctly configured. The Airflow admin user is set to `admin` with password `admin`.

### 2. Launch Services

Start all containers in detached mode. This process includes service startup and critical initialization steps handled by dedicated containers (`airflow-init`, `dbt-init`).

```bash
docker compose up -d
```

### 3. Verification and Access

After launching, wait a few minutes for the initial database setup (`airflow-init`) and dbt project build (`dbt-init`) to complete successfully. The `airflow-scheduler` will only start after `airflow-init` completes successfully.

| Service | Access URL | Credentials |
| :--- | :--- | :--- |
| **Airflow Webserver** | `http://localhost:8080` | **User:** `admin`, **Password:** `admin` |
| **ClickHouse Web Client (Tabix)** | `http://localhost:8124` | (Connect to host: `clickhouse`, port: `8123`) |

## Visual Documentation Checklist

These are the key visual assets required to document the project:

### Data Pipeline (DAGs)
![DAGs Diagram](images/DAGs.jpeg)

### dbt Workflow
![dbt Workflow](images/dbt.jpeg)

[//]: # (### Results of analytical queries)

[//]: # ()
[//]: # (![Query1]&#40;images/1.jpeg&#41;)

[//]: # ()
[//]: # (![Query2]&#40;images/2.jpeg&#41;)


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

### 4. Build the Data Warehouse & Docs

You must run dbt to create the tables and persist the documentation into ClickHouse. Run this command:

```bash
docker exec -it dbt dbt build
```

Success Check: Ensure you see PASS for the models and tests.

Note: This step automatically stamps table/column descriptions into the database so they appear in OpenMetadata.


## OpenMetadata Setup (Docs & Quality)

OpenMetadata is used to catalog the data, view descriptions, and run data quality tests.

Access UI: http://localhost:8585

Login: admin@open-metadata.org / admin (admin / admin)

### A. Connect ClickHouse (Database)

Go to Settings > Services > Databases > Add New Service > ClickHouse.

For the Fact Tables:

- Service Name: clickhouse_facts

- Username: default

- Password: password

- Host & Port: clickhouse:8123  (Do NOT use localhost)

- Database Schema: youtube_dbt

- Ingestion: Click Add Ingestion > Deploy & Run.

For the Dimension Tables:

- Repeat the steps above but name it clickhouse_dims.

- Database Schema: youtube_gold

- Ingestion: Click Add Ingestion > Deploy & Run.

### B. Connect Superset (Dashboard)

Go to Settings > Services > Dashboards > Add New Service > Superset.

- Service Name: Superset

- Username: admin

- Password: admin

- Host Port: http://superset:8088 (Do NOT use localhost)

- Provider: db

- Ingestion: Click Add Ingestion > Deploy & Run.

## Data Quality Results

The following data quality tests have been implemented and verified in OpenMetadata:

1. Uniqueness: Checked on Surrogate Keys (video_sk, channel_sk) in dimension tables.

2. Not Null: Checked on Foreign Keys in fact tables.

3. Data Validity: Checked that view_count is non-negative (Between 0 and Max).


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

### Openmetadata tests
![dbt Workflow](images/metadata_results.png)




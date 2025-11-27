import duckdb
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from datetime import datetime
import os
from clickhouse_driver import Client
from pyiceberg.catalog.rest import RestCatalog


CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "raw_youtube")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_TCP_PORT = int(os.getenv("CLICKHOUSE_TCP_PORT", "9000"))


def _clickhouse_client() -> Client:
    return Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_TCP_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
    )


def ingest_to_iceberg():
    conn = duckdb.connect("lab.duckdb")
    conn.install_extension("httpfs")
    conn.load_extension("httpfs")

    # Configure S3 (MinIO)
    conn.sql("""
    SET s3_region='us-east-1';
    SET s3_url_style='path';
    SET s3_endpoint='minio:9000';
    SET s3_access_key_id='minioadmin';
    SET s3_secret_access_key='minioadmin';
    SET s3_use_ssl=false;
    """)

    # Load CSVs from MinIO into DuckDB
    tables = ['estonian_youtubers', 'videos_metadata']

    for table in tables:
        s3_path = f's3://data-bucket/{table}.csv'
        conn.sql(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM read_csv('{s3_path}')")

    print("✅ DuckDB tables created from MinIO CSVs")

    # Create Iceberg catalog
    def _iceberg_catalog():
        catalog = RestCatalog(
            name="rest",
            **{
                "uri": "http://iceberg_rest:8181",
                "warehouse": "s3://data-bucket/",
                "s3.endpoint": "http://minio:9000",
                "s3.access-key-id": "minioadmin",
                "s3.secret-access-key": "minioadmin",
                "s3.path-style-access": "true",
            }
        )
        return catalog
    
    catalog = _iceberg_catalog()
    
    namespace = "bronze"
    table_name = "estonian_youtubers"
    
    # Create namespace if it doesn't exist
    try:
        catalog.create_namespace(namespace)
        print(f"Namespace '{namespace}' created")
    except NamespaceAlreadyExistsError:
        print(f"Namespace '{namespace}' already exists")
    
    # Fetch data as PyArrow Table
    arrow_table = conn.sql("SELECT * FROM estonian_youtubers").arrow()
    print(f"Arrow table has {len(arrow_table)} rows")
    
    # Create or append to Iceberg table
    table_identifier = f"{namespace}.{table_name}"
    
    try:
        table = catalog.load_table(table_identifier)
        print(f"Table '{table_identifier}' exists, appending data")
        table.append(arrow_table)
    except NoSuchTableError:
        table = catalog.create_table(
            identifier=table_identifier,
            schema=arrow_table.schema,
        )
        table.append(arrow_table)
        print(f"Table '{table_identifier}' created and data ingested")
    
    # Show table info
    print(f"\n Iceberg Table Statistics:")
    print(f"   Location: {table.location()}")
    print(f"   Metadata: {table.metadata_location}")
    print(f"   Snapshots: {len(list(table.snapshots()))}")
    print(f"   Current snapshot: {table.current_snapshot().snapshot_id}")
    
    # CRITICAL: Check if data files exist
    print(f"\n🔍 Checking data files in current snapshot:")
    snapshot = table.current_snapshot()
    if snapshot:
        scan = table.scan()
        data_files = []
        try:
            for task in scan.plan_files():
                file_path = task.file.file_path
                data_files.append(file_path)
                print(f"   📄 {file_path}")
        except Exception as e:
            print(f"   Error scanning files: {e}")
        
        if not data_files:
            print("   WARNING: No data files found!")
        else:
            print(f"   Found {len(data_files)} data file(s)")
    else:
        print("   No snapshot available!")
    
    # Now try ClickHouse
    print(f"\n🔌 Testing ClickHouse connectivity to MinIO...")
    ch_client = _clickhouse_client()
    ch_table_name = table_identifier.replace(".", "_")
    
    # Test 1: Can ClickHouse list the bucket?
    print("\n1️⃣ Test: List MinIO bucket")
    try:
        result = ch_client.execute("""
            SELECT * FROM s3(
                'http://minio:9000/data-bucket/*',
                'minioadmin',
                'minioadmin'
            ) LIMIT 1
        """)
        print("   Can list bucket")
    except Exception as e:
        print(f"   Cannot list bucket: {e}")
    
    # Test 2: Can ClickHouse see the specific Parquet files?
    print("\n2️ Test: Read Parquet files")
    s3_pattern = f"http://minio:9000/data-bucket/{namespace}/{table_name}/data/*.parquet"
    print(f"   Pattern: {s3_pattern}")
    
    try:
        result = ch_client.execute(f"""
            SELECT * FROM s3(
                '{s3_pattern}',
                'minioadmin',
                'minioadmin',
                'Parquet'
            ) LIMIT 1
        """)
        print(f"   Can read Parquet files: {result}")
    except Exception as e:
        print(f"   Cannot read Parquet files: {e}")
    
    # Test 3: Try with explicit structure
    print("\n3️⃣ Test: Read with explicit schema")
    try:
        # Get schema from arrow table
        cols = arrow_table.schema.names
        
        result = ch_client.execute(f"""
            SELECT count(*) FROM s3(
                '{s3_pattern}',
                'minioadmin',
                'minioadmin',
                'Parquet'
            )
        """)
        print(f"   Row count: {result[0][0]}")
    except Exception as e:
        print(f"   Failed: {e}")
    
    # Test 4: Create the actual ClickHouse table
    print(f"\n4️⃣ Creating ClickHouse table: {table_identifier}")
    try:
        ch_client.execute(f"CREATE DATABASE IF NOT EXISTS {namespace}")

        ch_client.execute(f"DROP TABLE IF EXISTS {namespace}.{table_name}")
        
        create_sql = f"""
        CREATE TABLE {namespace}.{table_name}
        ENGINE = MergeTree()
        ORDER BY tuple()
        AS SELECT * FROM s3(
            '{s3_pattern}',
            'minioadmin',
            'minioadmin',
            'Parquet'
        )
        """
        
        print(f"   SQL: {create_sql}")
        ch_client.execute(create_sql)
        
        count = ch_client.execute(f"SELECT count(*) FROM {namespace}.{table_name}")[0][0]
        print(f"   Table created with {count:,} rows")
        
        
    except Exception as e:
        print(f"   Failed to create table: {e}")
        
        # Debug: Check what files actually exist in MinIO
        print("\n🔍 Debug: Checking MinIO directly...")
        try:
            # List all Parquet files
            list_result = ch_client.execute(f"""
                SELECT _path FROM s3(
                    'http://minio:9000/data-bucket/{namespace}/{table_name}/data/*',
                    'minioadmin',
                    'minioadmin'
                )
            """)
            print(f"   Files found: {list_result}")
        except Exception as e2:
            print(f"   Cannot list files: {e2}")
    
    conn.close()
    return table_identifier


if __name__ == "__main__":
    ingest_to_iceberg()
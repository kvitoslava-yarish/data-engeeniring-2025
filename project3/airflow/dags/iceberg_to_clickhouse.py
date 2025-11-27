from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
from minio import Minio
from glob import glob
import os
from minio.error import S3Error
import json



BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "data"))


sys.path.append('scripts')


def init_minio_buckets():
    """Initialize MinIO bucket and upload CSVs"""

    client = Minio(
        "minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

    bucket = "data-bucket"

    # 1. Create bucket if missing
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        print(f"Bucket '{bucket}' created")
        
        # Set bucket policy to allow read/write access
        # This is important for Iceberg REST catalog to access the bucket
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": ["*"]},
                    "Action": [
                        "s3:GetBucketLocation",
                        "s3:ListBucket",
                        "s3:ListBucketMultipartUploads"
                    ],
                    "Resource": [f"arn:aws:s3:::{bucket}"]
                },
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": ["*"]},
                    "Action": [
                        "s3:GetObject",
                        "s3:PutObject",
                        "s3:DeleteObject",
                        "s3:AbortMultipartUpload",
                        "s3:ListMultipartUploadParts"
                    ],
                    "Resource": [f"arn:aws:s3:::{bucket}/*"]
                }
            ]
        }
        
        try:
            client.set_bucket_policy(bucket, json.dumps(policy))
            print(f"Bucket policy set for '{bucket}'")
        except S3Error as e:
            print(f"Could not set bucket policy: {e}")
            
    else:
        print(f"Bucket '{bucket}' already exists")

    # 2. Upload CSV files
    csv_files = glob(os.path.join(DATA_DIR, "*.csv"))

    if not csv_files:
        print("No CSV files found in", DATA_DIR)
        return

    print(f"\n Uploading {len(csv_files)} CSV file(s)...")
    
    for file_path in csv_files:
        object_name = os.path.basename(file_path)
        
        try:
            # Check if file already exists
            try:
                stat = client.stat_object(bucket, object_name)
                file_size = os.path.getsize(file_path)
                
                # Only upload if file size is different (simple change detection)
                if stat.size == file_size:
                    print(f"Skipped (unchanged): {object_name}")
                    continue
            except S3Error:
                pass  # File doesn't exist, will upload
            
            # Upload the file
            client.fput_object(bucket, object_name, file_path)
            print(f"⬆Uploaded: {object_name}")
            
        except Exception as e:
            print(f"Failed to upload {object_name}: {e}")

    print("\n🎉 MinIO initialization complete!")
    
    # 3. List bucket contents for verification
    try:
        objects = client.list_objects(bucket)
        print(f"\nBucket '{bucket}' contents:")
        for obj in objects:
            size_mb = obj.size / (1024 * 1024)
            print(f"   • {obj.object_name} ({size_mb:.2f} MB)")
    except Exception as e:
        print(f"Could not list bucket contents: {e}")



default_args = {
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'iceberg_bronze_layer',
    default_args=default_args,
    description='Ingest data into Apache Iceberg bronze layer',
    schedule_interval='@daily',  # Run daily
    catchup=False,
)

# Task 1: Ingest data to Iceberg

init_minio = PythonOperator(
        task_id='init_minio',
        python_callable=init_minio_buckets,
        dag=dag,
    )

ingest_task = BashOperator(
    task_id='ingest_iceberg_to_clickhouse',
    bash_command='python /opt/airflow/scripts/iceberg_ingest.py',
    dag=dag,
)


init_minio >> ingest_task
import os
import urllib.request
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


PROJECT_ID = "taxi-data-project-489408"
BUCKET_NAME = f"nyc-taxi-data-bucket-{PROJECT_ID}"
DATASET_NAME = "taxi_dataset"
TABLE_NAME = "raw_taxi_trips"


LOCAL_FILE_PATH = os.path.join(os.getcwd(), "yellow_tripdata_2024-01.parquet")
GCS_FILE_PATH = "raw/yellow_tripdata_2024-01.parquet"
DATA_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), "google_credentials.json")

def download_taxi_data():
    """NYC Taxi ka Parquet data download karne ka function"""
    print(f"Downloading data from {DATA_URL}...")
    urllib.request.urlretrieve(DATA_URL, LOCAL_FILE_PATH)
    print(f"Data downloaded successfully at {LOCAL_FILE_PATH}")


default_args = {
    "owner": "ayush",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="nyc_taxi_batch_pipeline",
    default_args=default_args,
    description="Batch ELT pipeline for NYC Taxi Data",
    schedule="@monthly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["data-engineering", "gcp"],
) as dag:
    
    download_data_task = PythonOperator(
        task_id="download_taxi_data",
        python_callable=download_taxi_data,
    )


    create_bucket_task = GCSCreateBucketOperator(
        task_id="create_gcs_bucket",
        bucket_name=BUCKET_NAME,
        project_id=PROJECT_ID,
        location="US",
    )

    
    upload_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id="upload_file_to_gcs",
        src=LOCAL_FILE_PATH,
        dst=GCS_FILE_PATH,
        bucket=BUCKET_NAME,
    )

    
    create_bq_dataset_task = BigQueryCreateEmptyDatasetOperator(
        task_id="create_bq_dataset",
        dataset_id=DATASET_NAME,
        project_id=PROJECT_ID,
        location="US",
    )

    
    load_to_bq_task = GCSToBigQueryOperator(
        task_id="load_gcs_to_bq",
        bucket=BUCKET_NAME,
        source_objects=[GCS_FILE_PATH],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}",
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE", 
        autodetect=True, 
    )


    download_data_task >> create_bucket_task >> upload_to_gcs_task
    upload_to_gcs_task >> create_bq_dataset_task >> load_to_bq_task
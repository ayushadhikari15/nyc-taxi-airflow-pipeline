# NYC Taxi Data Engineering Pipeline (Airflow & GCP)

## 📌 Overview
An end-to-end ELT (Extract, Load, Transform) pipeline built with Apache Airflow to automate the flow of New York City Taxi trip data into Google Cloud Platform.

## 🛠️ Tech Stack
- **Orchestration:** Apache Airflow
- **Cloud Provider:** Google Cloud Platform (GCP)
- **Data Lake:** Google Cloud Storage (GCS)
- **Data Warehouse:** BigQuery
- **Language:** Python 3.12

## 🏗️ Architecture
1. **Extraction:** Downloads NYC Taxi Parquet data using Python.
2. **Staging:** Creates a GCS bucket and uploads the raw data file.
3. **Loading:** Creates a BigQuery dataset and loads the GCS data into a BigQuery table for analysis.

## 🚀 Key Features
- **Security:** Integrated GCP Service Account via Airflow Connections.
- **Error Handling:** Configured retries for robust cloud operations.
- **Automation:** Fully scheduled batch processing DAG.

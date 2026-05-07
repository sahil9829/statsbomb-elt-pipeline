# Sports Analytics ELT Pipeline ⚽️

## Project Overview
An automated, end-to-end ELT (Extract, Load, Transform) pipeline designed to extract open-source sports data, process it into analytical formats, and serve it via a serverless SQL query engine. 

## 🏗️ Architecture
1. **Orchestration:** Dockerized **Apache Airflow** managing task scheduling, dependencies, and retry logic.
2. **Extraction (Python/Requests):** Connects to the StatsBomb API, extracting JSON payloads and staging them locally.
3. **Data Lake (AWS S3):** Secure cloud storage layer handling both raw and processed zones, utilizing IAM roles for access.
4. **Transformation (Pandas/PyArrow):** Flattens nested JSON structures and converts them into highly compressed, columnar **Parquet** files for optimized querying.
5. **Serving Layer (AWS Athena):** Serverless interactive query service allowing direct SQL querying on the S3 Data Lake via schema-on-read DDL.

## 🛠️ Tech Stack
* **Language:** Python 3.8
* **Infrastructure:** Docker, Docker Compose
* **Orchestration:** Apache Airflow
* **Cloud (AWS):** S3 (Data Lake), Athena (Query Engine), IAM
* **Data Processing:** Pandas, PyArrow (Parquet)

## 🚀 How to Run Locally
1. Clone the repository.
2. Create a `.env` file with your `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `S3_BUCKET_NAME`.
3. Run `docker compose up airflow-init` to build the Postgres database.
4. Run `docker compose up -d` to spin up the Airflow webserver and scheduler.
5. Access the UI at `localhost:8080` and trigger the `statsbomb_daily_extraction` DAG.
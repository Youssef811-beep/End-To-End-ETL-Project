# 🚀 Airflow + Databricks ETL Pipeline (Production-Style)

End-to-end data pipeline using Apache Airflow (Docker + CeleryExecutor) and Databricks with Bronze–Silver–Gold architecture.
## 🏗 Architecture

Airflow (Scheduler + Worker)
        ↓
Redis Queue
        ↓
Databricks API (Token Auth)
        ↓
Databricks Jobs (Bronze → Silver → Gold)
        ↓
Delta Lake Storage
# 🚀 Airflow + Databricks ETL Pipeline (Bronze–Silver–Gold)

## 📌 Project Explanation

This project demonstrates an end-to-end data engineering pipeline designed to simulate real-world production workflows using Apache Airflow and Databricks.

The primary objective of this project is to orchestrate scalable ETL (Extract, Transform, Load) processes by integrating a workflow orchestration tool (Airflow) with a cloud-based data processing platform (Databricks).

### 🔄 Workflow Overview

The pipeline follows a layered architecture inspired by modern data engineering practices:

* **Bronze Layer (Raw Ingestion):**
  Raw data is ingested from source files into Databricks using PySpark. Additional metadata such as ingestion timestamp, source file name, and batch ID is added to track data lineage.

* **Silver Layer (Data Cleaning & Transformation):**
  Data is processed to remove duplicates, handle null values, and standardize formats. This layer ensures that the dataset is clean, structured, and ready for downstream consumption.

* **Gold Layer (Business Aggregation):**
  Aggregated datasets are created to generate business insights, such as total orders by status. This layer is optimized for analytics and reporting.

### ⚙️ Orchestration using Airflow

Apache Airflow is used to orchestrate the pipeline using Directed Acyclic Graphs (DAGs):

* Multiple tasks are defined to trigger Databricks jobs sequentially.
* Task dependencies ensure correct execution order (Bronze → Silver → Gold).
* Cron-based scheduling is configured to automate pipeline execution on weekdays at 10 PM IST.
* Retry mechanisms are implemented to handle transient failures.

### 🔗 Integration with Databricks

Airflow integrates with Databricks using:

* **Databricks Connection:** Configured with workspace URL and personal access token.
* **Job IDs:** Each Databricks notebook is deployed as a job and triggered via Airflow using `DatabricksRunNowOperator`.

This enables Airflow to remotely execute PySpark notebooks on Databricks compute.

### 🐳 Docker-based Deployment

The entire Airflow environment is containerized using Docker, including:

* Scheduler
* Worker (CeleryExecutor)
* Redis (message broker)
* Postgres (metadata database)

This setup simulates a distributed execution environment similar to production systems.

### 🚀 Key Highlights

* Built a modular and scalable ETL pipeline using industry-standard tools
* Implemented distributed task execution using CeleryExecutor
* Integrated Airflow with Databricks via REST API and token-based authentication
* Designed a layered data architecture (Bronze, Silver, Gold)
* Automated workflow scheduling and dependency management
* Debugged and resolved real-world issues related to worker health and task execution

### 🎯 Conclusion

This project reflects a production-style data pipeline where Airflow acts as the orchestration layer and Databricks performs large-scale data processing. It demonstrates strong understanding of workflow automation, distributed systems, and modern data engineering practices.

## 📌 Overview

This project demonstrates an end-to-end data engineering pipeline using Apache Airflow and Databricks. It follows a Bronze–Silver–Gold architecture to process and transform data using PySpark.

## 🏗 Architecture

Airflow (Docker + CeleryExecutor) orchestrates Databricks jobs using REST API with token authentication.

Pipeline Flow:
Airflow → Redis → Worker → Databricks Job → PySpark Notebook → Delta Tables

## ⚙️ Tech Stack

* Apache Airflow (CeleryExecutor)
* Docker
* Databricks (PySpark, Delta Lake)
* Redis & Postgres
* Python

## 🔄 Pipeline Stages

### 1. Bronze Layer

* Raw data ingestion from CSV
* Add metadata (ingestion time, batch_id)

### 2. Silver Layer

* Data cleaning (null handling, deduplication)
* Data standardization

### 3. Gold Layer

* Aggregations for business insights

## 📅 Scheduling

* Cron-based schedule: Weekdays at 10 PM IST
* Retry mechanism enabled for fault tolerance

## 🔗 Airflow DAG

* Multi-task DAG with dependencies
* DatabricksRunNowOperator used for triggering jobs

## 🔐 Connection Setup

* Databricks workspace connected via token-based authentication
* Configured using Airflow connections

## 🐳 Docker Setup

```bash
docker compose up -d
```


## 📊 Key Features

* Distributed execution using CeleryExecutor
* Real-world orchestration using Airflow
* Modular ETL pipeline
* Error handling and retries

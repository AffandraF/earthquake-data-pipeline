# Earthquake Data Pipeline

![Python](https://img.shields.io/badge/Python-3.10%20%7C%203.11-blue)
![Airflow](https://img.shields.io/badge/Orchestrator-Apache%20Airflow-red)
![Spark](https://img.shields.io/badge/Engine-PySpark-orange)
![Postgres](https://img.shields.io/badge/Data%20Warehouse-PostgreSQL-blue)
![Docker](https://img.shields.io/badge/Container-Docker-2496ED)

An end-to-end data engineering project that processes global historical earthquake data. This pipeline transforms raw data into analytical insights using a **Medallion Architecture**. This repository provides two alternative data processing engines:
1. **Pandas Pipeline**: Orchestrated using **Apache Airflow** and running entirely within **Docker** containers.
2. **PySpark Pipeline**: Designed for large-scale distributed processing, running locally (or ready to be deployed to a Spark cluster).

---

## Repository Structure

```text
├── airflow/              # Docker configuration & Apache Airflow DAGs
│   ├── dags/             # Airflow DAGs (eq_dag.py)
│   ├── Dockerfile        # Dockerfile for custom Airflow image
│   └── requirements.txt  # Python package dependencies for Airflow
├── data/                 # Data storage directory (raw CSV & Parquet files)
├── src/                  # Main pipeline source code
│   ├── pandas/           # Pandas pipeline source code
│   │   ├── Bronze.py     # Data ingestion using Pandas
│   │   ├── Silver.py     # Transformation & enrichment using Pandas
│   │   └── Gold.py       # Aggregation & target loading using Pandas
│   └── pyspark/          # PySpark pipeline source code (flattened layout)
│       ├── bronze.py     # Ingestion (Bronze stage) via PySpark
│       ├── silver.py     # Transformation & enrichment (Silver stage) via PySpark
│       ├── gold.py       # Aggregation (Gold stage) via PySpark
│       ├── postgres_loader.py # PostgreSQL database loader (JDBC)
│       ├── settings.py   # Database & general project settings
│       ├── spark_session.py # Spark Session helper
│       ├── validate.py   # Script to validate Pandas vs PySpark outputs
│       ├── main.py       # PySpark CLI entry point
│       ├── databricks_notebook.py # Databricks notebook execution template
│       └── flow.md       # PySpark local setup guide
└── docker-compose.yaml   # Service orchestration (Postgres & Airflow)
```

---

## Data Architecture (Medallion)

The system implements a **Medallion Architecture** to guarantee data quality at every stage:

* **Bronze Layer (Raw):** Ingests raw data from the [Kaggle Earthquake Dataset](https://www.kaggle.com/datasets/warcoder/earthquake-dataset) and stores it as Parquet format without schema modifications.
* **Silver Layer (Cleansed & Enriched):**
    * Standardizes column names and data types.
    * **Deduplication & Validation:** Removes duplicate records using *MD5 Hashing* to create a unique ID, and filters out invalid coordinates/magnitudes.
    * **Enrichment:** Fills missing location details (Country and Continent) using coordinate-based reverse geocoding.
* **Gold Layer (Aggregated):**
    * Classifies earthquake severity based on the magnitude scale (Minor, Light, Moderate, Strong).
    * Creates country-level aggregate statistics (earthquake frequency, average magnitude, maximum/minimum magnitudes, and potential tsunami counts).

---

## Tech Stack & Dependencies

* **Language:** Python 3.10 / 3.11
* **Orchestration:** Apache Airflow 2.11.0 (Slim)
* **Big Data Engine:** Apache Spark / PySpark 3.x
* **Database:** PostgreSQL 15
* **Infrastructure:** Docker & Docker Compose
* **Key Libraries:** `pyspark`, `pandas`, `reverse_geocoder`, `pycountry-convert`, `sqlalchemy`, `psycopg2-binary`

---

## Main Transformation Logic

### 1. Deduplication (Idempotency) & Unique ID
To prevent duplicate records on pipeline reruns, a synthetic *Primary Key* is generated using MD5 hashing of key attribute combinations:
* **Pandas:**
  ```python
  base = f"{row.magnitude}|{row.date_time}|{row.latitude}|{row.longitude}"
  return hashlib.md5(base.encode()).hexdigest()
  ```
* **PySpark:**
  ```python
  df.withColumn("id", F.md5(F.concat_ws("|", F.col("magnitude").cast("string"), ...)))
  ```

### 2. Location Enrichment (Reverse Geocoding)
Fills in missing country and continent names based on latitude and longitude coordinates:
* **Logic:** `(Lat, Lon) -> Country Code (ISO) -> Country Name & Continent`
* **Edge Cases:** Custom handling for specific country codes (e.g., 'TL' for Timor Leste).
* **PySpark Implementation:** Utilizes a *Spark User Defined Function* (UDF) to distribute the coordinate lookup logic across worker nodes efficiently.

### 3. Severity Classification
Classifies earthquakes for risk assessment using the following criteria:
* **Minor:** < 3.0
* **Light:** 3.0 - 4.9
* **Moderate:** 5.0 - 6.9
* **Strong:** >= 7.0

---

## Target Schema (PostgreSQL)

The final aggregated table in the Gold layer (`gold.country_stats`) is ready for visualization:

| Column | Data Type | Description |
| :--- | :--- | :--- |
| `country` | VARCHAR (PK) | Country name |
| `eq_count` | INT | Total number of recorded earthquakes |
| `avg_magnitude` | FLOAT | Average earthquake magnitude |
| `max_magnitude` | FLOAT | Strongest recorded earthquake magnitude |
| `min_magnitude` | FLOAT | Weakest recorded earthquake magnitude |
| `tsunami_count` | INT | Number of events with tsunami potential |

---

## Getting Started

### A. Pandas Pipeline (via Airflow & Docker)

Ensure that **Docker Desktop** is running on your machine.

1. **Clone the repository**
   ```bash
   git clone https://github.com/AffandraF/earthquake-data-pipeline.git
   cd earthquake-data-pipeline
   ```

2. **Start Services**
   Run the following command to build images and spin up the Airflow & PostgreSQL containers:
   ```bash
   docker compose up -d --build
   ```

3. **Trigger & Monitor Pipeline**
   * Open your browser and navigate to the Airflow UI at `http://localhost:8080` (Default credentials: `admin` / `admin`).
   * Turn on and trigger the `earthquake_data_pipeline` DAG.

4. **Query Final Data**
   Connect to the PostgreSQL database and run the following queries:
   ```sql
   SELECT * FROM gold.gold_data LIMIT 10;
   SELECT * FROM gold.country_stats;
   ```

---

### B. PySpark Pipeline (Local Execution)

To run the ETL pipeline locally using PySpark, follow these steps (For a detailed setup guide, refer to [flow.md](file:///c:/Affandra/Data%20Engineer/_Porto/earthquake-data-pipeline/src/pyspark/flow.md)):

1. **Windows Prerequisites**:
   * Install Java JDK (version 8, 11, or 17) and configure the `JAVA_HOME` environment variable.
   * Download `winutils.exe` for Hadoop 3.x, place it in `C:\hadoop\bin`, and configure the `HADOOP_HOME` environment variable.

2. **Setup Anaconda Environment**:
   ```bash
   conda create -n eq_pyspark python=3.10 -y
   conda activate eq_pyspark
   pip install pyspark psycopg2-binary reverse_geocoder pycountry-convert
   ```

3. **Start PostgreSQL Database**:
   Ensure your database container is up and running:
   ```bash
   docker compose up -d postgres
   ```

4. **Run the PySpark Pipeline**:
   * To run the complete pipeline (*all steps*):
     ```bash
     python -m src.pyspark.main --step all --source-path ./data/earthquake_data.csv --bronze-path ./data/bronze/earthquake_data.parquet
     ```
   * To run a specific stage (options: `bronze`, `silver`, `gold`):
     ```bash
     python -m src.pyspark.main --step silver --bronze-path ./data/bronze/earthquake_data.parquet
     ```

---

### C. Data Validation & Comparison (Pandas vs PySpark)

An automated validation script is provided to ensure that the outputs from both the Pandas and PySpark pipelines are identical in structure and values:

```bash
python -m src.pyspark.validate
```

This script compares:
* **Row counts** across each medallion layer.
* **Data schema** (column names and data types).
* **Null value metrics** (null percentages/counts).
* **Row-by-row content equivalence** to identify any discrepancies.

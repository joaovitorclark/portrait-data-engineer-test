# 📄 Project: Data Pipeline with Airflow, PostgreSQL, and Medallion Architecture

## 📦 Project Structure

```plaintext
portrait-data-engineer-test/
├── airflow-docker/
│   ├── dags/
│   │   ├── create_gold_tables_from_parquet.py
│   │   ├── export_tables_to_parquet.py
│   │   ├── move_bronze_to_silver_partitioned.py
│   │   ├── move_landing_to_bronze_partitioned.py
│   │   ├── orchestrate_full_pipeline.py
│   │   └── silver_to_gold_feature_engineering.py
│   └── docker-compose.yaml
├── app/
│   └── bucket/
│       ├── landing/
│       ├── bronze/
│       ├── silver/
│       └── gold/
├── load_data.py
├── requirements.txt
└── README.md  <-- (this file)
```

## 💡 Technology Stack
- **Apache Airflow**
- **PostgreSQL**
- **Python 3.9+**
- **Docker / Docker Compose**

## 🛠️ Setup Instructions

### Step 1: Start PostgreSQL Container
```bash
docker-compose up -d
```
PostgreSQL Configuration:
- Host: `host.docker.internal`
- Port: `5432`
- Database: `healthcare`
- Username: `postgres`
- Password: `postgres`

### Step 2: Populate Database
```bash
python load_data.py
```
This script creates and populates:
- `appointments`
- `patients`
- `prescriptions`
- `providers`

### Step 3: Start Airflow Containers
```bash
cd airflow-docker
docker-compose up -d
```
- Airflow UI: [http://localhost:8080](http://localhost:8080)
- User/Password: `airflow/airflow`

### Step 4: Run the Orchestrator DAG
Trigger the `orchestrate_full_pipeline` DAG in the Airflow UI.

---

## 📖 About Medallion Architecture

This project follows the **Medallion Architecture**:

| Layer | Purpose |
|:---|:---|
| **Landing** | Raw data extracted from the source. |
| **Bronze** | Data partitioned and organized for initial quality control. |
| **Silver** | Cleansed, deduplicated, and validated data ready for transformation. |
| **Gold** | Enriched, feature-engineered data ready for business use or machine learning models. |

### Importance of Partitioning
Partitioning by `year/month/day`:
- Improves read performance.
- Reduces data scanned during queries.
- Avoids small file issues, which can severely affect processing engines like Spark and Athena.
- Facilitates time-travel queries and efficient reprocessing.

Proper partition management ensures scalability and cost optimization.

---

## 🔗 DAGs and Their Responsibilities

### 1. `export_postgres_tables_to_parquet`
- Connects to the PostgreSQL database.
- Extracts all tables from schema `public`.
- Saves each table as a Parquet file into the `landing/` folder.
- Adds a `extraction_timestamp` to all records.

### 2. `move_landing_to_bronze_partitioned`
- Reads all Parquet files from `landing/`.
- Partitions the data by `year/month/day` based on the `extraction_timestamp`.
- Moves files to `bronze/`.
- Deletes the original files from `landing/` after moving.

### 3. `move_bronze_to_silver_partitioned`
- Selects the **most recent partition** in `bronze/`.
- Compares it with the corresponding files in `silver/`:
  - If identical (excluding timestamps), updates `updated_at` only.
  - If different, overwrites the partition in `silver/`.

### 4. `silver_to_gold_feature_engineering`
- Reads the most recent `silver/` partition.
- Applies feature engineering:
  - Computes time since last appointment.
  - Categorizes patients into age groups.
  - Calculates months registered.
  - Analyzes prescription frequency.
- Saves the enriched data into the corresponding `gold/` partition.

### 5. `create_gold_tables_from_parquet`
- Reads the most recent `gold/` partition.
- Creates the schema `gold` in the PostgreSQL database if not exists.
- Creates the corresponding tables (`appointments`, `patients`, `prescriptions`, `providers`) with inferred schemas from the Parquet files.

### 6. `orchestrate_full_pipeline`
- Coordinates the execution of all the above DAGs sequentially.
- Ensures that each step completes before triggering the next.

---

## 📈 Best Practices and Notes

- **Partition Management:**
  - Always monitor partition sizes.
  - Avoid too many small files; prefer batch consolidation when necessary.
  - Ensure partition pruning when querying to optimize performance.

- **Airflow Monitoring:**
  - Monitor task durations and failures in the Airflow UI.
  - Implement SLA misses alerts if needed for production.

- **Data Validation:**
  - Always validate extracted and transformed data.
  - Implement checksums or row counts if critical.

- **Schema Evolution:**
  - Be cautious if schemas change. Current DAGs expect static schema.
  - Future enhancement: support dynamic schema evolution.

---

#  Conclusion

With this setup, you have a complete, production-grade data pipeline implementing the Medallion Architecture using open-source tools.

Just trigger `orchestrate_full_pipeline`, and the entire process—from raw extraction to database-ready enriched tables—is handled automatically!

Happy Data Engineering! 🚀


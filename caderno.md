# 📄 Project: Data Pipeline with Airflow, PostgreSQL, and Medallion Architecture

## 📦 Project Structure

```plaintext
 portrait-data-engineer-test/
├── .vscode/
│
├── airflow-docker/
│   ├── dags/
│   │   ├── create_gold_tables_from_parquet.py
│   │   ├── export_tables_to_parquet.py
│   │   ├── move_bronze_to_silver_partitioned.py
│   │   ├── move_landing_to_bronze_partitioned.py
│   │   ├── orchestrate_full_pipeline.py
│   │   ├── silver_to_gold_feature_engineering.py
│   ├── config.yaml
│   ├── docker-compose.yaml
│   └── plugins/
│
├── app/
│   ├── bucket/
│   │   ├── landing/
│   │   ├── bronze/
│   │   ├── silver/
│   │   └── gold/
│   ├── BI.ipynb
│   ├── build_report.py
│   ├── exploratory_analysis.ipynb
│
├── output/
│   ├── A1_age_distribution.png
│   ├── B1_appointment_types.png
│   ├── B2_emergencies_per_day.png
│   ├── C1_most_prescribed_medications.png
│   ├── C2_correlation.png
│   └── Report.pdf
│
├── sample_datasets/
│
├── .gitignore
├── README.md
├── caderno.md
├── docker-compose.yaml
├── load_data.py
├── requirements.txt
├── sample_queries.sql

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
- User/Password: `admin/admin`

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
| **Gold** | Enriched, feature-engineered data ready for business use  |

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


# Project Development Report

 

## Creation Process

I adopted the assumption that I did not have direct access to the `sample_dataset` data, considering that, in an ideal scenario, it would already be stored in a database.

Since the task required building an ETL process, I planned a pipeline focused on extracting data from the provided database.

---

## Extraction

I chose to use Apache Airflow, deployed via a Docker image, as the orchestrator. This decision was based on the practicality of simulating the process locally, alongside the locally provided database.

In a cloud environment, alternative orchestration strategies could be used. On AWS, for example, Glue and Managed Workflows for Apache Airflow (MWAA) are commonly adopted (SaaS or PaaS models). On Azure, the standard would typically be Azure Data Factory. Within Databricks environments, the native notebook orchestration features could be leveraged.

For this project, a DAG (`export_postgres_tables_to_parquet`) was created to perform the data extraction. In a streaming scenario, tools like AWS Kinesis could be incorporated to continuously ingest data into a landing zone, such as S3 or another data lake.

---

## Exploratory Data Analysis (EDA)

The notebook `exploratory analysis.ipynb` reads the files from the landing zone extracted previously. It documents the process of getting familiar with the datasets, inspecting data structure, granularity, information completeness, and identifying potential errors.

Following the EDA, I developed the entire transformation strategy and outlined the analyses required. I decided to keep the notebook draft as evidence of the exploratory work.

---

## Transformation

Based on the EDA planning, I first saved the transformed datasets into a **bronze layer**, partitioned by extraction timestamp, simulating the structure of a data lake.

The DAG (`move_landing_to_bronze_partitioned`) is responsible for adding the extraction timestamp and creating partitioned datasets based on the date of extraction. This strategy would likely be adopted in production environments.

Subsequently, the DAG (`move_bronze_to_silver_partitioned`) was developed to validate and deduplicate records, creating a **silver layer** that consolidates clean and enriched data.

Finally, the DAG (`create_gold_tables_from_parquet`) was implemented to create **gold tables**, translating the logic and transformations designed in the `exploratory analysis.ipynb` notebook. 

In a larger or more complex environment, it would be preferable to split this final transformation into multiple DAGs (one per table), enabling more granular feature engineering and maintainability.

---

## Loading

The DAG (`orchestrate_full_pipeline`) coordinates the final loading stage, creating the **gold schema** within the same database initially provided and inserting the fully processed tables.

These gold tables are production-ready and could serve as data sources for:

- Business Intelligence tools (e.g., Power BI)
- Analytical applications
- Machine Learning models

---

# Analytics

The process of building analytical insights began within the exploratory notebook and was refined in the `BI.ipynb` notebook, where the construction of final tables started.

The script [`app/build_report.py`] automates the generation of the `Report.pdf`, compiling all required analyses and answers to business questions.

In a production-grade setup, it would be more appropriate to integrate with a BI tool like Power BI or Tableau to provide dynamic visualization.

Additionally, the project includes a dedicated **SQL section** where all queries that answer the business questions are listed, allowing validation directly against the gold schema.

In a real-world scenario, several additional insights could be generated to support the business, such as:

- Identifying frequent patients who have not returned for a long time and triggering follow-up communication
- Using patients' birthdays to offer promotions and increase engagement
- Profiling patients who do not return after their first appointment
- Identifying professionals with the highest patient retention rates and analyzing their practices to replicate success across the clinic

And many other potential analyses could be developed to enhance business decision-making.



## SQL


### A1 - Distribution of Patients by Age Group

```sql
SELECT 
    age_group,
    COUNT(*) AS number_of_patients
FROM gold.patients
GROUP BY age_group
ORDER BY number_of_patients DESC;

-- Result:
-- age_group | number_of_patients
-- ----------+--------------------
-- 71+       | 15
-- 31-50     | 14
-- 51-70     | 12
-- 19-30     | 8
-- Unknown   | 5
-- 0-18      | 1
```

---

### A2 - Appointment Frequency by Patient Type

```sql
SELECT 
    patient_type,
    COUNT(*) AS appointment_count,
    AVG(days_since_last_appointment) AS avg_days_since_last_appointment,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY days_since_last_appointment) AS median_days_since_last_appointment,
    STDDEV_POP(days_since_last_appointment) AS std_days_since_last_appointment
FROM gold.appointments a
JOIN gold.patients p ON a.patient_id = p.patient_id
WHERE days_since_last_appointment >= 0
GROUP BY patient_type;

-- Result:
-- patient_type | appointment_count | avg_days_since_last_appointment | median_days_since_last_appointment | std_days_since_last_appointment
-- -------------+--------------------+-------------------------------+------------------------------------+-------------------------------
-- Regular      | 37                  | 25.65                         | 17.0                              | 21.42
-- Long-term    | 18                  | 23.11                         | 10.0                              | 25.77
-- New          | 2                   | 9.50                          | 9.5                               | 2.12
```

---

### B1 - Most Common Appointment Type by Age Group

```sql
SELECT 
    p.age_group,
    a.appointment_type,
    COUNT(*) AS appointment_count
FROM gold.appointments a
JOIN gold.patients p ON a.patient_id = p.patient_id
GROUP BY p.age_group, a.appointment_type
ORDER BY p.age_group, appointment_count DESC;

-- Result:
-- age_group | appointment_type | appointment_count
-- ----------+------------------+-------------------
-- 0-18      | Consultation      | 1
-- 19-30     | Checkup           | 4
-- 31-50     | Consultation      | 5
-- 51-70     | Checkup           | 7
-- 71+       | Checkup           | 9
```

---

### B2 - Days of the Week with the Most Emergency Visits

```sql
SELECT 
    day_of_week,
    COUNT(*) AS emergency_count
FROM gold.appointments
WHERE appointment_type = 'Emergency'
GROUP BY day_of_week
ORDER BY emergency_count DESC;

-- Result:
-- day_of_week | emergency_count
-- ------------+-----------------
-- Friday      | 9
-- Monday      | 6
-- Saturday    | 6
-- Thursday    | 4
-- Sunday      | 3
-- Tuesday     | 3
-- Wednesday   | 2
```

---

### C1 - Most Prescribed Medication Categories by Age Group

```sql
SELECT 
    p.age_group,
    pr.medication_category,
    COUNT(*) AS prescription_count
FROM gold.prescriptions pr
JOIN gold.patients p ON pr.patient_id = p.patient_id
GROUP BY p.age_group, pr.medication_category
ORDER BY p.age_group, prescription_count DESC;

-- Result:
-- age_group | medication_category | prescription_count
-- ----------+---------------------+--------------------
-- 0-18      | Other                | 2
-- 19-30     | Other                | 5
-- 31-50     | Other                | 16
-- 51-70     | Other                | 19
-- 71+       | Other                | 25
```

---

### C2 - Correlation Between Appointment and Prescription Counts per Patient

```sql
SELECT 
    p.patient_id,
    COUNT(DISTINCT a.appointment_id) AS num_appointments,
    COUNT(DISTINCT pr.prescription_id) AS num_prescriptions
FROM gold.patients p
LEFT JOIN gold.appointments a ON p.patient_id = a.patient_id
LEFT JOIN gold.prescriptions pr ON p.patient_id = pr.patient_id
GROUP BY p.patient_id;

-- Result (example rows):
-- patient_id | num_appointments | num_prescriptions
-- -----------+------------------+-------------------
-- UUID-001   | 3                | 5
-- UUID-002   | 1                | 0
-- UUID-003   | 5                | 3

-- Correlation calculated separately.
-- Reported Pearson correlation: -0.13
```

# 📋 Conclusion

This project successfully demonstrates the end-to-end development of a data pipeline applying Medallion Architecture principles, leveraging modern tools such as Apache Airflow, PostgreSQL, Python, and Docker.

Throughout the project:
- A complete ETL pipeline was built to extract, transform, and load data from a relational database into structured layers (Landing, Bronze, Silver, Gold).
- Key engineering best practices were applied, including partitioning strategies, schema validation, and orchestration control through Airflow DAGs.
- Analytical insights were generated through structured exploratory analysis, feature engineering, and business-driven SQL queries.
- Documentation, reporting, and result validation were integrated into the development process, ensuring reproducibility and transparency.

While this project was developed in a simulated environment (using Docker containers locally), the structure and practices adopted are aligned with real-world production standards and could easily be migrated to cloud-based architectures.

In addition, the selective use of AI-assisted coding tools like GitHub Copilot and ChatGPT contributed to enhancing productivity, maintaining code quality, and ensuring documentation excellence.

This project serves as a robust foundation for more complex data engineering solutions, including scaling to real-time data ingestion, schema evolution handling, and dynamic reporting integration.



# 🤖 AI Assistance Report

Continuous usage of **GitHub Copilot** was adopted throughout the project development.

Main uses of **ChatGPT** included:
- Support in setting up the Docker Compose configuration for creating the Airflow environment.
- Support in indenting and structuring the DAGs, based on a layout template provided, ensuring organized ETL, Load, and Extract functions.
- Assistance in building the report generation script and automating PDF creation.
- Help in constructing and improving visualizations and analytical charts.
- Support in reviewing and refining this documentation, including grammatical and stylistic corrections.

The use of AI tools significantly enhanced development efficiency, ensured cleaner code organization, and improved overall documentation quality.


# 📞 Contact

- **Name:** João Vitor Clark
- **Phone:** +55 11 99100-4202
- **Email:** [joaovitorclark@gmail.com](mailto:joaovitorclark@gmail.com)
- **GitHub:** [github.com/joaovitorclark](https://github.com/joaovitorclark)
- **LinkedIn:** [linkedin.com/in/joaovitorclark](https://www.linkedin.com/in/joaovitorclark/)
- **Date:** 2025-04-28
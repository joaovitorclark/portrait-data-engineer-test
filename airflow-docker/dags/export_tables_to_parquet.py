from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import yaml
import psycopg2
import os
from sqlalchemy import create_engine

# Função que conecta no banco, exporta tabelas para Parquet
def export_tables_to_parquet():
    with open('/opt/airflow/dags/config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    pg_config = config['postgres']

    connection_string = (
        f"postgresql+psycopg2://{pg_config['username']}:{pg_config['password']}@"
        f"{pg_config['host']}:{pg_config['port']}/{pg_config['database']}"
    )

    engine = create_engine(connection_string)

    extraction_time = datetime.now()

    tables_query = """
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public' 
      AND table_type = 'BASE TABLE';
    """

    with engine.connect() as conn:
        result = conn.execute(tables_query)
        tables = [row[0] for row in result.fetchall()]

        output_dir = '/opt/airflow/app/bucket/landing'
        os.makedirs(output_dir, exist_ok=True)

        for table in tables:
            df = pd.read_sql_table(table, conn)
            df['extraction_timestamp'] = extraction_time
            df.to_parquet(os.path.join(output_dir, f"{table}.parquet"))
            print(f"Tabela {table} salva em Parquet com timestamp de extração.")

# Define a DAG
with DAG(
    dag_id='export_postgres_tables_to_parquet',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None, 
    catchup=False,
    tags=['landing', 'extract'],
) as dag:

    export_task = PythonOperator(
        task_id='export_tables',
        python_callable=export_tables_to_parquet
    )

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
import re
import psycopg2
from sqlalchemy import create_engine, text

def create_gold_tables():
    # Paths
    current_path = '/opt/airflow/app'
    gold_path = os.path.join(current_path, 'bucket', 'gold')

    # Banco de dados
    config = {
        'host': 'host.docker.internal',  # correto para containers docker no Mac/Windows
        'port': '5432',
        'database': 'healthcare',
        'username': 'postgres',
        'password': 'postgres'
    }
    # Monta a connection string
    connection_string = (
        f"postgresql+psycopg2://{config['username']}:{config['password']}@"
        f"{config['host']}:{config['port']}/{config['database']}"
    )
    engine = create_engine(connection_string)

    def find_latest_partition(base_path):
        partitions = []
        for root, dirs, files in os.walk(base_path):
            match = re.search(r'year=(\d+)/month=(\d+)/day=(\d+)', root)
            if match:
                year, month, day = map(int, match.groups())
                partitions.append((year, month, day, root))
        if not partitions:
            return None
        partitions.sort()
        latest_partition = partitions[-1]
        return latest_partition[-1]  # retorna o caminho da última partição

    gold_partition = find_latest_partition(gold_path)
    if not gold_partition:
        raise Exception("Nenhuma partição encontrada na Gold.")

    print(f"Lendo arquivos da gold: {gold_partition}")

    # Tabelas para criar
    tables = {
        'appointments': pd.read_parquet(os.path.join(gold_partition, "appointments.parquet")),
        'patients': pd.read_parquet(os.path.join(gold_partition, "patients.parquet")),
        'prescriptions': pd.read_parquet(os.path.join(gold_partition, "prescriptions.parquet")),
        'providers': pd.read_parquet(os.path.join(gold_partition, "providers.parquet")),
    }

    # Cria o schema gold se não existir
    with engine.connect() as conn:
        conn.execute(text('CREATE SCHEMA IF NOT EXISTS gold;'))
        print("Schema 'gold' garantido.")

    # Função para mapear tipos pandas -> PostgreSQL
    def map_dtype(dtype):
        if pd.api.types.is_integer_dtype(dtype):
            return 'BIGINT'
        elif pd.api.types.is_float_dtype(dtype):
            return 'FLOAT'
        elif pd.api.types.is_bool_dtype(dtype):
            return 'BOOLEAN'
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return 'TIMESTAMP'
        else:
            return 'TEXT'

    # Cria tabelas se não existirem
    for table_name, df in tables.items():
        columns = []
        for col, dtype in df.dtypes.items():
            sql_type = map_dtype(dtype)
            columns.append(f'"{col}" {sql_type}')

        columns_sql = ",\n  ".join(columns)
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS gold.{table_name} (
          {columns_sql}
        );
        """

        with engine.connect() as conn:
            conn.execute(text(create_table_sql))
            print(f"Tabela gold.{table_name} garantida.")

# Define a DAG
with DAG(
    dag_id='create_gold_tables_from_parquet',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['gold', 'create-tables'],
) as dag:

    create_tables_task = PythonOperator(
        task_id='create_gold_tables',
        python_callable=create_gold_tables
    )

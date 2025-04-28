from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
import re

def find_latest_partition(bronze_dir):
    partitions = []

    # Procura todas as partições no formato year=XXXX/month=XX/day=XX
    for root, dirs, files in os.walk(bronze_dir):
        match = re.search(r'year=(\d+)/month=(\d+)/day=(\d+)', root)
        if match:
            year, month, day = map(int, match.groups())
            partitions.append((year, month, day, root))

    if not partitions:
        return None

    # Ordena as partições pela data
    partitions.sort()
    latest_partition = partitions[-1]  # pega a mais recente
    return latest_partition[-1]  # retorna o caminho

def move_to_silver_partitioned():
    bronze_dir = '/opt/airflow/app/bucket/bronze'
    silver_dir = '/opt/airflow/app/bucket/silver'

    latest_partition = find_latest_partition(bronze_dir)

    if not latest_partition:
        print("Nenhuma partição encontrada na bronze.")
        return

    print(f"Última partição encontrada: {latest_partition}")

    # Agora só vamos mover os arquivos dessa partição
    for file_name in os.listdir(latest_partition):
        if file_name.endswith('.parquet'):
            bronze_file_path = os.path.join(latest_partition, file_name)

            # Lê o arquivo da bronze
            bronze_df = pd.read_parquet(bronze_file_path)

            # Define o caminho correspondente na silver
            relative_path = os.path.relpath(latest_partition, bronze_dir)
            silver_partition_dir = os.path.join(silver_dir, relative_path)
            os.makedirs(silver_partition_dir, exist_ok=True)
            silver_file_path = os.path.join(silver_partition_dir, file_name)

            # Se o arquivo NÃO existe na silver
            if not os.path.exists(silver_file_path):
                bronze_df['updated_at'] = datetime.now()
                bronze_df.to_parquet(silver_file_path)
                print(f"Arquivo novo criado na silver: {silver_file_path}")
            else:
                # Se o arquivo já existe, compara os dados
                silver_df = pd.read_parquet(silver_file_path)

                bronze_compare = bronze_df.drop(columns=['extraction_timestamp'], errors='ignore').sort_index(axis=1)
                silver_compare = silver_df.drop(columns=['extraction_timestamp', 'updated_at'], errors='ignore').sort_index(axis=1)

                if bronze_compare.equals(silver_compare):
                    # Só atualiza o updated_at
                    silver_df['updated_at'] = datetime.now()
                    silver_df.to_parquet(silver_file_path)
                    print(f"Arquivo atualizado apenas updated_at na silver: {silver_file_path}")
                else:
                    # Dados mudaram: recria o arquivo
                    bronze_df['updated_at'] = datetime.now()
                    bronze_df.to_parquet(silver_file_path)
                    print(f"Arquivo recriado na silver por alteração detectada: {silver_file_path}")

# Define a DAG
with DAG(
    dag_id='move_bronze_to_silver_partitioned',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['silver', 'partitioned', 'bronze'],
) as dag:

    move_task = PythonOperator(
        task_id='move_to_silver',
        python_callable=move_to_silver_partitioned
    )

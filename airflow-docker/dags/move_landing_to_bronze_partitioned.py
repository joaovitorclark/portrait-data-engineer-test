from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

# Função para particionar e salvar, apagando depois
def move_to_bronze_partitioned():
    landing_dir = '/opt/airflow/app/bucket/landing'
    bronze_dir = '/opt/airflow/app/bucket/bronze'

    # Garante que o diretório bronze existe
    os.makedirs(bronze_dir, exist_ok=True)

    # Lista todos os arquivos Parquet da landing
    for file_name in os.listdir(landing_dir):
        if file_name.endswith('.parquet'):
            file_path = os.path.join(landing_dir, file_name)
            df = pd.read_parquet(file_path)

            if 'extraction_timestamp' not in df.columns:
                print(f"Aviso: {file_name} não tem a coluna 'extraction_timestamp'. Pulando {file_name}.")
                continue

            # Extrai ano, mês e dia
            df['extraction_timestamp'] = pd.to_datetime(df['extraction_timestamp'])
            df['year'] = df['extraction_timestamp'].dt.year
            df['month'] = df['extraction_timestamp'].dt.month
            df['day'] = df['extraction_timestamp'].dt.day

            # Para cada grupo (ano/mês/dia), salva em pasta separada
            for (year, month, day), group_df in df.groupby(['year', 'month', 'day']):
                partition_path = os.path.join(
                    bronze_dir,
                    f"year={year}",
                    f"month={month:02d}",
                    f"day={day:02d}"
                )
                os.makedirs(partition_path, exist_ok=True)

                # Nome do arquivo baseado no nome da tabela
                table_name = file_name.replace('.parquet', '')
                output_file = os.path.join(partition_path, f"{table_name}.parquet")

                # Remove as colunas auxiliares e salva
                group_df.drop(columns=['year', 'month', 'day']).to_parquet(output_file)
                print(f"Arquivo salvo: {output_file}")

            # Após salvar todos os dados, remove o arquivo da landing
            os.remove(file_path)
            print(f"Arquivo original removido: {file_name}")

# Define a DAG
with DAG(
    dag_id='move_landing_to_bronze_partitioned',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Só roda manualmente ou via trigger
    catchup=False,
    tags=['bronze', 'partitioned', 'landing'],
) as dag:

    move_task = PythonOperator(
        task_id='move_to_bronze',
        python_callable=move_to_bronze_partitioned
    )

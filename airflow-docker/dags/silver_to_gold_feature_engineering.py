from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import numpy as np
import os
import re

def silver_to_gold():
    current_path = '/opt/airflow/app'
    silver_path = os.path.join(current_path, 'bucket', 'silver')
    gold_path = os.path.join(current_path, 'bucket', 'gold')

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

    silver_partition = find_latest_partition(silver_path)
    if not silver_partition:
        raise Exception("Nenhuma partição encontrada na Silver.")

    print(f"Lendo arquivos da silver: {silver_partition}")

    # Lê os arquivos
    df_appointments = pd.read_parquet(os.path.join(silver_partition, "appointments.parquet"))
    df_patients = pd.read_parquet(os.path.join(silver_partition, "patients.parquet"))
    df_prescriptions = pd.read_parquet(os.path.join(silver_partition, "prescriptions.parquet"))
    df_providers = pd.read_parquet(os.path.join(silver_partition, "providers.parquet"))

    # Feature Engineering

    # Appointments
    df_appointments['appointment_date'] = pd.to_datetime(df_appointments['appointment_date'])
    df_appointments['day_of_week'] = df_appointments['appointment_date'].dt.day_name()
    df_appointments = df_appointments.sort_values(['patient_id', 'appointment_date'])
    df_appointments['days_since_last_appointment'] = (
        df_appointments.groupby('patient_id')['appointment_date']
        .diff().dt.days.fillna(-1).astype(int)
    )

    # Patients
    df_patients['age_group'] = (
        pd.cut(
            df_patients['age'],
            bins=[-1, 18, 30, 50, 70, float('inf')],
            labels=['0-18', '19-30', '31-50', '51-70', '71+']
        )
        .astype(str)
        .replace('nan', 'Unknown')
    )

    df_patients['registration_date'] = pd.to_datetime(df_patients['registration_date'])
    today = pd.Timestamp.today()

    def calculate_months_since(date):
        return (today.year - date.year) * 12 + (today.month - date.month)

    df_patients['months_registered'] = df_patients['registration_date'].apply(calculate_months_since)

    df_patients['patient_type'] = pd.cut(
        df_patients['months_registered'],
        bins=[-1, 6, 24, float('inf')],
        labels=['New', 'Regular', 'Long-term']
    )
    df_patients['patient_type'] = df_patients['patient_type'].cat.add_categories('Unknown').fillna('Unknown')

    # Prescriptions
    df_prescriptions['prescription_date'] = pd.to_datetime(df_prescriptions['prescription_date'])
    df_prescriptions = df_prescriptions.sort_values(['patient_id', 'medication_name', 'prescription_date'])

    df_prescriptions['prescription_frequency'] = (
        df_prescriptions.groupby(['patient_id', 'medication_name'])['prescription_date']
        .diff().dt.days
    )

    df_prescriptions['avg_prescription_frequency'] = (
        df_prescriptions.groupby(['patient_id', 'medication_name'])['prescription_frequency']
        .transform('mean').round(1)
    )

    df_prescriptions['prescription_repeats'] = (
        df_prescriptions.groupby(['patient_id', 'medication_name'])['prescription_id']
        .transform('count')
    )

    df_prescriptions['prescription_frequency'] = df_prescriptions['prescription_frequency'].fillna(-1).astype(int)

    # Salvando na GOLD
    relative_partition = os.path.relpath(silver_partition, silver_path)
    gold_partition = os.path.join(gold_path, relative_partition)
    os.makedirs(gold_partition, exist_ok=True)

    print(f"Salvando na gold: {gold_partition}")

    df_appointments.to_parquet(os.path.join(gold_partition, "appointments.parquet"))
    df_patients.to_parquet(os.path.join(gold_partition, "patients.parquet"))
    df_prescriptions.to_parquet(os.path.join(gold_partition, "prescriptions.parquet"))
    df_providers.to_parquet(os.path.join(gold_partition, "providers.parquet"))

    print("Arquivos salvos na camada GOLD com sucesso!")

# Define a DAG
with DAG(
    dag_id='silver_to_gold_feature_engineering',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Manual ou trigger
    catchup=False,
    tags=['gold', 'feature-engineering', 'silver'],
) as dag:

    feature_engineering_task = PythonOperator(
        task_id='process_silver_to_gold',
        python_callable=silver_to_gold
    )

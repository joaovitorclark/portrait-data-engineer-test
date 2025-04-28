from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

# Define a DAG
with DAG(
    dag_id='orchestrate_full_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Trigger manual
    catchup=False,
    tags=['orchestration', 'full-pipeline'],
) as dag:

    start = EmptyOperator(task_id="start_pipeline")

    trigger_extract = TriggerDagRunOperator(
        task_id="trigger_export_postgres_tables",
        trigger_dag_id="export_postgres_tables_to_parquet",
    )

    trigger_landing_to_bronze = TriggerDagRunOperator(
        task_id="trigger_move_landing_to_bronze",
        trigger_dag_id="move_landing_to_bronze_partitioned",
    )

    trigger_bronze_to_silver = TriggerDagRunOperator(
        task_id="trigger_move_bronze_to_silver",
        trigger_dag_id="move_bronze_to_silver_partitioned",
    )

    trigger_silver_to_gold = TriggerDagRunOperator(
        task_id="trigger_silver_to_gold",
        trigger_dag_id="silver_to_gold_feature_engineering",
    )

    trigger_create_gold_tables = TriggerDagRunOperator(
        task_id="trigger_create_gold_tables",
        trigger_dag_id="create_gold_tables_from_parquet",
    )

    end = EmptyOperator(task_id="end_pipeline")

    # Orquestração
    start >> trigger_extract >> trigger_landing_to_bronze >> trigger_bronze_to_silver >> trigger_silver_to_gold >> trigger_create_gold_tables >> end

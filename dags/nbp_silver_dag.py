from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime

default_args = {
    "retries": 3,
}

with DAG(
    dag_id="nbp_silver_ingestion",
    schedule="@daily",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    default_args=default_args,
    tags=["silver", "nbp"],
) as dag:

    wait_for_bronze = ExternalTaskSensor(
        task_id="wait_for_bronze",
        external_dag_id="nbp_bronze_ingestion",
        external_task_id="ingest_nbp_exchange_rates",
        mode="poke",
    )

    process_silver = SparkSubmitOperator(
        task_id="process_nbp_exchange_rates",
        application="/opt/airflow/src/silver/nbp_exchange_rates.py",
        conn_id="spark_default",
        name="silver.nbp_exchange_rates.{{ ds }}",
        packages="io.delta:delta-spark_2.12:3.2.0",
    )

    wait_for_bronze >> process_silver
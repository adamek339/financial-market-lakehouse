from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    "retries": 3,
}

with DAG(
    dag_id="nbp_bronze_ingestion",
    schedule="@daily",
    start_date=datetime(2026, 6, 1),
    catchup=True,
    default_args=default_args,
    tags=["bronze", "nbp"],
) as dag:

    ingest_nbp = SparkSubmitOperator(
        task_id="ingest_nbp_exchange_rates",
        application="/opt/airflow/src/bronze/nbp_exchange_rates.py",
        application_args=["{{ ds }}"],
        conn_id="spark_default",
        name="bronze.nbp_exchange_rates.{{ ds }}",
    )
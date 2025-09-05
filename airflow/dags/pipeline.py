import os
from datetime import datetime

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from dotenv import load_dotenv

from airflow import DAG

load_dotenv()
BASE_PATH = os.getenv("BASE_PATH")

with DAG(
    dag_id="pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    spark_task = SparkSubmitOperator(
        task_id="data_ingestion",
        application=f"{BASE_PATH}/spark/data_ingestion.py",
        conn_id="spark_default",
        verbose=True,
    )

    spark_task

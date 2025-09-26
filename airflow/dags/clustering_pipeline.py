import os
from datetime import datetime

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from dotenv import load_dotenv

from airflow import DAG

load_dotenv()
BASE_PATH = os.getenv("BASE_PATH")

with DAG(
    dag_id="clustering_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    data_ingestion = SparkSubmitOperator(
        task_id="data_ingestion",
        application=f"{BASE_PATH}/spark/data_ingestion.py",
        conn_id="spark_default",
        verbose=True,
    )

    clustering = SparkSubmitOperator(
        task_id="clustering",
        application=f"{BASE_PATH}/spark/clustering.py",
        conn_id="spark_default",
        verbose=True,
    )

    data_ingestion >> clustering

import os
from dotenv import load_dotenv
from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

load_dotenv()
BASE_PATH = os.getenv("BASE_PATH")

with DAG(
    dag_id="example_sparksubmit",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    spark_task = SparkSubmitOperator(
        task_id="count_lines",
        application=f"{BASE_PATH}/spark/example_count_lines.py",
        conn_id="spark_default",
        verbose=True,
    )

    spark_task

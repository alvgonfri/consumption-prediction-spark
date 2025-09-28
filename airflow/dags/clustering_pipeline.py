import os
from datetime import datetime

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.bash import BashOperator
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
        application=f"{BASE_PATH}/spark/clustering/clustering.py",
        conn_id="spark_default",
        verbose=True,
    )

    preprocessing = SparkSubmitOperator(
        task_id="preprocessing",
        application=f"{BASE_PATH}/spark/preprocessing.py",
        conn_id="spark_default",
        verbose=True,
    )

    training = SparkSubmitOperator(
        task_id="training",
        application=f"{BASE_PATH}/spark/clustering/clustering_training.py",
        conn_id="spark_default",
        verbose=True,
    )

    evaluation = SparkSubmitOperator(
        task_id="evaluation",
        application=f"{BASE_PATH}/spark/clustering/clustering_evaluation.py",
        conn_id="spark_default",
        verbose=True,
    )

    model_selection = BashOperator(
        task_id="model_selection",
        bash_command=f"python {BASE_PATH}/scripts/clustering_model_selection.py",
    )

    prediction = SparkSubmitOperator(
        task_id="prediction",
        application=f"{BASE_PATH}/spark/clustering/clustering_prediction.py",
        conn_id="spark_default",
        verbose=True,
    )

    (
        data_ingestion
        >> clustering
        >> preprocessing
        >> training
        >> evaluation
        >> model_selection
        >> prediction
    )

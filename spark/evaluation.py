import json
import os
import sqlite3
import time
from datetime import datetime

from dotenv import load_dotenv
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import SparkSession

load_dotenv()
BASE_PATH = os.getenv("BASE_PATH")

RUN_DATETIME = datetime.now().isoformat()


def log_to_db(
    model_name,
    params,
    inference_time,
    rmse_train,
    mae_train,
    r2_train,
    rmse_val,
    mae_val,
    r2_val,
    rmse_test,
    mae_test,
    r2_test,
    db_path=os.path.join(BASE_PATH, "db", "audit.db"),
):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO audit (
            model, run_datetime, params, inference_time,
            rmse_train, mae_train, r2_train,
            rmse_val, mae_val, r2_val,
            rmse_test, mae_test, r2_test
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        (
            model_name,
            RUN_DATETIME,
            params,
            inference_time,
            rmse_train,
            mae_train,
            r2_train,
            rmse_val,
            mae_val,
            r2_val,
            rmse_test,
            mae_test,
            r2_test,
        ),
    )
    conn.commit()
    conn.close()


def evaluation():
    spark = SparkSession.builder.appName("Evaluation").getOrCreate()

    data_path = os.path.join(BASE_PATH, "data", "processed")

    train_df = spark.read.parquet(os.path.join(data_path, "train.parquet"))
    val_df = spark.read.parquet(os.path.join(data_path, "val.parquet"))
    test_df = spark.read.parquet(os.path.join(data_path, "test.parquet"))

    models_path = os.path.join(BASE_PATH, "models")

    # ========================= Drop unnecessary columns =========================
    cols_to_drop = ["date", "customer", "temp_h"]

    train_df = train_df.drop(*cols_to_drop)
    val_df = val_df.drop(*cols_to_drop)
    test_df = test_df.drop(*cols_to_drop)

    # ====================== Evaluate all models ======================
    target_col = "cons_h"

    evaluator_rmse = RegressionEvaluator(
        labelCol=target_col, predictionCol="prediction", metricName="rmse"
    )
    evaluator_mae = RegressionEvaluator(
        labelCol=target_col, predictionCol="prediction", metricName="mae"
    )
    evaluator_r2 = RegressionEvaluator(
        labelCol=target_col, predictionCol="prediction", metricName="r2"
    )

    for name in os.listdir(models_path):
        if not os.path.isdir(os.path.join(models_path, name)) or name in [
            "clustering",
            "best_model",
        ]:
            continue

        print(f"\n=== Evaluating {name} ===")

        model_path = os.path.join(models_path, name)
        model = PipelineModel.load(model_path)

        # Training metrics
        train_predictions = model.transform(train_df)
        rmse_train = evaluator_rmse.evaluate(train_predictions)
        mae_train = evaluator_mae.evaluate(train_predictions)
        r2_train = evaluator_r2.evaluate(train_predictions)

        # Validate
        val_predictions = model.transform(val_df)
        rmse_val = evaluator_rmse.evaluate(val_predictions)
        mae_val = evaluator_mae.evaluate(val_predictions)
        r2_val = evaluator_r2.evaluate(val_predictions)

        # Test
        start_time = time.time()
        test_predictions = model.transform(test_df)
        test_predictions.count()  # Action to trigger computation
        inference_time = time.time() - start_time

        rmse_test = evaluator_rmse.evaluate(test_predictions)
        mae_test = evaluator_mae.evaluate(test_predictions)
        r2_test = evaluator_r2.evaluate(test_predictions)

        # ===================== Log results to DB ======================
        params_path = os.path.join(model_path, "params.json")
        if os.path.exists(params_path):
            with open(params_path, "r") as f:
                params = json.load(f)
            params_str = json.dumps(params)
        else:
            params_str = None

        log_to_db(
            model_name=name,
            params=params_str,
            inference_time=inference_time,
            rmse_train=rmse_train,
            mae_train=mae_train,
            r2_train=r2_train,
            rmse_val=rmse_val,
            mae_val=mae_val,
            r2_val=r2_val,
            rmse_test=rmse_test,
            mae_test=mae_test,
            r2_test=r2_test,
        )

    spark.stop()


evaluation()

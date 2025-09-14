import os
import time

from dotenv import load_dotenv
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import SparkSession

load_dotenv()
BASE_PATH = os.getenv("BASE_PATH")


def evaluation():
    spark = SparkSession.builder.appName("Evaluation").getOrCreate()

    data_path = os.path.join(BASE_PATH, "data", "processed")

    train_df = spark.read.parquet(os.path.join(data_path, "train.parquet"))
    val_df = spark.read.parquet(os.path.join(data_path, "val.parquet"))
    test_df = spark.read.parquet(os.path.join(data_path, "test.parquet"))

    models_path = os.path.join(BASE_PATH, "models")

    # ====================== Evaluate all models ======================
    for name in os.listdir(models_path):
        if not os.path.isdir(os.path.join(models_path, name)):
            continue

        model_path = os.path.join(models_path, name)
        print(f"\n=== Evaluating {name} ===")
        model = PipelineModel.load(model_path)

        # Evaluators
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

        # Training metrics
        train_predictions = model.transform(train_df)
        rmse_train = evaluator_rmse.evaluate(train_predictions)
        mae_train = evaluator_mae.evaluate(train_predictions)
        r2_train = evaluator_r2.evaluate(train_predictions)
        print(
            f"Train -> RMSE: {rmse_train:.4f}, MAE: {mae_train:.4f}, R2: {r2_train:.4f}"
        )

        # Validate
        val_predictions = model.transform(val_df)
        rmse_val = evaluator_rmse.evaluate(val_predictions)
        mae_val = evaluator_mae.evaluate(val_predictions)
        r2_val = evaluator_r2.evaluate(val_predictions)
        print(
            f"Validation -> RMSE: {rmse_val:.4f}, MAE: {mae_val:.4f}, R2: {r2_val:.4f}"
        )

        # Test
        start_time = time.time()
        test_predictions = model.transform(test_df)
        test_predictions.count()  # Action to trigger computation
        inference_time = time.time() - start_time

        rmse_test = evaluator_rmse.evaluate(test_predictions)
        mae_test = evaluator_mae.evaluate(test_predictions)
        r2_test = evaluator_r2.evaluate(test_predictions)
        print(f"Test -> RMSE: {rmse_test:.4f}, MAE: {mae_test:.4f}, R2: {r2_test:.4f}")
        print(f"Test inference time: {inference_time:.4f} seconds")

    spark.stop()


evaluation()

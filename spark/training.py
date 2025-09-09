import os

from dotenv import load_dotenv
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.regression import (
    DecisionTreeRegressor,
    GBTRegressor,
    LinearRegression,
    RandomForestRegressor,
)
from pyspark.sql import SparkSession

load_dotenv()
BASE_PATH = os.getenv("BASE_PATH")


def training():
    spark = SparkSession.builder.appName("Training").getOrCreate()

    data_path = os.path.join(BASE_PATH, "data", "processed")

    train_df = spark.read.parquet(os.path.join(data_path, "train.parquet"))
    val_df = spark.read.parquet(os.path.join(data_path, "val.parquet"))
    test_df = spark.read.parquet(os.path.join(data_path, "test.parquet"))

    # ====================== Preprocessing ======================
    target_col = "cons_h"
    feature_cols = [c for c in train_df.columns if c != target_col]

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_assembled")
    scaler = StandardScaler(
        inputCol="features_assembled", outputCol="features", withMean=True, withStd=True
    )

    # ====================== Define models ======================
    models = {
        "Linear Regression": LinearRegression(
            featuresCol="features", labelCol=target_col
        ),
        "Decision Tree": DecisionTreeRegressor(
            featuresCol="features", labelCol=target_col
        ),
        "Random Forest": RandomForestRegressor(
            featuresCol="features", labelCol=target_col
        ),
        "Gradient-Boosted Tree": GBTRegressor(
            featuresCol="features", labelCol=target_col
        ),
    }

    # ====================== Evaluator ======================
    evaluator_rmse = RegressionEvaluator(
        labelCol=target_col, predictionCol="prediction", metricName="rmse"
    )
    evaluator_mae = RegressionEvaluator(
        labelCol=target_col, predictionCol="prediction", metricName="mae"
    )
    evaluator_r2 = RegressionEvaluator(
        labelCol=target_col, predictionCol="prediction", metricName="r2"
    )

    # ====================== Train & Evaluate ======================
    for name, model in models.items():
        print(f"\n=== {name} ===")

        pipeline = Pipeline(stages=[assembler, scaler, model])

        # Fit only on train
        pipeline_model = pipeline.fit(train_df)

        # Validate
        val_predictions = pipeline_model.transform(val_df)
        rmse_val = evaluator_rmse.evaluate(val_predictions)
        mae_val = evaluator_mae.evaluate(val_predictions)
        r2_val = evaluator_r2.evaluate(val_predictions)
        print(
            f"Validation -> RMSE: {rmse_val:.4f}, MAE: {mae_val:.4f}, R2: {r2_val:.4f}"
        )

        # Test
        test_predictions = pipeline_model.transform(test_df)
        rmse_test = evaluator_rmse.evaluate(test_predictions)
        mae_test = evaluator_mae.evaluate(test_predictions)
        r2_test = evaluator_r2.evaluate(test_predictions)
        print(f"Test -> RMSE: {rmse_test:.4f}, MAE: {mae_test:.4f}, R2: {r2_test:.4f}")

    spark.stop()


training()

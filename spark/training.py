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
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
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

        stages = (
            [assembler] + ([scaler] if name == "Linear Regression" else []) + [model]
        )
        pipeline = Pipeline(stages=stages)

        if name == "Linear Regression":
            param_grid = (
                ParamGridBuilder()
                .addGrid(model.regParam, [0.0, 0.01, 0.1])
                .addGrid(
                    model.elasticNetParam, [0.0, 0.5, 1.0]
                )  # Ridge, ElasticNet, Lasso
                .build()
            )
        elif name == "Decision Tree":
            param_grid = (
                ParamGridBuilder()
                .addGrid(model.maxDepth, [5, 10, 20])
                .addGrid(model.minInstancesPerNode, [1, 5, 10])
                .build()
            )
        elif name == "Random Forest":
            param_grid = (
                ParamGridBuilder()
                .addGrid(model.numTrees, [20, 50, 100])
                .addGrid(model.maxDepth, [5, 10, 20])
                .build()
            )
        elif name == "Gradient-Boosted Tree":
            param_grid = (
                ParamGridBuilder()
                .addGrid(model.maxDepth, [3, 5, 10])
                .addGrid(model.maxIter, [20, 50, 100])
                .build()
            )

        crossval = CrossValidator(
            estimator=pipeline,
            estimatorParamMaps=param_grid,
            evaluator=evaluator_rmse,  # RMSE as main metric
            numFolds=3,
        )

        cv_model = crossval.fit(train_df)

        best_model = cv_model.bestModel

        # Training metrics
        train_predictions = best_model.transform(train_df)
        rmse_train = evaluator_rmse.evaluate(train_predictions)
        mae_train = evaluator_mae.evaluate(train_predictions)
        r2_train = evaluator_r2.evaluate(train_predictions)
        print(
            f"Train -> RMSE: {rmse_train:.4f}, MAE: {mae_train:.4f}, R2: {r2_train:.4f}"
        )

        # Validate
        val_predictions = best_model.transform(val_df)
        rmse_val = evaluator_rmse.evaluate(val_predictions)
        mae_val = evaluator_mae.evaluate(val_predictions)
        r2_val = evaluator_r2.evaluate(val_predictions)
        print(
            f"Validation -> RMSE: {rmse_val:.4f}, MAE: {mae_val:.4f}, R2: {r2_val:.4f}"
        )

        # Test
        test_predictions = best_model.transform(test_df)
        rmse_test = evaluator_rmse.evaluate(test_predictions)
        mae_test = evaluator_mae.evaluate(test_predictions)
        r2_test = evaluator_r2.evaluate(test_predictions)
        print(f"Test -> RMSE: {rmse_test:.4f}, MAE: {mae_test:.4f}, R2: {r2_test:.4f}")

    spark.stop()


training()

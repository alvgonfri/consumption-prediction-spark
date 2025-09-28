import json
import os
import shutil

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
    clusters_df = spark.read.parquet(
        os.path.join(data_path, "customer_clusters.parquet")
    )

    train_df = train_df.join(clusters_df, on="customer", how="inner")

    # ========================= Drop unnecessary columns =========================
    cols_to_drop = ["date", "customer", "temp_h"]

    train_df = train_df.drop(*cols_to_drop)

    # ========================= Delete models from previous runs =========================
    clustering_models_dir = os.path.join(BASE_PATH, "models", "clustering")
    if os.path.exists(clustering_models_dir):
        for item in os.listdir(clustering_models_dir):
            item_path = os.path.join(clustering_models_dir, item)
            if os.path.isdir(item_path):
                shutil.rmtree(item_path)

    # ====================== Preprocessing ======================
    target_col = "cons_h"
    feature_cols = [c for c in train_df.columns if c not in (target_col, "cluster")]

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
            featuresCol="features_assembled", labelCol=target_col
        ),
        "Random Forest": RandomForestRegressor(
            featuresCol="features_assembled", labelCol=target_col
        ),
        "Gradient-Boosted Tree": GBTRegressor(
            featuresCol="features_assembled", labelCol=target_col
        ),
    }

    # ====================== Training and hyperparameter tuning per cluster ======================
    evaluator_rmse = RegressionEvaluator(
        labelCol=target_col, predictionCol="prediction", metricName="rmse"
    )

    clusters = [
        row["cluster"] for row in clusters_df.select("cluster").distinct().collect()
    ]

    for cluster in clusters:
        print(f"\n=== Cluster {cluster} ===")

        cluster_train_df = train_df.filter(train_df["cluster"] == cluster)

        for name, model in models.items():
            print(f"\n=== {name} ===")

            stages = (
                [assembler]
                + ([scaler] if name == "Linear Regression" else [])
                + [model]
            )
            pipeline = Pipeline(stages=stages)

            if name == "Linear Regression":
                param_grid = (
                    ParamGridBuilder()
                    .addGrid(model.regParam, [0.01])
                    .addGrid(model.elasticNetParam, [0.0])
                    .build()
                )
            elif name == "Decision Tree":
                param_grid = (
                    ParamGridBuilder()
                    .addGrid(model.maxDepth, [5])
                    .addGrid(model.minInstancesPerNode, [5])
                    .build()
                )
            elif name == "Random Forest":
                param_grid = (
                    ParamGridBuilder()
                    .addGrid(model.numTrees, [20])
                    .addGrid(model.maxDepth, [5])
                    .build()
                )
            elif name == "Gradient-Boosted Tree":
                param_grid = (
                    ParamGridBuilder()
                    .addGrid(model.maxDepth, [3])
                    .addGrid(model.maxIter, [20])
                    .build()
                )

            crossval = CrossValidator(
                estimator=pipeline,
                estimatorParamMaps=param_grid,
                evaluator=evaluator_rmse,  # RMSE as main metric
                numFolds=3,
            )

            cv_model = crossval.fit(cluster_train_df)

            # ====================== Save the best model ======================
            best_model = cv_model.bestModel
            model_path = os.path.join(
                BASE_PATH,
                "models",
                "clustering",
                f"cluster_{cluster}",
                name.replace(" ", "_").lower(),
            )
            best_model.write().overwrite().save(model_path)

            # Save best hyperparameters
            if name == "Linear Regression":
                param_names = ["regParam", "elasticNetParam"]
            elif name == "Decision Tree":
                param_names = ["maxDepth", "minInstancesPerNode"]
            elif name == "Random Forest":
                param_names = ["numTrees", "maxDepth"]
            elif name == "Gradient-Boosted Tree":
                param_names = ["maxDepth", "maxIter"]
            else:
                param_names = []

            params = {
                param[0].name: param[1]
                for param in best_model.stages[-1].extractParamMap().items()
                if param[0].name in param_names
            }

            with open(os.path.join(model_path, "params.json"), "w") as f:
                json.dump(params, f, indent=4)

    spark.stop()


training()

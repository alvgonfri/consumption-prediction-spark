import os
import shutil
from datetime import timedelta

from dotenv import load_dotenv
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window

load_dotenv()
BASE_PATH = os.getenv("BASE_PATH")


def prediction():
    spark = SparkSession.builder.appName("Prediction").getOrCreate()

    spark.sparkContext.setCheckpointDir(os.path.join(BASE_PATH, "spark", "checkpoints"))

    data_path = os.path.join(BASE_PATH, "data", "processed")

    df_predict = spark.read.parquet(os.path.join(data_path, "predict.parquet"))
    clusters_df = spark.read.parquet(
        os.path.join(data_path, "customer_clusters.parquet")
    )

    df_predict = df_predict.join(clusters_df, on="customer", how="inner")

    df_predict = df_predict.withColumnRenamed(
        "consumption", "cons_h"
    ).withColumnRenamed("temperature", "temp_h")

    last_date = df_predict.filter(F.col("cons_h") != -1.0).agg(F.max("date")).first()[0]
    print(f"Last date with known consumption: {last_date}")

    clusters = [
        row["cluster"] for row in clusters_df.select("cluster").distinct().collect()
    ]

    results = []

    for cluster in clusters:
        print(f"\n=== Predicting for cluster {cluster} ===")

        cluster_df = df_predict.filter(df_predict.cluster == cluster)

        model_path = os.path.join(
            BASE_PATH, "models", "clustering", f"cluster_{cluster}", "best_model"
        )
        model = PipelineModel.load(model_path)

        # ======================= Initialize columns to be used in the feature engineering =======================
        for h in range(1, 13):
            cluster_df = cluster_df.withColumn(
                f"cons_h-{h}", F.lit(None).cast("double")
            )

        for col in [
            "cons_min_24h",
            "cons_max_24h",
            "cons_mean_24h",
            "cons_std_24h",
            "cons_min_48h",
            "cons_max_48h",
            "cons_mean_48h",
            "cons_std_48h",
            "temp_min_24h",
            "temp_max_24h",
            "temp_mean_24h",
            "temp_std_24h",
            "temp_min_48h",
            "temp_max_48h",
            "temp_mean_48h",
            "temp_std_48h",
        ]:
            cluster_df = cluster_df.withColumn(col, F.lit(None).cast("double"))

        # ======================= Predict consumption for the next 24 hours =======================
        for i in range(1, 25):
            hour_to_predict = last_date + timedelta(hours=i)
            print(f"Predicting for date: {hour_to_predict}")

            # ======================= Add columns with the consumption of the 12 previous hours (just for the hour to predict) =======================
            for h in range(1, 13):
                cluster_df = cluster_df.withColumn(
                    f"cons_h-{h}",
                    F.when(
                        F.col("date") == hour_to_predict,
                        F.lag("cons_h", h).over(
                            Window.partitionBy("customer").orderBy("date")
                        ),
                    ).otherwise(F.col(f"cons_h-{h}")),
                )

            # ======================= Add consumption statistics for the previous 24 and 48 hours (just for the hour to predict) =======================
            window_24h = (
                Window.partitionBy("customer")
                .orderBy(F.col("date"))
                .rowsBetween(-24, -1)
            )
            window_48h = (
                Window.partitionBy("customer")
                .orderBy(F.col("date"))
                .rowsBetween(-48, -1)
            )

            for stat, func in [
                ("min", F.min),
                ("max", F.max),
                ("mean", F.mean),
                ("std", F.stddev),
            ]:
                cluster_df = cluster_df.withColumn(
                    f"cons_{stat}_24h",
                    F.when(
                        F.col("date") == hour_to_predict,
                        func("cons_h").over(window_24h),
                    ).otherwise(F.col(f"cons_{stat}_24h")),
                )
                cluster_df = cluster_df.withColumn(
                    f"cons_{stat}_48h",
                    F.when(
                        F.col("date") == hour_to_predict,
                        func("cons_h").over(window_48h),
                    ).otherwise(F.col(f"cons_{stat}_48h")),
                )

            # ======================= Add temperature statistics for the previous 24 and 48 hours (just for the hour to predict) =======================
            window_temp_24h = (
                Window.partitionBy("customer")
                .orderBy(F.col("date"))
                .rowsBetween(-24, -1)
            )
            window_temp_48h = (
                Window.partitionBy("customer")
                .orderBy(F.col("date"))
                .rowsBetween(-48, -1)
            )

            for stat, func in [
                ("min", F.min),
                ("max", F.max),
                ("mean", F.mean),
                ("std", F.stddev),
            ]:
                cluster_df = cluster_df.withColumn(
                    f"temp_{stat}_24h",
                    F.when(
                        F.col("date") == hour_to_predict,
                        func("temp_h").over(window_temp_24h),
                    ).otherwise(F.col(f"temp_{stat}_24h")),
                )
                cluster_df = cluster_df.withColumn(
                    f"temp_{stat}_48h",
                    F.when(
                        F.col("date") == hour_to_predict,
                        func("temp_h").over(window_temp_48h),
                    ).otherwise(F.col(f"temp_{stat}_48h")),
                )

            # ======================= Make predictions for the hour to predict =======================
            predictions = model.transform(
                cluster_df.filter(F.col("date") == hour_to_predict)
            )
            predictions = predictions.select("date", "customer", "prediction")

            # ======================== Update the main dataframe with the predictions ========================
            pred_dict = {
                (row["date"], row["customer"]): row["prediction"]
                for row in predictions.collect()
            }

            def get_pred(date, customer, current):
                return pred_dict.get((date, customer), current)

            get_pred_udf = F.udf(get_pred, DoubleType())

            cluster_df = cluster_df.withColumn(
                "cons_h",
                get_pred_udf(F.col("date"), F.col("customer"), F.col("cons_h")),
            )

            # The temperature for the hour to predict is not known, so we use the temperature of the same hour of the previous day as a proxy
            cluster_df = cluster_df.withColumn(
                "temp_h",
                F.when(
                    F.col("date") == hour_to_predict,
                    F.lag("temp_h", 24).over(
                        Window.partitionBy("customer").orderBy("date")
                    ),
                ).otherwise(F.col("temp_h")),
            )

            # Checkpoint to avoid stack overflow due to the long lineage
            cluster_df = cluster_df.checkpoint(eager=True)

        results.append(cluster_df)

    # ======================= Combine results from all clusters ========================
    final_df = results[0]
    for df in results[1:]:
        final_df = final_df.unionByName(df)

    # ======================= Save the predictions in CSV ========================
    final_df.filter(F.col("date") > last_date).select(
        "date", "customer", "cons_h"
    ).toPandas().to_csv(
        os.path.join(BASE_PATH, "data", "predictions", "clustering_predictions.csv"),
        index=False,
    )

    # Clean up checkpoints directory
    shutil.rmtree(os.path.join(BASE_PATH, "spark", "checkpoints"), ignore_errors=True)

    spark.stop()


prediction()

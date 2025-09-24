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
    model_path = os.path.join(BASE_PATH, "models", "best_model")

    df_predict = spark.read.parquet(os.path.join(data_path, "predict.parquet"))
    model = PipelineModel.load(model_path)

    df_predict = df_predict.withColumnRenamed(
        "consumption", "cons_h"
    ).withColumnRenamed("temperature", "temp_h")

    last_date = df_predict.filter(F.col("cons_h") != -1.0).agg(F.max("date")).first()[0]
    print(f"Last date with known consumption: {last_date}")

    # ======================= Initialize columns to be used in the feature engineering =======================
    for h in range(1, 13):
        df_predict = df_predict.withColumn(f"cons_h-{h}", F.lit(None).cast("double"))

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
        df_predict = df_predict.withColumn(col, F.lit(None).cast("double"))

    # ======================= Predict consumption for the next 24 hours =======================
    for i in range(1, 25):
        hour_to_predict = last_date + timedelta(hours=i)
        print(f"Predicting for date: {hour_to_predict}")

        # ======================= Add columns with the consumption of the 12 previous hours (just for the hour to predict) =======================
        for h in range(1, 13):
            df_predict = df_predict.withColumn(
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
            Window.partitionBy("customer").orderBy(F.col("date")).rowsBetween(-24, -1)
        )
        window_48h = (
            Window.partitionBy("customer").orderBy(F.col("date")).rowsBetween(-48, -1)
        )

        df_predict = df_predict.withColumn(
            "cons_min_24h",
            F.when(
                F.col("date") == hour_to_predict,
                F.when(
                    F.count("cons_h").over(window_48h) >= 48,
                    F.min("cons_h").over(window_24h),
                ),
            ).otherwise(F.col("cons_min_24h")),
        )
        df_predict = df_predict.withColumn(
            "cons_max_24h",
            F.when(
                F.col("date") == hour_to_predict,
                F.when(
                    F.count("cons_h").over(window_48h) >= 48,
                    F.max("cons_h").over(window_24h),
                ),
            ).otherwise(F.col("cons_max_24h")),
        )
        df_predict = df_predict.withColumn(
            "cons_mean_24h",
            F.when(
                F.col("date") == hour_to_predict,
                F.when(
                    F.count("cons_h").over(window_48h) >= 48,
                    F.mean("cons_h").over(window_24h),
                ),
            ).otherwise(F.col("cons_mean_24h")),
        )
        df_predict = df_predict.withColumn(
            "cons_std_24h",
            F.when(
                F.col("date") == hour_to_predict,
                F.when(
                    F.count("cons_h").over(window_48h) >= 48,
                    F.stddev("cons_h").over(window_24h),
                ),
            ).otherwise(F.col("cons_std_24h")),
        )
        df_predict = df_predict.withColumn(
            "cons_min_48h",
            F.when(
                F.col("date") == hour_to_predict,
                F.when(
                    F.count("cons_h").over(window_48h) >= 48,
                    F.min("cons_h").over(window_48h),
                ),
            ).otherwise(F.col("cons_min_48h")),
        )
        df_predict = df_predict.withColumn(
            "cons_max_48h",
            F.when(
                F.col("date") == hour_to_predict,
                F.when(
                    F.count("cons_h").over(window_48h) >= 48,
                    F.max("cons_h").over(window_48h),
                ),
            ).otherwise(F.col("cons_max_48h")),
        )
        df_predict = df_predict.withColumn(
            "cons_mean_48h",
            F.when(
                F.col("date") == hour_to_predict,
                F.when(
                    F.count("cons_h").over(window_48h) >= 48,
                    F.mean("cons_h").over(window_48h),
                ),
            ).otherwise(F.col("cons_mean_48h")),
        )
        df_predict = df_predict.withColumn(
            "cons_std_48h",
            F.when(
                F.col("date") == hour_to_predict,
                F.when(
                    F.count("cons_h").over(window_48h) >= 48,
                    F.stddev("cons_h").over(window_48h),
                ),
            ).otherwise(F.col("cons_std_48h")),
        )

        # ======================= Add temperature statistics for the previous 24 and 48 hours (just for the hour to predict) =======================
        window_temp_24h = (
            Window.partitionBy("customer").orderBy(F.col("date")).rowsBetween(-24, -1)
        )
        window_temp_48h = (
            Window.partitionBy("customer").orderBy(F.col("date")).rowsBetween(-48, -1)
        )

        df_predict = df_predict.withColumn(
            "temp_min_24h",
            F.when(
                F.col("date") == hour_to_predict,
                F.when(
                    F.count("temp_h").over(window_temp_48h) >= 48,
                    F.min("temp_h").over(window_temp_24h),
                ),
            ).otherwise(F.col("temp_min_24h")),
        )
        df_predict = df_predict.withColumn(
            "temp_max_24h",
            F.when(
                F.col("date") == hour_to_predict,
                F.when(
                    F.count("temp_h").over(window_temp_48h) >= 48,
                    F.max("temp_h").over(window_temp_24h),
                ),
            ).otherwise(F.col("temp_max_24h")),
        )
        df_predict = df_predict.withColumn(
            "temp_mean_24h",
            F.when(
                F.col("date") == hour_to_predict,
                F.when(
                    F.count("temp_h").over(window_temp_48h) >= 48,
                    F.mean("temp_h").over(window_temp_24h),
                ),
            ).otherwise(F.col("temp_mean_24h")),
        )
        df_predict = df_predict.withColumn(
            "temp_std_24h",
            F.when(
                F.col("date") == hour_to_predict,
                F.when(
                    F.count("temp_h").over(window_temp_48h) >= 48,
                    F.stddev("temp_h").over(window_temp_24h),
                ),
            ).otherwise(F.col("temp_std_24h")),
        )
        df_predict = df_predict.withColumn(
            "temp_min_48h",
            F.when(
                F.col("date") == hour_to_predict,
                F.when(
                    F.count("temp_h").over(window_temp_48h) >= 48,
                    F.min("temp_h").over(window_temp_48h),
                ),
            ).otherwise(F.col("temp_min_48h")),
        )
        df_predict = df_predict.withColumn(
            "temp_max_48h",
            F.when(
                F.col("date") == hour_to_predict,
                F.when(
                    F.count("temp_h").over(window_temp_48h) >= 48,
                    F.max("temp_h").over(window_temp_48h),
                ),
            ).otherwise(F.col("temp_max_48h")),
        )
        df_predict = df_predict.withColumn(
            "temp_mean_48h",
            F.when(
                F.col("date") == hour_to_predict,
                F.when(
                    F.count("temp_h").over(window_temp_48h) >= 48,
                    F.mean("temp_h").over(window_temp_48h),
                ),
            ).otherwise(F.col("temp_mean_48h")),
        )
        df_predict = df_predict.withColumn(
            "temp_std_48h",
            F.when(
                F.col("date") == hour_to_predict,
                F.when(
                    F.count("temp_h").over(window_temp_48h) >= 48,
                    F.stddev("temp_h").over(window_temp_48h),
                ),
            ).otherwise(F.col("temp_std_48h")),
        )

        # ======================= Make predictions for the hour to predict =======================
        predictions = model.transform(
            df_predict.filter(F.col("date") == hour_to_predict)
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

        df_predict = df_predict.withColumn(
            "cons_h", get_pred_udf(F.col("date"), F.col("customer"), F.col("cons_h"))
        )

        # The temperature for the hour to predict is not known, so we use the temperature of the same hour of the previous day as a proxy
        df_predict = df_predict.withColumn(
            "temp_h",
            F.when(
                F.col("date") == hour_to_predict,
                F.lag("temp_h", 24).over(
                    Window.partitionBy("customer").orderBy("date")
                ),
            ).otherwise(F.col("temp_h")),
        )

        # Checkpoint to avoid stack overflow due to the long lineage
        df_predict = df_predict.checkpoint(eager=True)

    # ======================= Save the predictions in CSV ========================
    df_predict.filter(F.col("date") > last_date).select(
        "date", "customer", "cons_h"
    ).toPandas().to_csv(
        os.path.join(BASE_PATH, "data", "predictions", "predictions.csv"), index=False
    )

    # Clean up checkpoints directory
    shutil.rmtree(os.path.join(BASE_PATH, "spark", "checkpoints"), ignore_errors=True)

    spark.stop()


prediction()

import os

from dotenv import load_dotenv
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

load_dotenv()
BASE_PATH = os.getenv("BASE_PATH")


def clustering():
    spark = SparkSession.builder.appName("Clustering").getOrCreate()

    file_path = os.path.join(BASE_PATH, "data", "processed", "data.parquet")
    df = spark.read.parquet(file_path)

    # ======================= Compute statistics per customer =======================
    df = df.withColumn("hour", F.hour("date")).withColumn(
        "day_of_week", F.dayofweek("date")
    )

    # Compute basic statistics per customer
    stats_df = df.groupBy("customer").agg(
        F.mean("consumption").alias("mean_consumption"),
        F.stddev("consumption").alias("std_consumption"),
        F.min("consumption").alias("min_consumption"),
        F.max("consumption").alias("max_consumption"),
        F.expr("percentile(consumption, 0.5)").alias("median_consumption"),
    )

    # Compute average consumption per hour for each customer
    hourly_df = (
        df.groupBy("customer", "hour")
        .agg(F.mean("consumption").alias("avg_hourly_consumption"))
        .groupBy("customer")
        .pivot("hour")
        .agg(F.first("avg_hourly_consumption"))
    )

    for h in range(24):
        hourly_df = hourly_df.withColumnRenamed(str(h), f"hour_{h}_consumption")

    # Compute average consumption per day of the week for each customer
    weekly_df = (
        df.groupBy("customer", "day_of_week")
        .agg(F.mean("consumption").alias("avg_daily_consumption"))
        .groupBy("customer")
        .pivot("day_of_week")
        .agg(F.first("avg_daily_consumption"))
    )

    for d in range(1, 8):
        weekly_df = weekly_df.withColumnRenamed(
            str(d), f"day_{d}_consumption"
        )  # 1=Sunday, 2=Monday, ..., 7=Saturday

    # Correlate consumption with temperature per customer
    temp_corr_df = df.groupBy("customer").agg(
        (F.covar_samp("consumption", "temperature") / F.variance("temperature")).alias(
            "temp_consumption_correlation"
        )
    )

    # Join all statistics into a single DataFrame
    final_df = (
        stats_df.join(hourly_df, on="customer", how="left")
        .join(weekly_df, on="customer", how="left")
        .join(temp_corr_df, on="customer", how="left")
    )

    # ======================= Clustering =======================
    feature_cols = [c for c in final_df.columns if c != "customer"]

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_assembled")
    scaler = StandardScaler(
        inputCol="features_assembled", outputCol="features", withMean=True, withStd=True
    )
    kmeans = KMeans(k=5, seed=42, featuresCol="features", predictionCol="cluster")

    pipeline = Pipeline(stages=[assembler, scaler, kmeans])

    model = pipeline.fit(final_df)

    clustered_df = model.transform(final_df).select("customer", "cluster")

    clustered_df.show(truncate=False)
    clustered_df.groupBy("cluster").count().orderBy("cluster").show()

    output_path = os.path.join(
        BASE_PATH, "data", "processed", "customer_clusters.parquet"
    )
    clustered_df.write.mode("overwrite").parquet(output_path)

    spark.stop()


clustering()

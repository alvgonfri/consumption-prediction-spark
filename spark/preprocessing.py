import os
from datetime import timedelta

import holidays
from dotenv import load_dotenv
from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoder, StringIndexer
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

load_dotenv()
BASE_PATH = os.getenv("BASE_PATH")


def preprocessing():
    spark = SparkSession.builder.appName("Preprocessing").getOrCreate()

    file_path = os.path.join(BASE_PATH, "data", "processed", "data.parquet")
    df = spark.read.parquet(file_path)

    # ======================= Add hours to be predicted =======================
    last_date = df.agg(F.max("date")).first()[0]
    customers = df.select("customer").distinct().collect()

    # Add new rows for the next 24 hours for each customer
    new_rows = []
    for i in range(1, 25):
        hour_to_predict = last_date + timedelta(hours=i)

        new_rows.extend(
            [(hour_to_predict, row["customer"], -1.0, -1.0) for row in customers]
        )  # Consumption and temperature set to -1.0 as they are unknown

    new_rows_df = spark.createDataFrame(new_rows, schema=df.schema)

    df = df.union(new_rows_df)

    # ======================= Add day_of_week column =======================
    es_holidays = holidays.Spain(years=[2019])
    holiday_dates = [d.strftime("%Y-%m-%d") for d in es_holidays.keys()]

    df = df.withColumn("date_only", F.to_date("date"))

    df = df.withColumn(
        "day_of_week",
        F.when(
            F.date_format("date_only", "yyyy-MM-dd").isin(holiday_dates),
            F.lit("Sunday/Holiday"),
        )
        .when(F.dayofweek("date") == 2, F.lit("Monday"))
        .when(F.dayofweek("date").isin(3, 4, 5), F.lit("Midweek"))
        .when(F.dayofweek("date") == 6, F.lit("Friday"))
        .when(F.dayofweek("date") == 7, F.lit("Saturday"))
        .when(F.dayofweek("date") == 1, F.lit("Sunday/Holiday"))
        .otherwise(F.lit("Unknown")),
    )

    df = df.drop("date_only")

    # ======================= Apply one-hot encoding to day_of_week =======================
    indexer = StringIndexer(inputCol="day_of_week", outputCol="day_of_week_index")

    encoder = OneHotEncoder(
        inputCols=["day_of_week_index"],
        outputCols=["day_of_week_ohe"],
    )

    pipeline = Pipeline(stages=[indexer, encoder])
    df = pipeline.fit(df).transform(df)

    df = df.drop("day_of_week", "day_of_week_index")

    # ======================= Split the dataset into data to predict and the rest =======================
    first_prediction_date = last_date + timedelta(hours=1)
    cutoff_date = first_prediction_date - F.expr("INTERVAL 48 HOURS")

    df_predict = df.filter(F.col("date") >= cutoff_date)
    df = df.filter(F.col("date") < cutoff_date)

    # ======================= Add columns with the consumption of the 12 previous hours =======================
    df = df.withColumnRenamed("consumption", "cons_h")

    for i in range(1, 13):
        df = df.withColumn(
            f"cons_h-{i}",
            F.lag("cons_h", i).over(
                Window.partitionBy("customer").orderBy(F.col("date"))
            ),
        )

    # ======================= Add consumption statistics for the previous 24 and 48 hours =======================
    window_24h = (
        Window.partitionBy("customer").orderBy(F.col("date")).rowsBetween(-24, -1)
    )
    window_48h = (
        Window.partitionBy("customer").orderBy(F.col("date")).rowsBetween(-48, -1)
    )

    df = df.withColumn(
        "cons_min_24h",
        F.when(
            F.count("cons_h").over(window_48h) >= 48, F.min("cons_h").over(window_24h)
        ),
    )
    df = df.withColumn(
        "cons_max_24h",
        F.when(
            F.count("cons_h").over(window_48h) >= 48, F.max("cons_h").over(window_24h)
        ),
    )
    df = df.withColumn(
        "cons_mean_24h",
        F.when(
            F.count("cons_h").over(window_48h) >= 48, F.mean("cons_h").over(window_24h)
        ),
    )
    df = df.withColumn(
        "cons_std_24h",
        F.when(
            F.count("cons_h").over(window_48h) >= 48,
            F.stddev("cons_h").over(window_24h),
        ),
    )

    df = df.withColumn(
        "cons_min_48h",
        F.when(
            F.count("cons_h").over(window_48h) >= 48, F.min("cons_h").over(window_48h)
        ),
    )
    df = df.withColumn(
        "cons_max_48h",
        F.when(
            F.count("cons_h").over(window_48h) >= 48, F.max("cons_h").over(window_48h)
        ),
    )
    df = df.withColumn(
        "cons_mean_48h",
        F.when(
            F.count("cons_h").over(window_48h) >= 48, F.mean("cons_h").over(window_48h)
        ),
    )
    df = df.withColumn(
        "cons_std_48h",
        F.when(
            F.count("cons_h").over(window_48h) >= 48,
            F.stddev("cons_h").over(window_48h),
        ),
    )

    # ======================= Add temperature statistics for the previous 24 and 48 hours =======================
    df = df.withColumnRenamed("temperature", "temp_h")

    window_temp_24h = (
        Window.partitionBy("customer").orderBy(F.col("date")).rowsBetween(-24, -1)
    )
    window_temp_48h = (
        Window.partitionBy("customer").orderBy(F.col("date")).rowsBetween(-48, -1)
    )

    df = df.withColumn(
        "temp_min_24h",
        F.when(
            F.count("temp_h").over(window_temp_48h) >= 48,
            F.min("temp_h").over(window_temp_24h),
        ),
    )
    df = df.withColumn(
        "temp_max_24h",
        F.when(
            F.count("temp_h").over(window_temp_48h) >= 48,
            F.max("temp_h").over(window_temp_24h),
        ),
    )
    df = df.withColumn(
        "temp_mean_24h",
        F.when(
            F.count("temp_h").over(window_temp_48h) >= 48,
            F.mean("temp_h").over(window_temp_24h),
        ),
    )
    df = df.withColumn(
        "temp_std_24h",
        F.when(
            F.count("temp_h").over(window_temp_48h) >= 48,
            F.stddev("temp_h").over(window_temp_24h),
        ),
    )

    df = df.withColumn(
        "temp_min_48h",
        F.when(
            F.count("temp_h").over(window_temp_48h) >= 48,
            F.min("temp_h").over(window_temp_48h),
        ),
    )
    df = df.withColumn(
        "temp_max_48h",
        F.when(
            F.count("temp_h").over(window_temp_48h) >= 48,
            F.max("temp_h").over(window_temp_48h),
        ),
    )
    df = df.withColumn(
        "temp_mean_48h",
        F.when(
            F.count("temp_h").over(window_temp_48h) >= 48,
            F.mean("temp_h").over(window_temp_48h),
        ),
    )
    df = df.withColumn(
        "temp_std_48h",
        F.when(
            F.count("temp_h").over(window_temp_48h) >= 48,
            F.stddev("temp_h").over(window_temp_48h),
        ),
    )

    # ======================= Drop rows with null values =======================
    df = df.dropna()

    # ======================= Split the dataset into train, val and test sets =======================

    # Calculate the cutoff dates for splitting
    train_frac = 0.7
    val_frac = 0.15

    min_date, max_date = df.agg(F.min("date"), F.max("date")).first()

    total_hours = (max_date - min_date).days * 24 + (
        max_date - min_date
    ).seconds // 3600

    train_cutoff = min_date + F.expr(f"INTERVAL {int(total_hours * train_frac)} HOURS")
    val_cutoff = min_date + F.expr(
        f"INTERVAL {int(total_hours * (train_frac + val_frac))} HOURS"
    )

    # Split the DataFrame
    train_df = df.filter(F.col("date") <= train_cutoff)
    val_df = df.filter((F.col("date") > train_cutoff) & (F.col("date") <= val_cutoff))
    test_df = df.filter(F.col("date") > val_cutoff)

    # ======================= Save the datasets =======================
    output_path = os.path.join(BASE_PATH, "data", "processed")

    train_df.write.mode("overwrite").parquet(os.path.join(output_path, "train.parquet"))
    val_df.write.mode("overwrite").parquet(os.path.join(output_path, "val.parquet"))
    test_df.write.mode("overwrite").parquet(os.path.join(output_path, "test.parquet"))

    df_predict.write.mode("overwrite").parquet(
        os.path.join(output_path, "predict.parquet")
    )

    spark.stop()


preprocessing()

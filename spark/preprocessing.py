import os

import holidays
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

load_dotenv()
BASE_PATH = os.getenv("BASE_PATH")


def preprocessing():
    spark = SparkSession.builder.appName("Preprocessing").getOrCreate()

    file_path = os.path.join(BASE_PATH, "data", "processed", "data.parquet")
    df = spark.read.parquet(file_path)

    # ======================= Add day_of_week column =======================
    es_holidays = holidays.Spain(years=[2019])
    holiday_dates = [d.strftime("%Y-%m-%d") for d in es_holidays.keys()]
    print(f"Holiday dates: {holiday_dates}")

    df = df.withColumn("date_only", F.to_date("date")).withColumn(
        "hour", F.hour("date")
    )

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
    df.withColumnRenamed("temperature", "temp_h")

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

    df.show(100, truncate=False)

    spark.stop()


preprocessing()

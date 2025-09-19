import os
from datetime import datetime, timedelta

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

load_dotenv()
BASE_PATH = os.getenv("BASE_PATH")


def data_ingestion():
    spark = SparkSession.builder.appName("DataIngestion").getOrCreate()

    # Read CSV file
    file_path = os.path.join(BASE_PATH, "data", "processed", "data.csv")

    df = spark.read.csv(file_path, header=True, inferSchema=True)

    # Filter rows older than current date minus 2 days
    now = datetime.now()
    limit = now - timedelta(days=2)

    limit_month = limit.month
    limit_day = limit.day
    limit_hour = limit.hour
    limit_minute = limit.minute

    print(f"Limit date and time: {limit}")

    month_condition = F.month("date") < limit_month

    day_condition = (F.month("date") == limit_month) & (
        F.dayofmonth("date") < limit_day
    )

    hour_condition = (
        (F.month("date") == limit_month)
        & (F.dayofmonth("date") == limit_day)
        & (F.hour("date") < limit_hour)
    )

    minute_condition = (
        (F.month("date") == limit_month)
        & (F.dayofmonth("date") == limit_day)
        & (F.hour("date") == limit_hour)
        & (F.minute("date") <= limit_minute)
    )

    df = df.filter(month_condition | day_condition | hour_condition | minute_condition)

    # Save the filtered DataFrame in Parquet format
    output_path = os.path.join(BASE_PATH, "data", "processed", "data.parquet")
    df.write.mode("overwrite").parquet(output_path)

    # Show the DataFrame
    # df.show(truncate=False)

    # print(f"Number of rows: {df.count()}")

    # df_grouped = df.groupBy("customer").count()

    # df_grouped.orderBy(df_grouped["count"].desc()).show(truncate=False)

    # df_grouped.orderBy(df_grouped["count"].asc()).show(truncate=False)

    spark.stop()


data_ingestion()

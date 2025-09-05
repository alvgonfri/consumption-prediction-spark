import os
from datetime import datetime

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

    # Filter rows based on current date and time
    now = datetime.now()
    current_month = now.month
    current_day = now.day
    current_hour = now.hour
    current_minute = now.minute

    print(f"Current date and time: {now}")

    month_condition = F.month("date") < current_month

    day_condition = (F.month("date") == current_month) & (
        F.dayofmonth("date") < current_day
    )

    hour_condition = (
        (F.month("date") == current_month)
        & (F.dayofmonth("date") == current_day)
        & (F.hour("date") < current_hour)
    )

    minute_condition = (
        (F.month("date") == current_month)
        & (F.dayofmonth("date") == current_day)
        & (F.hour("date") == current_hour)
        & (F.minute("date") <= current_minute)
    )

    df = df.filter(month_condition | day_condition | hour_condition | minute_condition)

    # Show the DataFrame
    df.show(truncate=False)

    print(f"Number of rows: {df.count()}")

    df_grouped = df.groupBy("customer").count()

    df_grouped.orderBy(df_grouped["count"].desc()).show(truncate=False)

    df_grouped.orderBy(df_grouped["count"].asc()).show(truncate=False)

    spark.stop()


data_ingestion()

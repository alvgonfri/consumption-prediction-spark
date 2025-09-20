import os

from dotenv import load_dotenv
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

load_dotenv()
BASE_PATH = os.getenv("BASE_PATH")


def prediction():
    spark = SparkSession.builder.appName("Prediction").getOrCreate()

    data_path = os.path.join(BASE_PATH, "data", "processed")
    model_path = os.path.join(BASE_PATH, "models", "best_model")

    predict_df = spark.read.parquet(os.path.join(data_path, "predict.parquet"))
    model = PipelineModel.load(model_path)

    # ======================= Select hour to predict =======================
    max_date = predict_df.agg(F.max("date")).first()[0]
    predict_df = predict_df.filter(F.col("date") == max_date)

    # ======================= Make predictions =======================
    predictions = model.transform(predict_df)
    predictions = predictions.select("date", "customer", "prediction")
    predictions = predictions.withColumnRenamed("prediction", "cons_h_predicted")

    predictions.show(truncate=False)

    spark.stop()


prediction()

import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()
BASE_PATH = os.getenv("BASE_PATH")

if __name__ == "__main__":
    # Create Spark session
    spark = SparkSession.builder \
        .appName("CountLinesExample") \
        .getOrCreate()

    # Create an example text file
    file_path = f"{BASE_PATH}/spark/example_text_file.txt"
    with open(file_path, "w") as f:
        f.write("Hello World\n")
        f.write("This is a test\n")
        f.write("With three lines\n")

    # Read the text file
    text_rdd = spark.sparkContext.textFile(file_path)

    # Count the number of lines
    num_lines = text_rdd.count()

    print(f"Number of lines: {num_lines}")

    # Delete the text file
    if os.path.exists(file_path):
        os.remove(file_path)

    # Stop Spark session
    spark.stop()

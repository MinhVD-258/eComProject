from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import os

def main():
    spark = SparkSession(SparkContext(conf=SparkConf()).getOrCreate())

    df = spark.read.option("header", "true") \
        .csv(f"s3a://{os.getenv('SPARK_APPLICATION_ARGS','ecommerce/2025-04-10')}/raw.csv")

    df = df.dropDuplicates()
    df = df.dropna(subset=['user_session'])

    df.coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("header", "true") \
        .option("delimiter", ",") \
        .csv(f"s3a://{os.getenv('SPARK_APPLICATION_ARGS')}/formatted")

    spark.stop()


if __name__ == "__main__":
    main()
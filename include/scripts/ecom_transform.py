import os
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import when, split, substring, to_timestamp, to_date


def log_step(message, df):
    """Helper function to log processing steps"""
    print(f"\n{message}")
    print(f"Số dòng hiện tại: {df.count()}")
    df.printSchema()
    return df


from pyspark.sql import SparkSession

from pyspark.sql.functions import col, lit, mean, count, udf
from pyspark.sql.types import DoubleType, StringType
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler, OneHotEncoder
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline


def clustering_based_imputation(df, target_col, feature_cols, k=50):

    spark = df.sql_ctx.sparkSession
    target_type = dict(df.dtypes)[target_col]
    is_numeric = target_type in ('int', 'bigint', 'double', 'float')

    stages = []
    processed_features = []

    for feature in feature_cols:
        feature_type = dict(df.dtypes)[feature]
        if feature_type == 'string':
            indexer = StringIndexer(inputCol=feature, outputCol=f"{feature}_index", handleInvalid="keep")
            encoder = OneHotEncoder(inputCol=f"{feature}_index", outputCol=f"{feature}_encoded")
            stages += [indexer, encoder]
            processed_features.append(f"{feature}_encoded")
        else:
            processed_features.append(feature)

    assembler = VectorAssembler(inputCols=processed_features, outputCol="features")
    scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
    stages += [assembler, scaler]

    non_null_df = df.filter(col(target_col).isNotNull()).cache()
    null_df = df.filter(col(target_col).isNull())

    if non_null_df.count() == 0:
        fill_value = df.select(mean(target_col)).first()[0] if is_numeric else \
            df.groupBy(target_col).count().orderBy("count", ascending=False).first()[0]
        return df.fillna({target_col: fill_value})

    kmeans = KMeans(featuresCol="scaled_features", k=k, seed=42)
    stages.append(kmeans)
    pipeline = Pipeline(stages=stages)
    model = pipeline.fit(non_null_df)

    clustered_non_null = model.transform(non_null_df)
    clustered_null = model.transform(null_df)

    if is_numeric:
        cluster_values = clustered_non_null.groupBy("prediction") \
            .agg(mean(target_col).alias("fill_value")) \
            .collect()
    else:
        cluster_values = clustered_non_null.groupBy("prediction", target_col) \
            .agg(count("*").alias("cnt")) \
            .orderBy("prediction", "cnt", ascending=[True, False]) \
            .dropDuplicates(["prediction"]) \
            .select("prediction", target_col) \
            .collect()

    fill_map = {row["prediction"]: row["fill_value" if is_numeric else target_col] for row in cluster_values}
    broadcast_map = spark.sparkContext.broadcast(fill_map)

    def fill_with_cluster_value(prediction):
        return broadcast_map.value.get(prediction)

    fill_udf = udf(fill_with_cluster_value, DoubleType() if is_numeric else StringType())

    imputed_null = clustered_null.withColumn(target_col, fill_udf(col("prediction")))

    result_df = clustered_non_null.select(df.columns).unionByName(imputed_null.select(df.columns))

    columns_to_drop = [c for c in result_df.columns
                       if c.endswith("_index") or c.endswith("_encoded") or
                       c in ["features", "scaled_features", "prediction"]]
    return result_df.drop(*columns_to_drop)


def main():
    spark = SparkSession(SparkContext(conf=SparkConf()).getOrCreate())

    df = spark.read.option("header", "true") \
        .parquet(f"s3a://{os.getenv('SPARK_APPLICATION_ARGS','ecommerce/2025-04-10')}/raw.parquet")

    df = log_step("Tạo cột price_predict...",
                    df.withColumn("price_predict",
                                when((col("price") == 0) | (col("price").isNull()),
                                        lit(None)).otherwise(col("price"))))

    print("\nXử lý missing data với clustering...")

    feature_cols = [c for c in df.columns
                    if c not in ["category_code", "brand", "price", "price_predict", "event_time", "user_session"]]

    print("Impute brand với clustering...")
    df = clustering_based_imputation(
        df=df,
        target_col="brand",
        feature_cols=feature_cols,
        k = 50
    )

    feature_cols.append("brand")
    print("Impute category_code với clustering...")
    df = clustering_based_imputation(
        df=df,
        target_col="category_code",
        feature_cols=feature_cols,
        k = 50
    )

    feature_cols.append("category_code")
    print("Impute price_predict với clustering...")
    df = clustering_based_imputation(
        df=df,
        target_col="price_predict",
        feature_cols=feature_cols,
        k = 50
    )

    df = log_step("Sau khi xử lý missing data:", df)

    df = log_step("Tách cột thời gian...",
                    df.withColumn("date", substring("event_time", 1, 10))
                    .withColumn("time", substring("event_time", 12, 12)))

    split_cols = split(col("category_code"), "\.")
    for i in range(1, 5):
        df = df.withColumn(f"sub_category_{i}",
                            when(split_cols.getItem(i - 1).isNotNull(),
                                split_cols.getItem(i - 1)).otherwise(lit(None)))

    df = log_step("Sau khi tách category_code:", df)

    output_cols = [
        "event_time", "date", "time", "event_type", "product_id", "category_id",
        "category_code", "sub_category_1", "sub_category_2",
        "sub_category_3", "sub_category_4", "brand",
        "price", "price_predict", "user_id", "user_session"
    ]
    final_df = df.select(output_cols)


    (final_df \
        .write \
        .mode("overwrite") \
        .option("header", "true") \
        .option("delimiter", ",") \
        .csv(f"s3a://{os.getenv('SPARK_APPLICATION_ARGS')}/formatted"))

    spark.stop()


if __name__ == "__main__":
    main()
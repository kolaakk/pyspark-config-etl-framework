from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def transform(df: DataFrame, spark: SparkSession, config: dict) -> DataFrame:
    out = (
        df.select(
            F.col("customer_id").cast("string").alias("customer_id"),
            F.trim(F.col("customer_name")).alias("customer_name"),
            F.lower(F.col("email")).alias("email"),
            F.col("country").alias("country"),
        )
        .withColumn("country_partition", F.col("country"))
    )
    return out

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def transform(df: DataFrame, spark: SparkSession, config: dict) -> DataFrame:
    params = config["transform"].get("params", {})
    currency = params.get("currency", "EUR")

    out = (
        df.select(
            F.col("product_id").cast("string").alias("product_id"),
            F.trim(F.col("product_name")).alias("product_name"),
            F.col("product_price").cast("decimal(18,2)").alias("product_price"),
            F.initcap(F.col("product_Category")).alias("product_category"),
        )
        .withColumn("currency", F.lit(currency))
        .withColumn("category_partition", F.col("product_category"))
        .filter(F.col("product_id").isNotNull())
    )
    return out

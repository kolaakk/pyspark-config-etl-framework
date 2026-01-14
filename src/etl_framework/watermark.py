from __future__ import annotations

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

#Stores last watermark in a Delta table (works both on Databricks and open-source)
def ensure_watermark_table(spark: SparkSession, table: str) -> None:
    # Create DB if using db.table syntax
    if "." in table:
        db = table.split(".", 1)[0]
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")

    schema = StructType([
        StructField("job_key", StringType(), False),
        StructField("watermark_value", StringType(), True),
        StructField("updated_ts", TimestampType(), True),
    ])

    if not spark.catalog.tableExists(table):
        empty_df = spark.createDataFrame([], schema)
        empty_df.write.format("delta").mode("overwrite").saveAsTable(table)


def read_watermark(spark: SparkSession, table: str, job_key: str) -> str | None:
    ensure_watermark_table(spark, table)
    df = spark.table(table).where(F.col("job_key") == job_key).select("watermark_value")
    row = df.limit(1).collect()
    return row[0]["watermark_value"] if row else None


def write_watermark(spark: SparkSession, table: str, job_key: str, watermark_value: str) -> None:
    ensure_watermark_table(spark, table)
    # MERGE into watermark table (simple)
    from delta.tables import DeltaTable

    delta_t = DeltaTable.forName(spark, table)
    src = spark.createDataFrame([(job_key, watermark_value)], ["job_key", "watermark_value"]) \
               .withColumn("updated_ts", F.current_timestamp())

    (
        delta_t.alias("t")
        .merge(src.alias("s"), "t.job_key = s.job_key")
        .whenMatchedUpdate(set={
            "watermark_value": "s.watermark_value",
            "updated_ts": "s.updated_ts"
        })
        .whenNotMatchedInsert(values={
            "job_key": "s.job_key",
            "watermark_value": "s.watermark_value",
            "updated_ts": "s.updated_ts"
        })
        .execute()
    )

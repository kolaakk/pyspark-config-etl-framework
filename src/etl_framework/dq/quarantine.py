from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F

def ensure_db(spark: SparkSession, db: str) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")

def append_quarantine(
    spark: SparkSession,
    df: DataFrame,
    table_fqn: str,
    max_rows: int
) -> None:
    db = table_fqn.split(".", 1)[0]
    ensure_db(spark, db)

    (
        df.limit(max_rows)
          .withColumn("quarantine_written_ts", F.current_timestamp())
          .write.format("delta").mode("append").saveAsTable(table_fqn)
    )

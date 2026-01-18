from __future__ import annotations

from typing import Optional
from pyspark.sql import DataFrame, SparkSession, functions as F


def _ensure_db(spark: SparkSession, db: str) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")


def append_results(
    spark: SparkSession,
    results_df: DataFrame,
    results_table: str,
) -> None:
    if "." in results_table:
        _ensure_db(spark, results_table.split(".", 1)[0])
    (
        results_df
        .withColumn("written_ts", F.current_timestamp())
        .write.format("delta").mode("append").saveAsTable(results_table)
    )


def append_failed_records(
    spark: SparkSession,
    failed_df: DataFrame,
    failed_table: str,
    sample_limit: int,
) -> None:
    if sample_limit <= 0:
        return
    if "." in failed_table:
        _ensure_db(spark, failed_table.split(".", 1)[0])

    (
        failed_df.limit(sample_limit)
        .withColumn("written_ts", F.current_timestamp())
        .write.format("delta").mode("append").saveAsTable(failed_table)
    )
    
def append_dashboard(
    spark: SparkSession,
    dashboard_df: DataFrame,
    dashboard_table: str,
) -> None:
    if "." in dashboard_table:
        _ensure_db(spark, dashboard_table.split(".", 1)[0])
    (
        dashboard_df
        .withColumn("written_ts", F.current_timestamp())
        .write.format("delta").mode("append").saveAsTable(dashboard_table)
    )

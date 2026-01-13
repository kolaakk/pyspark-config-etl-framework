from __future__ import annotations

import os
import sys
from pyspark.sql import SparkSession

from etl_framework.config import load_config
from etl_framework.job import SparkETLJob

#This auto-configures Delta extensions only if not on Databricks.

def _is_databricks() -> bool:
    # Databricks typically sets one of these.
    return (
        "DATABRICKS_RUNTIME_VERSION" in os.environ
        or "DB_IS_DRIVER" in os.environ
    )


def build_spark(app_name: str) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)

    if not _is_databricks():
        # open-source delta-spark needs these extensions
        builder = (
            builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                   .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )

    return builder.getOrCreate()


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("Usage: spark-submit -m etl_framework.runner <config.json>")

    cfg = load_config(sys.argv[1])

    spark = build_spark(cfg.job_name)
    SparkETLJob(spark, cfg).run()
    spark.stop()


if __name__ == "__main__":
    main()

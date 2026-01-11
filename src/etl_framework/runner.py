from __future__ import annotations

import sys
from pyspark.sql import SparkSession

from etl_framework.config import load_config
from etl_framework.job import SparkETLJob


def build_spark(app_name: str) -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("Usage: spark-submit -m etl_framework.runner <config.json>")

    config_path = sys.argv[1]
    cfg = load_config(config_path)

    spark = build_spark(cfg.job_name)
    SparkETLJob(spark, cfg).run()
    spark.stop()


if __name__ == "__main__":
    main()

from __future__ import annotations

import importlib
from typing import Any, Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from etl_framework.config import JobConfig, to_dict
from etl_framework.io import read_source, write_delta


class SparkETLJob:
    def __init__(self, spark: SparkSession, config: JobConfig):
        self.spark = spark
        self.config = config

    def run(self) -> None:
        self._log(f"Starting job: {self.config.job_name}")

        df_in = read_source(
            self.spark,
            self.config.source.format,
            self.config.source.path,
            self.config.source.options,
        )
        self._log(f"Read complete. Rows: {df_in.count()}")

        self._validate_required_columns(df_in)

        df_out = self._apply_transform(df_in)
        self._log(f"Transform complete. Rows: {df_out.count()}")

        df_out = self._post_transform(df_out)

        write_delta(
            df_out,
            mode=self.config.target.mode,
            table=self.config.target.table,
            path=self.config.target.path,
            partition_by=self.config.target.partition_by,
            overwrite_schema=self.config.target.overwrite_schema,
        )

        self._log(f"Job finished: {self.config.job_name}")

    def _apply_transform(self, df: DataFrame) -> DataFrame:
        func_path = self.config.transform.function
        module_path, func_name = func_path.rsplit(".", 1)

        self._log(f"Loading transform: {func_path}")
        module = importlib.import_module(module_path)
        fn = getattr(module, func_name)

        cfg_dict: Dict[str, Any] = to_dict(self.config)
        return fn(df, self.spark, cfg_dict)

    def _validate_required_columns(self, df: DataFrame) -> None:
        missing = [c for c in self.config.required_columns if c not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

    def _post_transform(self, df: DataFrame) -> DataFrame:
        if self.config.drop_duplicates_on:
            self._log(f"Dropping duplicates on: {self.config.drop_duplicates_on}")
            df = df.dropDuplicates(self.config.drop_duplicates_on)

        df = (
            df.withColumn("_ingest_ts", F.current_timestamp())
              .withColumn("_job_name", F.lit(self.config.job_name))
        )
        return df

    def _log(self, msg: str) -> None:
        print(f"[{self.config.job_name}] {msg}")

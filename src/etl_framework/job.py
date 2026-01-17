from __future__ import annotations

import importlib
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from etl_framework.config import JobConfig
from etl_framework.strategies.write_strategy import run_write
from etl_framework.strategies.merge_scd1 import run_merge_scd1
from etl_framework.strategies.merge_scd2 import run_scd2
from etl_framework.watermark import read_watermark, write_watermark
from etl_framework.dq.runner import DQRunner



class SparkETLJob:
    def __init__(self, spark: SparkSession, config: JobConfig):
        self.spark = spark
        self.config = config

    def run(self) -> None:
        self._log("Starting")

        df_in = self._read_source()

        self._validate_required_columns(df_in)

        df_out = self._apply_transform(df_in)
        df_out = self._post_transform(df_out)
        # DQ PRE
        self._run_dq(stage="pre", df=df_out, source_df=df_in)

        self._write_by_strategy(df_out)
        # DQ POST (optional): run on same df_out or reload from target
        self._run_dq(stage="post", df=df_out, source_df=df_in)

        self._update_watermark(df_in)

        self._log("Finished")

    def _read_source(self) -> DataFrame:
        s = self.config.source
        df = self.spark.read.format(s.format)
        for k, v in (s.options or {}).items():
            df = df.option(k, v)
        df = df.load(s.path)

        # Watermark filter (incremental ingestion)
        wm = self.config.watermark
        if wm.enabled:
            if not wm.column:
                raise ValueError("watermark.enabled=true but watermark.column is missing.")
            job_key = wm.job_key or self.config.job_name
            last = read_watermark(self.spark, wm.metadata_table, job_key) or wm.initial_value
            if last:
                self._log(f"Watermark filter: {wm.column} > {last}")
                df = df.where(F.col(wm.column) > F.lit(last))
        return df

    def _apply_transform(self, df: DataFrame) -> DataFrame:
        func_path = self.config.transform.function
        module_path, func_name = func_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        fn = getattr(module, func_name)

        # pass a dict-like config if you want; here we pass dataclass config
        return fn(df, self.spark, {"job_name": self.config.job_name})

    def _validate_required_columns(self, df: DataFrame) -> None:
        missing = [c for c in self.config.required_columns if c not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

    def _post_transform(self, df: DataFrame) -> DataFrame:
        if self.config.drop_duplicates_on:
            df = df.dropDuplicates(self.config.drop_duplicates_on)

        return (
            df.withColumn("_ingest_ts", F.current_timestamp())
              .withColumn("_job_name", F.lit(self.config.job_name))
        )

    def _write_by_strategy(self, df: DataFrame) -> None:
        strat = self.config.target.write_strategy

        if strat == "write":
            run_write(df, self.config.target)
        elif strat == "merge_scd1":
            run_merge_scd1(self.spark, df, self.config.target)
        elif strat == "scd2":
            run_scd2(self.spark, df, self.config.target)
        else:
            raise ValueError(f"Unknown write_strategy: {strat}")

    def _update_watermark(self, df_in: DataFrame) -> None:
        wm = self.config.watermark
        if not wm.enabled:
            return
        if not wm.column:
            raise ValueError("watermark.enabled=true but watermark.column is missing.")

        job_key = wm.job_key or self.config.job_name
        max_val = df_in.select(F.max(F.col(wm.column)).alias("m")).collect()[0]["m"]
        if max_val is not None:
            write_watermark(self.spark, wm.metadata_table, job_key, str(max_val))
            self._log(f"Watermark updated to: {max_val}")
           
    def _run_dq(self, stage: str, df: DataFrame, source_df: DataFrame) -> None:
        if not self.config.dq.enabled:
            return

        checks = self.config.dq.pre_checks if stage == "pre" else self.config.dq.post_checks
        if not checks:
            return

        target_ref = {"table": self.config.target.table, "path": self.config.target.path}

        runner = DQRunner(
            self.spark,
            results_db=self.config.dq.results_db,
            fail_fast=self.config.dq.fail_fast,
            failed_sample_limit=self.config.dq.failed_sample_limit,
        )

        runner.run_checks(
            df=df,
            source_df=source_df,
            target_ref=target_ref,
            job_name=self.config.job_name,
            dataset=self.config.dq.dataset,
            stage=stage,
            checks=checks,
        )

    
    def _log(self, msg: str) -> None:
        print(f"[{self.config.job_name}] {msg}")
    
    


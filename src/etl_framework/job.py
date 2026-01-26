from __future__ import annotations

import importlib
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from typing import Optional
from etl_framework.config import JobConfig
from etl_framework.strategies.write_strategy import run_write
from etl_framework.strategies.merge_scd1 import run_merge_scd1
from etl_framework.strategies.merge_scd2 import run_scd2
from etl_framework.watermark import read_watermark, write_watermark
from etl_framework.dq.runner import DQRunner
from etl_framework.dq.runner import DQRunner, DQRunSummary



class SparkETLJob:
    def __init__(self, spark: SparkSession, config: JobConfig):
        self.spark = spark
        self.config = config

    def run(self) -> None:
        self._log("Starting")
        
        # 1) Read source
        df_in = self._read_source()
        
         # 2) Validate required columns early (fail fast)
        self._validate_required_columns(df_in)
        
        # 2) Transform
        df_out = self._apply_transform(df_in)
        df_out = self._post_transform(df_out)

        # DQ PRE (on transformed dataframe, before writing)
        pre_summary = self._run_dq(stage="pre", df=df_out, source_df=df_in)

        # Optional: block immediately if PRE DQ fails (bank common)
        if pre_summary and pre_summary.fail_count > 0:
            raise RuntimeError(
                f"PRE DQ failed (fails={pre_summary.fail_count}). run_id={pre_summary.run_id}"
            )

        # 4) Write / Merge (write | merge_scd1 | scd2)
        self._write_by_strategy(df_out)

        # DQ POST (optional): run on same df_out or reload from target
        #self._run_dq(stage="post", df=df_out, source_df=df_in)
        
        # # POST DQ must read target
        target_df = self._read_target_df()

        post_summary = self._run_dq(stage="post", df=target_df, source_df=df_in)
        
        # # Publish guard: block if POST fails but PRE passed
        guard = self.config.dq.publish_guard
        if guard.enabled and guard.block_on_post_fail:
            if post_summary and post_summary.fail_count > 0:
                raise RuntimeError(
                    f"POST DQ failed — blocking publish (fails={post_summary.fail_count}). "
                    f"post_run_id={post_summary.run_id}"
                )

        #  Update watermark (if enabled)
        self._update_watermark(df_in)

        self._log("Finished")

    def _read_source(self) -> DataFrame:
        s = self.config.source
        df = self.spark.read.format(s.format)
        for k, v in (s.options or {}).items():
            df = df.option(k, v)
        df = df.load(s.path)
        
    # ADD THIS METHOD RIGHT HERE (inside the class)
    def _read_target_df(self) -> DataFrame:
        if self.config.target.table:
            return self.spark.table(self.config.target.table)
        if self.config.target.path:
            return self.spark.read.format("delta").load(self.config.target.path)
        raise ValueError("Target is missing table/path; cannot run post DQ checks.")

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
    
    def _read_target_df(self) -> DataFrame:
        """
        Read the Delta target after write/merge.
        Used for POST DQ checks so we validate the actual persisted data.
        """
        if self.config.target.table:
            return self.spark.table(self.config.target.table)

        if self.config.target.path:
            return self.spark.read.format("delta").load(self.config.target.path)

        raise ValueError("Target is missing table/path; cannot run post DQ checks.")
    
    def _run_dq(self, stage: str, df: DataFrame, source_df: DataFrame) -> Optional[DQRunSummary]:
        """
        Runs DQ checks for the given stage ("pre" or "post") if enabled in config.
        Writes:
          - per-category results tables
          - per-category failed samples
          - consolidated dashboard
          - quarantine (optional)
        """
        if not getattr(self.config, "dq", None) or not self.config.dq.enabled:
            return

        # Validate required fields for naming
        if not self.config.dq.layer or not self.config.dq.dataset:
            raise ValueError("DQ is enabled but dq.layer or dq.dataset is missing in config.")

        checks = self.config.dq.pre_checks if stage == "pre" else self.config.dq.post_checks
        # If you rely fully on governance rule sets, checks may be empty; that's OK.
        # The DQRunner will load checks from governance if dq.rule_set is present.

        target_ref = {"table": self.config.target.table, "path": self.config.target.path}

        runner = DQRunner(self.spark)
        runner.run_checks(
            df=df,
            source_df=source_df,
            target_ref=target_ref,
            job_name=self.config.job_name,
            layer=self.config.dq.layer,
            dataset=self.config.dq.dataset,
            stage=stage,
            dq_config=self.config.dq,
            checks=checks,
        )

        
            
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
           
    

    
    def _log(self, msg: str) -> None:
        print(f"[{self.config.job_name}] {msg}")
    
    


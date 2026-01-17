from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T
from etl_framework.dq.base import DQCheck


class TimelinessSLACheck(DQCheck):
    category = "timeliness_sla"

    def run(self, *, df: DataFrame, source_df: DataFrame | None, target_ref: dict | None,
            job_name: str, dataset: str, stage: str, run_id: str, params: dict):
        spark: SparkSession = df.sparkSession

        col = params["timestamp_column"]          # e.g. "updated_at"
        max_age_minutes = int(params.get("max_age_minutes", 60))

        max_ts = df.select(F.max(F.col(col)).alias("m")).collect()[0]["m"]
        if max_ts is None:
            status = "FAIL"
            age_min = None
        else:
            age_min = df.select((F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.lit(max_ts))) / 60).collect()[0][0]
            status = "PASS" if age_min <= max_age_minutes else "FAIL"

        rows = [{
            "check_category": self.category,
            "check_name": self.name,
            "job_name": job_name,
            "dataset": dataset,
            "stage": stage,
            "status": status,
            "severity": params.get("severity", "HIGH"),
            "metric_name": "max_data_age_minutes",
            "metric_value": str(age_min),
            "threshold": f"<= {max_age_minutes}",
            "message": f"max({col})={max_ts}",
            "run_id": run_id,
        }]

        schema = T.StructType([T.StructField(c, T.StringType()) for c in [
            "check_category","check_name","job_name","dataset","stage","status","severity",
            "metric_name","metric_value","threshold","message","run_id"
        ]])
        return spark.createDataFrame(rows, schema=schema), None

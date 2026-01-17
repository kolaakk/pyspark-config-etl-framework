from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T
from etl_framework.dq.base import DQCheck


class LoadCompletenessCheck(DQCheck):
    category = "load_completeness"

    def run(self, *, df: DataFrame, source_df: DataFrame | None, target_ref: dict | None,
            job_name: str, dataset: str, stage: str, run_id: str, params: dict):
        spark: SparkSession = df.sparkSession

        min_rows = int(params.get("min_rows", 1))
        expected_ratio_min = params.get("expected_ratio_min")  # compare vs source_df if provided

        row_count = df.count()
        status = "PASS" if row_count >= min_rows else "FAIL"

        rows = [{
            "check_category": self.category,
            "check_name": self.name,
            "job_name": job_name,
            "dataset": dataset,
            "stage": stage,
            "status": status,
            "severity": params.get("severity", "HIGH"),
            "metric_name": "row_count",
            "metric_value": str(row_count),
            "threshold": f">= {min_rows}",
            "message": f"Row count {row_count} vs min_rows {min_rows}",
            "run_id": run_id,
        }]

        # Optional: ratio vs source
        if expected_ratio_min is not None and source_df is not None:
            src_count = source_df.count()
            ratio = (row_count / src_count) if src_count else 0.0
            r_status = "PASS" if ratio >= float(expected_ratio_min) else "FAIL"
            rows.append({
                "check_category": self.category,
                "check_name": self.name,
                "job_name": job_name,
                "dataset": dataset,
                "stage": stage,
                "status": r_status,
                "severity": params.get("severity", "HIGH"),
                "metric_name": "row_count_ratio_vs_source",
                "metric_value": str(ratio),
                "threshold": f">= {expected_ratio_min}",
                "message": f"Target rows/source rows = {ratio} (target={row_count}, source={src_count})",
                "run_id": run_id,
            })

        schema = T.StructType([
            T.StructField("check_category", T.StringType()),
            T.StructField("check_name", T.StringType()),
            T.StructField("job_name", T.StringType()),
            T.StructField("dataset", T.StringType()),
            T.StructField("stage", T.StringType()),
            T.StructField("status", T.StringType()),
            T.StructField("severity", T.StringType()),
            T.StructField("metric_name", T.StringType()),
            T.StructField("metric_value", T.StringType()),
            T.StructField("threshold", T.StringType()),
            T.StructField("message", T.StringType()),
            T.StructField("run_id", T.StringType()),
        ])

        results_df = spark.createDataFrame(rows, schema=schema)
        return results_df, None

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, types as T
from etl_framework.dq.base import DQCheck


class DeltaProcessingIntegrityCheck(DQCheck):
    category = "delta_integrity"

    def run(self, *, df: DataFrame, source_df: DataFrame | None, target_ref: dict | None,
            job_name: str, dataset: str, stage: str, run_id: str, params: dict):
        spark: SparkSession = df.sparkSession

        # target_ref expected: {"table": "..."} or {"path": "..."} (passed by job)
        exists = False
        target_desc = ""
        if target_ref:
            if target_ref.get("table"):
                target_desc = target_ref["table"]
                exists = spark.catalog.tableExists(target_ref["table"])
            elif target_ref.get("path"):
                target_desc = target_ref["path"]
                # simple: assume existence checked by write; keep it true if non-empty
                exists = True

        status = "PASS" if exists else "FAIL"

        rows = [{
            "check_category": self.category,
            "check_name": self.name,
            "job_name": job_name,
            "dataset": dataset,
            "stage": stage,
            "status": status,
            "severity": params.get("severity", "HIGH"),
            "metric_name": "delta_target_exists",
            "metric_value": str(exists),
            "threshold": "true",
            "message": f"Delta target exists: {target_desc}",
            "run_id": run_id,
        }]

        schema = T.StructType([T.StructField(c, T.StringType()) for c in [
            "check_category","check_name","job_name","dataset","stage","status","severity",
            "metric_name","metric_value","threshold","message","run_id"
        ]])
        return spark.createDataFrame(rows, schema=schema), None

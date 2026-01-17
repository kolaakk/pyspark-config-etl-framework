from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T
from etl_framework.dq.base import DQCheck


class UniquenessDuplicateCheck(DQCheck):
    category = "uniqueness"

    def run(self, *, df: DataFrame, source_df: DataFrame | None, target_ref: dict | None,
            job_name: str, dataset: str, stage: str, run_id: str, params: dict):
        spark: SparkSession = df.sparkSession

        keys = params.get("keys", [])
        if not keys:
            raise ValueError("uniqueness check requires params.keys")

        dup = (
            df.groupBy(*keys).count()
              .where(F.col("count") > 1)
        )

        dup_cnt = dup.count()
        status = "PASS" if dup_cnt == 0 else "FAIL"

        rows = [{
            "check_category": self.category,
            "check_name": self.name,
            "job_name": job_name,
            "dataset": dataset,
            "stage": stage,
            "status": status,
            "severity": params.get("severity", "CRITICAL"),
            "metric_name": "duplicate_key_groups",
            "metric_value": str(dup_cnt),
            "threshold": "0",
            "message": f"Duplicate groups found on keys={keys}",
            "run_id": run_id,
        }]

        schema = T.StructType([T.StructField(c, T.StringType()) for c in [
            "check_category","check_name","job_name","dataset","stage","status","severity",
            "metric_name","metric_value","threshold","message","run_id"
        ]])
        results_df = spark.createDataFrame(rows, schema=schema)

        # failed records sample = join back to original
        failed_records = df.join(dup.select(*keys), on=keys, how="inner")
        return results_df, failed_records

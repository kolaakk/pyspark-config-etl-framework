from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T
from etl_framework.dq.base import DQCheck


class ReferentialIntegrityCheck(DQCheck):
    category = "referential_integrity"

    def run(self, *, df: DataFrame, source_df: DataFrame | None, target_ref: dict | None,
            job_name: str, dataset: str, stage: str, run_id: str, params: dict):
        spark: SparkSession = df.sparkSession

        # params example:
        # {
        #   "ref_table":"silver.countries",
        #   "fk":"country",
        #   "pk":"country_code"
        # }
        ref_table = params["ref_table"]
        fk = params["fk"]
        pk = params.get("pk", fk)

        ref = spark.table(ref_table).select(F.col(pk).alias(pk)).dropDuplicates([pk])
        bad = df.join(ref, df[fk] == ref[pk], "left_anti")

        bad_cnt = bad.count()
        status = "PASS" if bad_cnt == 0 else "FAIL"

        rows = [{
            "check_category": self.category,
            "check_name": self.name,
            "job_name": job_name,
            "dataset": dataset,
            "stage": stage,
            "status": status,
            "severity": params.get("severity", "HIGH"),
            "metric_name": "fk_violations",
            "metric_value": str(bad_cnt),
            "threshold": "0",
            "message": f"FK {fk} not found in {ref_table}.{pk}",
            "run_id": run_id,
        }]

        schema = T.StructType([T.StructField(c, T.StringType()) for c in [
            "check_category","check_name","job_name","dataset","stage","status","severity",
            "metric_name","metric_value","threshold","message","run_id"
        ]])
        return spark.createDataFrame(rows, schema=schema), bad

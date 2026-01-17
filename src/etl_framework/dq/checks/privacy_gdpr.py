from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T
from etl_framework.dq.base import DQCheck


class PrivacySecurityGDPRCheck(DQCheck):
    category = "privacy_gdpr"

    def run(self, *, df: DataFrame, source_df: DataFrame | None, target_ref: dict | None,
            job_name: str, dataset: str, stage: str, run_id: str, params: dict):
        spark: SparkSession = df.sparkSession

        rows = []
        failed_union = None

        # 1) banned columns
        banned = params.get("banned_columns", [])
        present = [c for c in banned if c in df.columns]
        status = "PASS" if not present else "FAIL"
        rows.append({
            "check_category": self.category,
            "check_name": "banned_columns",
            "job_name": job_name,
            "dataset": dataset,
            "stage": stage,
            "status": status,
            "severity": params.get("severity", "CRITICAL"),
            "metric_name": "banned_columns_present",
            "metric_value": ",".join(present),
            "threshold": "none present",
            "message": "Banned columns must not appear",
            "run_id": run_id,
        })

        # 2) masking checks
        # params: {"masking":[{"col":"email","regex":"^.+@.+$","must_match":true, "sample_fail":true}]}
        masking = params.get("masking", [])
        for m in masking:
            col = m["col"]
            regex = m["regex"]
            must_match = bool(m.get("must_match", True))
            if col not in df.columns:
                continue
            if must_match:
                bad = df.where(~F.col(col).rlike(regex))
            else:
                bad = df.where(F.col(col).rlike(regex))

            bad_cnt = bad.count()
            st = "PASS" if bad_cnt == 0 else "FAIL"
            rows.append({
                "check_category": self.category,
                "check_name": f"masking:{col}",
                "job_name": job_name,
                "dataset": dataset,
                "stage": stage,
                "status": st,
                "severity": m.get("severity", params.get("severity", "HIGH")),
                "metric_name": "masking_fail_count",
                "metric_value": str(bad_cnt),
                "threshold": "0",
                "message": f"Masking regex check on {col}",
                "run_id": run_id,
            })
            bad = bad.withColumn("_privacy_rule", F.lit(f"masking:{col}"))
            failed_union = bad if failed_union is None else failed_union.unionByName(bad, allowMissingColumns=True)

        schema = T.StructType([T.StructField(c, T.StringType()) for c in [
            "check_category","check_name","job_name","dataset","stage","status","severity",
            "metric_name","metric_value","threshold","message","run_id"
        ]])
        return spark.createDataFrame(rows, schema=schema), failed_union

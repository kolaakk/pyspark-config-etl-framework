from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T
from etl_framework.dq.base import DQCheck


class BusinessRuleValidityCheck(DQCheck):
    category = "business_validity"

    def run(self, *, df: DataFrame, source_df: DataFrame | None, target_ref: dict | None,
            job_name: str, dataset: str, stage: str, run_id: str, params: dict):
        spark: SparkSession = df.sparkSession

        # params: {"rules":[{"name":"price_positive","condition":"product_price > 0","max_fail_rate":0.0}, ...]}
        rules = params.get("rules", [])
        rows = []
        failed_union = None

        total = df.count() or 1

        for rule in rules:
            cond = rule["condition"]
            rname = rule.get("name", cond)
            max_fail_rate = float(rule.get("max_fail_rate", 0.0))

            failed = df.where(f"NOT ({cond})")
            fail_cnt = failed.count()
            fail_rate = fail_cnt / total

            status = "PASS" if fail_rate <= max_fail_rate else "FAIL"
            rows.append({
                "check_category": self.category,
                "check_name": rname,
                "job_name": job_name,
                "dataset": dataset,
                "stage": stage,
                "status": status,
                "severity": rule.get("severity", params.get("severity", "HIGH")),
                "metric_name": "fail_rate",
                "metric_value": f"{fail_rate} (fail={fail_cnt}, total={total})",
                "threshold": f"<= {max_fail_rate}",
                "message": f"Rule: {cond}",
                "run_id": run_id,
            })

            failed_tagged = failed.withColumn("_failed_rule", F.lit(rname))
            failed_union = failed_tagged if failed_union is None else failed_union.unionByName(failed_tagged, allowMissingColumns=True)

        schema = T.StructType([T.StructField(c, T.StringType()) for c in [
            "check_category","check_name","job_name","dataset","stage","status","severity",
            "metric_name","metric_value","threshold","message","run_id"
        ]])
        results_df = spark.createDataFrame(rows, schema=schema)
        return results_df, failed_union

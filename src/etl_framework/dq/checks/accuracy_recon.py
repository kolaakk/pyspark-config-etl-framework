from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T
from etl_framework.dq.base import DQCheck


class DataAccuracyReconciliationCheck(DQCheck):
    category = "accuracy_recon"

    def run(self, *, df: DataFrame, source_df: DataFrame | None, target_ref: dict | None,
            job_name: str, dataset: str, stage: str, run_id: str, params: dict):
        spark: SparkSession = df.sparkSession
        if source_df is None:
            raise ValueError("accuracy_recon requires source_df (pre-transform read).")

        # config: {"recon": [{"metric":"sum","col":"product_price","tolerance":0.01}, ...]}
        recon = params.get("recon", [])
        rows = []

        for r in recon:
            metric = r["metric"]
            col = r.get("col")
            tol = float(r.get("tolerance", 0.0))

            if metric == "row_count":
                a = source_df.count()
                b = df.count()
            elif metric == "count_distinct":
                a = source_df.select(F.col(col)).distinct().count()
                b = df.select(F.col(col)).distinct().count()
            elif metric == "sum":
                a = float(source_df.select(F.sum(F.col(col)).alias("x")).collect()[0]["x"] or 0.0)
                b = float(df.select(F.sum(F.col(col)).alias("x")).collect()[0]["x"] or 0.0)
            else:
                raise ValueError(f"Unsupported recon metric: {metric}")

            diff = abs(a - b)
            status = "PASS" if diff <= tol else "FAIL"

            rows.append({
                "check_category": self.category,
                "check_name": self.name,
                "job_name": job_name,
                "dataset": dataset,
                "stage": stage,
                "status": status,
                "severity": r.get("severity", params.get("severity", "MEDIUM")),
                "metric_name": f"{metric}:{col or ''}".strip(":"),
                "metric_value": f"source={a},target={b},diff={diff}",
                "threshold": f"diff <= {tol}",
                "message": "Source vs target reconciliation",
                "run_id": run_id,
            })

        schema = T.StructType([T.StructField(c, T.StringType()) for c in [
            "check_category","check_name","job_name","dataset","stage","status","severity",
            "metric_name","metric_value","threshold","message","run_id"
        ]])
        return spark.createDataFrame(rows, schema=schema), None

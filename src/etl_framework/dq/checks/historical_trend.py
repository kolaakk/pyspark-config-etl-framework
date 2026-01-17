from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T
from etl_framework.dq.base import DQCheck


class HistoricalTrendAnomalyCheck(DQCheck):
    category = "historical_trend"

    def run(self, *, df: DataFrame, source_df: DataFrame | None, target_ref: dict | None,
            job_name: str, dataset: str, stage: str, run_id: str, params: dict):
        spark: SparkSession = df.sparkSession

        # params: {"metric":"row_count","window_runs":10,"zscore_max":3.0}
        metric = params.get("metric", "row_count")
        window_runs = int(params.get("window_runs", 10))
        zmax = float(params.get("zscore_max", 3.0))

        current_val = df.count() if metric == "row_count" else df.select(F.count("*")).collect()[0][0]

        # Read previous values from this category table (if exists)
        table = params.get("history_table", "dq.load_completeness_results")
        prev_vals = []
        if spark.catalog.tableExists(table):
            hist = (
                spark.table(table)
                .where((F.col("job_name") == job_name) & (F.col("dataset") == dataset) & (F.col("metric_name") == "row_count"))
                .orderBy(F.col("written_ts").desc())
                .limit(window_runs)
            )
            prev_vals = [int(r["metric_value"]) for r in hist.select("metric_value").collect() if str(r["metric_value"]).isdigit()]

        status = "WARN"
        msg = "Not enough history"
        threshold = f"|z| <= {zmax}"

        if len(prev_vals) >= 3:
            avg = sum(prev_vals) / len(prev_vals)
            var = sum((x - avg) ** 2 for x in prev_vals) / (len(prev_vals) - 1)
            std = var ** 0.5 if var > 0 else 0.0
            z = (current_val - avg) / std if std > 0 else 0.0
            status = "PASS" if abs(z) <= zmax else "FAIL"
            msg = f"current={current_val}, avg={avg}, std={std}, z={z}"

        rows = [{
            "check_category": self.category,
            "check_name": self.name,
            "job_name": job_name,
            "dataset": dataset,
            "stage": stage,
            "status": status,
            "severity": params.get("severity", "MEDIUM"),
            "metric_name": metric,
            "metric_value": str(current_val),
            "threshold": threshold,
            "message": msg,
            "run_id": run_id,
        }]

        schema = T.StructType([T.StructField(c, T.StringType()) for c in [
            "check_category","check_name","job_name","dataset","stage","status","severity",
            "metric_name","metric_value","threshold","message","run_id"
        ]])
        return spark.createDataFrame(rows, schema=schema), None

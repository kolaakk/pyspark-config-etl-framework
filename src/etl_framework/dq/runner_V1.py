from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from pyspark.sql import DataFrame, SparkSession, functions as F

from etl_framework.dq.registry import CHECKS
from etl_framework.dq.writer import append_results, append_failed_records, append_dashboard
from etl_framework.dq.naming import prefix as dq_prefix, results_table, failed_table, dashboard_table, quarantine_table
from etl_framework.dq.scoring import add_scoring_columns, compute_dataset_score
from etl_framework.dq.quarantine import append_quarantine
from etl_framework.dq.governance import rule_set_is_approved, load_rule_set_checks

@dataclass
class DQRunSummary:
    run_id: str
    fail_count: int
    warn_count: int

class DQRunner:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def run_checks(
        self,
        *,
        df: DataFrame,
        source_df: Optional[DataFrame],
        target_ref: Optional[Dict[str, Any]],
        job_name: str,
        layer: str,
        dataset: str,
        stage: str,
        dq_config: Any,   # your DQConfig dataclass
        checks: List[Dict[str, Any]],
        run_id: Optional[str] = None,
    ) -> None:
        run_id = run_id or str(uuid.uuid4())

        # Governance override (optional)
        if dq_config.rule_set and dq_config.rule_set.rule_set_id:
            rs_id = dq_config.rule_set.rule_set_id
            rs_ver = dq_config.rule_set.version

            if dq_config.enforce_approved_rule_set and not rule_set_is_approved(self.spark, rs_id, rs_ver):
                raise RuntimeError(f"Rule set not approved: {rs_id} v{rs_ver}")

            # Replace checks list from governance (stage-specific)
            checks = load_rule_set_checks(self.spark, rs_id, rs_ver, stage)

        prefix_ = dq_prefix(layer, dataset, dq_config.naming.prefix_template)

        all_results = None
        quarantine_union = None

        for item in checks:
            check_type = item["type"]
            name = item.get("name", check_type)
            params = item.get("params", {})

            if check_type not in CHECKS:
                raise ValueError(f"Unknown DQ check type: {check_type}")

            check = CHECKS[check_type](name=name)
            results_df, failed_df = check.run(
                df=df,
                source_df=source_df,
                target_ref=target_ref,
                job_name=job_name,
                dataset=dataset,
                stage=stage,
                run_id=run_id,
                params=params,
            )

            # Ensure columns exist for dashboard rollup
            results_df = (
                results_df
                .withColumn("layer", F.lit(layer))
                .withColumn("dataset", F.lit(dataset))
                .withColumn("dq_category", F.lit(check.category))
                .withColumn("dq_check_type", F.lit(check_type))
            )

            # Per-category tables (your naming)
            append_results(
                self.spark,
                results_df,
                results_table(prefix_, check.category)
            )

            if failed_df is not None:
                # Per-category failed samples
                append_failed_records(
                    self.spark,
                    failed_df.withColumn("dq_category", F.lit(check.category))
                            .withColumn("dq_check_name", F.lit(name))
                            .withColumn("dq_run_id", F.lit(run_id))
                            .withColumn("layer", F.lit(layer))
                            .withColumn("dataset", F.lit(dataset))
                            .withColumn("stage", F.lit(stage)),
                    failed_table(prefix_, check.category),
                    dq_config.failed_sample_limit,
                )

                # Quarantine (optional, full-ish, not just sample)
                if dq_config.quarantine.enabled:
                    q = (
                        failed_df
                        .withColumn("_dq_run_id", F.lit(run_id))
                        .withColumn("_dq_category", F.lit(check.category))
                        .withColumn("_dq_rule_name", F.lit(name))
                        .withColumn("_dq_stage", F.lit(stage))
                    )
                    quarantine_union = q if quarantine_union is None else quarantine_union.unionByName(q, allowMissingColumns=True)

            all_results = results_df if all_results is None else all_results.unionByName(results_df, allowMissingColumns=True)

        if all_results is None:
            return

        # Scoring + dashboard
        scored = add_scoring_columns(
            all_results,
            severity_weights=dq_config.scoring.severity_weights,
            status_multipliers=dq_config.scoring.status_multipliers,
        )

        score_df = compute_dataset_score(scored).withColumn("dq_run_id", F.lit(run_id))

        dashboard = (
            scored.groupBy("layer", "dataset", "job_name", "stage", "run_id")
                  .agg(
                      F.count(F.lit(1)).alias("checks_total"),
                      F.sum(F.when(F.col("status") == "FAIL", 1).otherwise(0)).alias("checks_failed"),
                      F.sum(F.when(F.col("status") == "WARN", 1).otherwise(0)).alias("checks_warn"),
                      F.sum("dq_penalty").alias("total_penalty"),
                  )
                  .join(score_df.select("dq_run_id", "dq_score", "dq_grade", "total_penalty"),
                        scored.groupBy().agg(F.lit(run_id).alias("dq_run_id")).select("dq_run_id"),
                        "dq_run_id",
                        "left")
                  .drop("dq_run_id")
        )

        append_dashboard(self.spark, dashboard, dashboard_table(prefix_))

        # Quarantine write
        if dq_config.quarantine.enabled and quarantine_union is not None:
            q_table = quarantine_table(
                dq_config.quarantine.database,
                layer,
                dataset,
                dq_config.quarantine.table_template
            )
            append_quarantine(self.spark, quarantine_union, q_table, dq_config.quarantine.max_rows_per_run)

        # fail_fast
        if dq_config.fail_fast:
            fails = scored.where("status = 'FAIL'").count()
            if fails > 0:
                raise RuntimeError(f"DQ fail_fast: {fails} failures. run_id={run_id}")

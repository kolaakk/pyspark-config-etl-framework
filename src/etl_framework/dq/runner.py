from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession, functions as F

from etl_framework.dq.registry import CHECKS
from etl_framework.dq.writer import append_results, append_failed_records, append_dashboard
from etl_framework.dq.naming import (
    prefix as dq_prefix,
    results_table,
    failed_table,
    dashboard_table,
    quarantine_table,
)
from etl_framework.dq.scoring import add_scoring_columns, compute_dataset_score
from etl_framework.dq.quarantine import append_quarantine
from etl_framework.dq.governance import rule_set_is_approved, load_rule_set_checks


@dataclass
class DQRunSummary:
    run_id: str
    fail_count: int
    warn_count: int


class DQRunner:
    """
    Runs DQ checks (either inline from config or from governance rule sets),
    writes per-category results, per-category failed samples, a consolidated dashboard,
    and optionally quarantines failing rows.

    Returns DQRunSummary so job.py can implement publish guards.
    """

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
        stage: str,                # "pre" or "post"
        dq_config: Any,            # your DQConfig dataclass instance
        checks: List[Dict[str, Any]],
        run_id: Optional[str] = None,
    ) -> DQRunSummary:
        """
        Execute configured checks for a given stage.

        Governance mode:
          - If dq_config.rule_set exists and rule_set_id is non-empty, checks are loaded from governance tables.
          - If dq_config.enforce_approved_rule_set is True, an APPROVED approval is required.

        Inline mode:
          - Uses `checks` passed in from dq.pre_checks / dq.post_checks.

        Writes:
          - dq_<layer>_<dataset>_<category>_results
          - dq_<layer>_<dataset>_<category>_failed_records (sample)
          - dq_<layer>_<dataset>_dashboard (consolidated)
          - quarantine.<layer>_<dataset> (optional)

        Returns:
          DQRunSummary(run_id, fail_count, warn_count)
        """
        run_id = run_id or str(uuid.uuid4())

        # Decide governance vs inline
        use_governance = (
            getattr(dq_config, "rule_set", None) is not None
            and getattr(dq_config.rule_set, "rule_set_id", "").strip() != ""
        )

        if use_governance:
            rs_id = dq_config.rule_set.rule_set_id
            rs_ver = int(dq_config.rule_set.version)

            if getattr(dq_config, "enforce_approved_rule_set", False):
                if not rule_set_is_approved(self.spark, rs_id, rs_ver):
                    raise RuntimeError(f"Rule set not approved: {rs_id} v{rs_ver}")

            # Replace checks with governance-derived rules for this stage
            checks = load_rule_set_checks(self.spark, rs_id, rs_ver, stage)

        # Naming prefix e.g. dq_silver_customers
        prefix_ = dq_prefix(layer, dataset, dq_config.naming.prefix_template)

        all_results: Optional[DataFrame] = None
        quarantine_union: Optional[DataFrame] = None

        # If no checks at all, return clean summary (still a run_id)
        if not checks:
            return DQRunSummary(run_id=run_id, fail_count=0, warn_count=0)

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

            # Normalize/augment results so dashboard can aggregate consistently
            results_df = (
                results_df
                .withColumn("layer", F.lit(layer))
                .withColumn("dataset", F.lit(dataset))
                .withColumn("dq_category", F.lit(check.category))
                .withColumn("dq_check_type", F.lit(check_type))
                .withColumn("dq_rule_name", F.lit(name))
            )

            # Write per-category results
            append_results(
                self.spark,
                results_df,
                results_table(prefix_, check.category)
            )

            # Failed sample + quarantine
            if failed_df is not None:
                # Per-category failed records (sample)
                failed_aug = (
                    failed_df
                    .withColumn("layer", F.lit(layer))
                    .withColumn("dataset", F.lit(dataset))
                    .withColumn("stage", F.lit(stage))
                    .withColumn("dq_run_id", F.lit(run_id))
                    .withColumn("dq_category", F.lit(check.category))
                    .withColumn("dq_check_type", F.lit(check_type))
                    .withColumn("dq_rule_name", F.lit(name))
                )

                append_failed_records(
                    self.spark,
                    failed_aug,
                    failed_table(prefix_, check.category),
                    int(getattr(dq_config, "failed_sample_limit", 50)),
                )

                # Union for quarantine (write once per run)
                if getattr(dq_config, "quarantine", None) and dq_config.quarantine.enabled:
                    q = (
                        failed_df
                        .withColumn("_dq_run_id", F.lit(run_id))
                        .withColumn("_dq_stage", F.lit(stage))
                        .withColumn("_dq_category", F.lit(check.category))
                        .withColumn("_dq_check_type", F.lit(check_type))
                        .withColumn("_dq_rule_name", F.lit(name))
                        .withColumn("_dq_job_name", F.lit(job_name))
                    )
                    quarantine_union = q if quarantine_union is None else quarantine_union.unionByName(q, allowMissingColumns=True)

            all_results = results_df if all_results is None else all_results.unionByName(results_df, allowMissingColumns=True)

        # If for some reason we have no results
        if all_results is None:
            return DQRunSummary(run_id=run_id, fail_count=0, warn_count=0)

        # ---- Scoring + dashboard ----
        scored = add_scoring_columns(
            all_results,
            severity_weights=dq_config.scoring.severity_weights,
            status_multipliers=dq_config.scoring.status_multipliers,
        )

        score_df = (
            compute_dataset_score(scored)
            .withColumn("dq_run_id", F.lit(run_id))
            .withColumn("layer", F.lit(layer))
            .withColumn("dataset", F.lit(dataset))
            .withColumn("job_name", F.lit(job_name))
            .withColumn("stage", F.lit(stage))
        )

        dashboard = (
            scored.groupBy("layer", "dataset", "job_name", "stage")
                  .agg(
                      F.lit(run_id).alias("run_id"),
                      F.count(F.lit(1)).alias("checks_total"),
                      F.sum(F.when(F.col("status") == "FAIL", 1).otherwise(0)).alias("checks_failed"),
                      F.sum(F.when(F.col("status") == "WARN", 1).otherwise(0)).alias("checks_warn"),
                      F.sum("dq_penalty").alias("total_penalty"),
                  )
                  .crossJoin(score_df.select("dq_score", "dq_grade"))
        )

        append_dashboard(self.spark, dashboard, dashboard_table(prefix_))

        # ---- Quarantine write (optional) ----
        if getattr(dq_config, "quarantine", None) and dq_config.quarantine.enabled and quarantine_union is not None:
            q_table = quarantine_table(
                dq_config.quarantine.database,
                layer,
                dataset,
                dq_config.quarantine.table_template,
            )
            append_quarantine(
                self.spark,
                quarantine_union,
                q_table,
                int(dq_config.quarantine.max_rows_per_run),
            )

        # ---- Summary counts for publish guard ----
        fail_count = scored.where(F.col("status") == "FAIL").count()
        warn_count = scored.where(F.col("status") == "WARN").count()

        return DQRunSummary(run_id=run_id, fail_count=int(fail_count), warn_count=int(warn_count))

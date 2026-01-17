from __future__ import annotations

import uuid
from typing import Any, Dict, List, Optional
from pyspark.sql import DataFrame, SparkSession

from etl_framework.dq.registry import CHECKS
from etl_framework.dq.writer import append_results, append_failed_records


class DQRunner:
    def __init__(
        self,
        spark: SparkSession,
        *,
        results_db: str = "dq",
        fail_fast: bool = False,
        failed_sample_limit: int = 50,
    ):
        self.spark = spark
        self.results_db = results_db
        self.fail_fast = fail_fast
        self.failed_sample_limit = failed_sample_limit

    def run_checks(
        self,
        *,
        df: DataFrame,
        source_df: Optional[DataFrame],
        target_ref: Optional[Dict[str, Any]],
        job_name: str,
        dataset: str,
        stage: str,
        checks: List[Dict[str, Any]],
        run_id: Optional[str] = None,
    ) -> None:
        run_id = run_id or str(uuid.uuid4())

        for item in checks:
            check_type = item["type"]
            name = item.get("name", check_type)
            params = item.get("params", {})

            if check_type not in CHECKS:
                raise ValueError(f"Unknown DQ check type: {check_type}")

            check_cls = CHECKS[check_type]
            check = check_cls(name=name)

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

            results_table = f"{self.results_db}.{check.category}_results"
            append_results(self.spark, results_df, results_table)

            if failed_df is not None:
                failed_table = f"{self.results_db}.{check.category}_failed_records"
                append_failed_records(self.spark, failed_df, failed_table, self.failed_sample_limit)

            # Fail-fast behavior (if any FAIL rows)
            if self.fail_fast:
                fail_count = results_df.where("status = 'FAIL'").count()
                if fail_count > 0:
                    raise RuntimeError(f"DQ fail_fast triggered by check={check_type} name={name}, run_id={run_id}")

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, Any
from pyspark.sql import DataFrame


@dataclass
class DQResult:
    check_category: str
    check_name: str
    job_name: str
    dataset: str
    stage: str                 # "pre" or "post"
    status: str                # "PASS" | "FAIL" | "WARN"
    severity: str              # "LOW" | "MEDIUM" | "HIGH" | "CRITICAL"
    metric_name: str
    metric_value: str
    threshold: Optional[str]
    message: str
    run_id: str
    event_ts_col: str = "_ingest_ts"


class DQCheck:
    """
    Base class for a DQ category check.
    Implement run(...) and return:
      - summary_results_df: rows of DQResult schema (as DataFrame)
      - failed_records_df: optional DataFrame of failed records for sampling
    """
    category: str = "UNKNOWN"

    def __init__(self, name: str):
        self.name = name

    def run(
        self,
        *,
        df: DataFrame,
        source_df: DataFrame | None,
        target_ref: Dict[str, Any] | None,
        job_name: str,
        dataset: str,
        stage: str,
        run_id: str,
        params: Dict[str, Any],
    ) -> tuple[DataFrame, Optional[DataFrame]]:
        raise NotImplementedError

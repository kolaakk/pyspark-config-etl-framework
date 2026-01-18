from __future__ import annotations

import json
from typing import List, Dict, Any
from pyspark.sql import SparkSession, functions as F

GOV_DB = "dq_governance"

def ensure_governance_db(spark: SparkSession) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {GOV_DB}")

def rule_set_is_approved(spark: SparkSession, rule_set_id: str, version: int) -> bool:
    ensure_governance_db(spark)
    approvals_tbl = f"{GOV_DB}.approvals"
    if not spark.catalog.tableExists(approvals_tbl):
        return False
    df = (spark.table(approvals_tbl)
          .where((F.col("rule_set_id") == rule_set_id) &
                 (F.col("version") == version) &
                 (F.col("approval_status") == "APPROVED"))
          .limit(1))
    return df.count() == 1

def load_rule_set_checks(
    spark: SparkSession,
    rule_set_id: str,
    version: int,
    stage: str
) -> List[Dict[str, Any]]:
    ensure_governance_db(spark)
    rules_tbl = f"{GOV_DB}.rule_set_rules"
    if not spark.catalog.tableExists(rules_tbl):
        raise ValueError(f"Governance rules table missing: {rules_tbl}")

    rows = (
        spark.table(rules_tbl)
        .where((F.col("rule_set_id") == rule_set_id) &
               (F.col("version") == version) &
               (F.col("stage") == stage) &
               (F.col("enabled") == True))
        .select("check_type", "rule_name", "params_json", "severity")
        .collect()
    )

    checks: List[Dict[str, Any]] = []
    for r in rows:
        params = json.loads(r["params_json"]) if r["params_json"] else {}
        # allow governance to enforce severity centrally
        if r["severity"]:
            params["severity"] = r["severity"]
        checks.append({
            "type": r["check_type"],
            "name": r["rule_name"],
            "params": params
        })
    return checks

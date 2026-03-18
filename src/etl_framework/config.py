from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
#from typing import List, Dict, Any



@dataclass
class SourceConfig:
    format: str
    path: str
    options: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TransformConfig:
    function: str
    params: Dict[str, Any] = field(default_factory=dict)


@dataclass
class WatermarkConfig:
    enabled: bool = False
    column: Optional[str] = None                 # e.g. "updated_at"
    initial_value: Optional[str] = None          # e.g. "1970-01-01T00:00:00Z"
    metadata_table: str = "etl_metadata.watermarks"
    job_key: Optional[str] = None                # if None -> job_name


@dataclass
class MergeSCD1Config:
    keys: List[str] = field(default_factory=list)
    match_condition: Optional[str] = None

    # Hash-diff change detection
    hashdiff_enabled: bool = False
    hashdiff_columns: List[str] = field(default_factory=list)  # if empty -> all non-keys

    # Column selection for update/insert
    update_columns: List[str] = field(default_factory=list)    # if empty -> update all
    insert_columns: List[str] = field(default_factory=list)    # if empty -> insert all

    # Soft delete instead of delete
    soft_delete: bool = False
    soft_delete_column: str = "is_deleted"
    soft_delete_ts_column: str = "deleted_ts"


@dataclass
class SCD2Config:
    keys: List[str] = field(default_factory=list)
    match_condition: Optional[str] = None

    tracked_columns: List[str] = field(default_factory=list)    # attributes to track for changes
    hashdiff_enabled: bool = True                                # usually true for SCD2

    effective_from_col: str = "effective_from"
    effective_to_col: str = "effective_to"
    is_current_col: str = "is_current"
    soft_delete_column: str = "is_deleted"
    soft_delete_ts_column: str = "deleted_ts"

    # if true, records missing from source are soft-deleted (SCD2 current row is closed + marked deleted)
    soft_delete_missing: bool = False


@dataclass
class TargetConfig:
    write_strategy: str = "write"   # "write" | "merge_scd1" | "scd2"

    # write_strategy="write"
    mode: str = "append"
    table: Optional[str] = None
    path: Optional[str] = None
    partition_by: List[str] = field(default_factory=list)
    overwrite_schema: bool = False

    merge_scd1: MergeSCD1Config = field(default_factory=MergeSCD1Config)
    scd2: SCD2Config = field(default_factory=SCD2Config)
    
# DQ config models (governance + scoring + quarantine)
@dataclass
class DQNaming:
    prefix_template: str = "dq_{layer}_{dataset}"

@dataclass
class DQScoring:
    severity_weights: Dict[str, float] = field(default_factory=lambda: {
        "LOW": 1, "MEDIUM": 3, "HIGH": 7, "CRITICAL": 10
    })
    status_multipliers: Dict[str, float] = field(default_factory=lambda: {
        "PASS": 0, "WARN": 0.5, "FAIL": 1
    })  

@dataclass
class DQQuarantine:
    enabled: bool = False
    database: str = "quarantine"
    table_template: str = "{layer}_{dataset}"
    max_rows_per_run: int = 5000

@dataclass
class DQRuleSetRef:
    rule_set_id: str = ""
    version: int = 1

@dataclass
class DQConfig:
    enabled: bool = False
    dataset: str = ""                 # logical dataset name (e.g. "customers")
    results_db: str = "dq"
    layer: str = ""                  # data layer (e.g. "raw", "silver", "gold")
    fail_fast: bool = False
    failed_sample_limit: int = 50
    naming: DQNaming = field(default_factory=DQNaming)
    enforce_approved_rule_set: bool = False
    rule_set: Optional[DQRuleSetRef] = None
    scoring: DQScoring = field(default_factory=DQScoring)
    quarantine: DQQuarantine = field(default_factory=DQQuarantine)

    pre_checks: List[Dict[str, Any]] = field(default_factory=list)
    post_checks: List[Dict[str, Any]] = field(default_factory=list)

@dataclass
class DQPublishGuard:
    """
    Controls whether the job should be blocked after persistence
    if POST DQ checks fail.
    """
    enabled: bool = False
    block_on_post_fail: bool = True

@dataclass
class JobConfig:
    job_name: str
    source: SourceConfig
    transform: TransformConfig
    target: TargetConfig
    watermark: WatermarkConfig = field(default_factory=WatermarkConfig)
    dq: DQConfig = field(default_factory=DQConfig)


    required_columns: List[str] = field(default_factory=list)
    drop_duplicates_on: List[str] = field(default_factory=list)


# Load config from JSON

def load_config(config_path: str) -> JobConfig:
    with open(config_path, "r", encoding="utf-8") as f:
        raw = json.load(f)
# ---- TARGET ----
    raw_target = raw["target"]
    raw_wm = raw.get("watermark", {})
    dq_raw = raw.get("dq", {})

    scd1_raw = raw_target.get("merge_scd1", {})
    scd2_raw = raw_target.get("scd2", {})

    target = TargetConfig(
        write_strategy=raw_target.get("write_strategy", "write"),
        mode=raw_target.get("mode", "append"),
        table=raw_target.get("table"),
        path=raw_target.get("path"),
        partition_by=raw_target.get("partition_by", []),
        overwrite_schema=raw_target.get("overwrite_schema", False),
        merge_scd1=MergeSCD1Config(**scd1_raw) if scd1_raw else MergeSCD1Config(),
        scd2=SCD2Config(**scd2_raw) if scd2_raw else SCD2Config(),
    )

    wm = WatermarkConfig(
        enabled=raw_wm.get("enabled", False),
        column=raw_wm.get("column"),
        initial_value=raw_wm.get("initial_value"),
        metadata_table=raw_wm.get("metadata_table", "etl_metadata.watermarks"),
        job_key=raw_wm.get("job_key"),
    )

    # nested: naming
    naming_raw = dq_raw.get("naming", {}) or {}
    naming = DQNaming(
        prefix_template=naming_raw.get("prefix_template", "dq_{layer}_{dataset}")
    )

    # nested: scoring
    scoring_raw = dq_raw.get("scoring", {}) or {}
    scoring = DQScoring(
        severity_weights=scoring_raw.get("severity_weights", {
            "LOW": 1, "MEDIUM": 3, "HIGH": 7, "CRITICAL": 10
        }),
        status_multipliers=scoring_raw.get("status_multipliers", {
            "PASS": 0, "WARN": 0.5, "FAIL": 1
        }),
    )

    # nested: quarantine
    quarantine_raw = dq_raw.get("quarantine", {}) or {}
    quarantine = DQQuarantine(
        enabled=bool(quarantine_raw.get("enabled", False)),
        database=quarantine_raw.get("database", "quarantine"),
        table_template=quarantine_raw.get("table_template", "{layer}_{dataset}"),
        max_rows_per_run=int(quarantine_raw.get("max_rows_per_run", 5000)),
    )

    # nested: rule_set
    rule_set_raw = dq_raw.get("rule_set")
    rule_set = None
    if isinstance(rule_set_raw, dict):
        rule_set = DQRuleSetRef(
            rule_set_id=rule_set_raw.get("rule_set_id", ""),
            version=int(rule_set_raw.get("version", 1)),
        )
        # if empty id, treat as None
        if not rule_set.rule_set_id:
            rule_set = None
        # ---- DQ publish guard ----
    guard_raw = dq_raw.get("publish_guard", {}) or {}
    publish_guard = DQPublishGuard(
        enabled=bool(guard_raw.get("enabled", False)),
        block_on_post_fail=bool(guard_raw.get("block_on_post_fail", True)),
    )

    dq = DQConfig(
        enabled=bool(dq_raw.get("enabled", False)),
        layer=dq_raw.get("layer", ""),
        dataset=dq_raw.get("dataset", raw.get("job_name", "")),
        naming=naming,
        enforce_approved_rule_set=bool(dq_raw.get("enforce_approved_rule_set", False)),
        results_db=dq_raw.get("results_db", "dq"),
        rule_set=rule_set,
        scoring=scoring,
        quarantine=quarantine,
        fail_fast=bool(dq_raw.get("fail_fast", False)),
        failed_sample_limit=int(dq_raw.get("failed_sample_limit", 50)),
        publish_guard=publish_guard,
        pre_checks=dq_raw.get("pre_checks", []),
        post_checks=dq_raw.get("post_checks", []),
    )


    return JobConfig(
        job_name=raw["job_name"],
        source=SourceConfig(**raw["source"]),
        transform=TransformConfig(**raw["transform"]),
        target=target,
        watermark=wm,
        dq=dq,
        required_columns=raw.get("required_columns", []),
        drop_duplicates_on=raw.get("drop_duplicates_on", []),
    )

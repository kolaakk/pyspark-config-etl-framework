from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


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


@dataclass
class JobConfig:
    job_name: str
    source: SourceConfig
    transform: TransformConfig
    target: TargetConfig
    watermark: WatermarkConfig = field(default_factory=WatermarkConfig)

    required_columns: List[str] = field(default_factory=list)
    drop_duplicates_on: List[str] = field(default_factory=list)


def load_config(config_path: str) -> JobConfig:
    with open(config_path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    raw_target = raw["target"]
    raw_wm = raw.get("watermark", {})

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

    return JobConfig(
        job_name=raw["job_name"],
        source=SourceConfig(**raw["source"]),
        transform=TransformConfig(**raw["transform"]),
        target=target,
        watermark=wm,
        required_columns=raw.get("required_columns", []),
        drop_duplicates_on=raw.get("drop_duplicates_on", []),
    )

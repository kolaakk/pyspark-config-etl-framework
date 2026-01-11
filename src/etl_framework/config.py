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
class TargetConfig:
    mode: str = "append"                 # append | overwrite
    table: Optional[str] = None          # e.g. "silver.products"
    path: Optional[str] = None           # e.g. "/mnt/delta/silver/products"
    partition_by: List[str] = field(default_factory=list)
    overwrite_schema: bool = False


@dataclass
class JobConfig:
    job_name: str
    source: SourceConfig
    transform: TransformConfig
    target: TargetConfig
    required_columns: List[str] = field(default_factory=list)
    drop_duplicates_on: List[str] = field(default_factory=list)


def load_config(config_path: str) -> JobConfig:
    with open(config_path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    return JobConfig(
        job_name=raw["job_name"],
        source=SourceConfig(**raw["source"]),
        transform=TransformConfig(**raw["transform"]),
        target=TargetConfig(**raw["target"]),
        required_columns=raw.get("required_columns", []),
        drop_duplicates_on=raw.get("drop_duplicates_on", []),
    )


def to_dict(cfg: JobConfig) -> Dict[str, Any]:
    return {
        "job_name": cfg.job_name,
        "source": {
            "format": cfg.source.format,
            "path": cfg.source.path,
            "options": cfg.source.options,
        },
        "transform": {
            "function": cfg.transform.function,
            "params": cfg.transform.params,
        },
        "target": {
            "mode": cfg.target.mode,
            "table": cfg.target.table,
            "path": cfg.target.path,
            "partition_by": cfg.target.partition_by,
            "overwrite_schema": cfg.target.overwrite_schema,
        },
        "required_columns": cfg.required_columns,
        "drop_duplicates_on": cfg.drop_duplicates_on,
    }

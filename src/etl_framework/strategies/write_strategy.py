from __future__ import annotations

from pyspark.sql import DataFrame

from etl_framework.io import write_delta
from etl_framework.config import TargetConfig


def run_write(df: DataFrame, target: TargetConfig) -> None:
    write_delta(
        df=df,
        mode=target.mode,
        table=target.table,
        path=target.path,
        partition_by=target.partition_by,
        overwrite_schema=target.overwrite_schema,
    )

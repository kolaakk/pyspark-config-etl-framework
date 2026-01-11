from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def read_source(spark: SparkSession, fmt: str, path: str, options: dict) -> DataFrame:
    reader = spark.read.format(fmt)
    for k, v in (options or {}).items():
        reader = reader.option(k, v)
    return reader.load(path)


def write_delta(
    df: DataFrame,
    mode: str,
    table: str | None,
    path: str | None,
    partition_by: list[str] | None,
    overwrite_schema: bool,
) -> None:
    if not table and not path:
        raise ValueError("Target must define either 'table' or 'path'.")

    writer = df.write.format("delta").mode(mode)

    if overwrite_schema:
        writer = writer.option("overwriteSchema", "true")

    if partition_by:
        for c in partition_by:
            if c not in df.columns:
                raise ValueError(f"partition_by column '{c}' not found in output DataFrame.")
        writer = writer.partitionBy(*partition_by)

    if table:
        writer.saveAsTable(table)
    else:
        writer.save(path)

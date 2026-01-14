from __future__ import annotations

from typing import List
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from etl_framework.hashing import add_hashdiff
from etl_framework.config import TargetConfig


def _build_match_condition(keys: List[str], match_condition: str | None) -> str:
    if match_condition and match_condition.strip():
        return match_condition
    if not keys:
        raise ValueError("scd2 requires keys or match_condition.")
    return " AND ".join([f"t.{k} = s.{k}" for k in keys])


def run_scd2(spark: SparkSession, df: DataFrame, target: TargetConfig) -> None:
    try:
        from delta.tables import DeltaTable
    except Exception as e:
        raise RuntimeError("DeltaTable API not available (Databricks or delta-spark required).") from e

    cfg = target.scd2

    if not target.table and not target.path:
        raise ValueError("Target must define table or path for scd2.")
    if not cfg.keys and not (cfg.match_condition and cfg.match_condition.strip()):
        raise ValueError("scd2 requires keys or match_condition.")
    if not cfg.tracked_columns:
        raise ValueError("scd2 requires tracked_columns (attributes to detect changes).")

    match_cond = _build_match_condition(cfg.keys, cfg.match_condition)

    # Prepare source with SCD columns + hashdiff
    src = df
    src = add_hashdiff(src, cfg.tracked_columns, out_col="_hashdiff")

    now_ts = F.current_timestamp()
    src = (
        src.withColumn(cfg.effective_from_col, now_ts)
           .withColumn(cfg.effective_to_col, F.lit(None).cast("timestamp"))
           .withColumn(cfg.is_current_col, F.lit(True))
           .withColumn(cfg.soft_delete_column, F.lit(False))
           .withColumn(cfg.soft_delete_ts_column, F.lit(None).cast("timestamp"))
    )

    # Ensure target exists
    if target.table:
        if not spark.catalog.tableExists(target.table):
            src.write.format("delta").mode("overwrite").saveAsTable(target.table)
        delta_t = DeltaTable.forName(spark, target.table)
    else:
        hconf = spark.sparkContext._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hconf)
        p = spark._jvm.org.apache.hadoop.fs.Path(target.path)
        if not fs.exists(p):
            src.write.format("delta").mode("overwrite").save(target.path)
        delta_t = DeltaTable.forPath(spark, target.path)

    # SCD2 merge pattern: use "current rows only" match
    # Only match against current rows
    current_match = f"({match_cond}) AND t.{cfg.is_current_col} = true"

    s = src.alias("s")
    t = delta_t.alias("t")

    # 1) Expire current rows when changed
    # When matched and hashdiff differs -> close current row
    expire_set = {
        cfg.is_current_col: "false",
        cfg.effective_to_col: "current_timestamp()",
    }

    mb = (
        t.merge(s, current_match)
         .whenMatchedUpdate(
             condition="t._hashdiff <> s._hashdiff OR t._hashdiff IS NULL",
             set=expire_set
         )
    )

    # 2) Insert new rows for:
    # - new keys (not matched)
    # - changed keys (the current row was expired above, so it won't match in future inserts)
    # BUT Delta MERGE insert only happens for NOT MATCHED rows in THIS merge evaluation.
    # To insert for changed keys in same job, we add a staging trick:
    #   Build "to_insert" = (new keys) UNION (changed keys)
    #
    # We'll compute to_insert outside and write via a second merge as pure insert.

    mb.execute()

    # Compute rows that need inserting: new keys OR changed keys
    # Join source to current target after expiration and insert those missing
    tgt_df = delta_t.toDF().alias("tgt")
    src_df = src.alias("src")

    # Current keys after expiration
    current_keys_df = tgt_df.where(F.col(cfg.is_current_col) == True) \
                            .select(*[F.col(k).alias(k) for k in cfg.keys]) \
                            .dropDuplicates(cfg.keys)

    to_insert = (
        src_df.join(current_keys_df, on=cfg.keys, how="left_anti")
              .dropDuplicates(cfg.keys)
    )

    # Insert via merge with false condition to force inserts only
    ins_delta = delta_t.alias("t")
    (
        ins_delta.merge(to_insert.alias("s"), "1=0")
        .whenNotMatchedInsertAll()
        .execute()
    )

    # 3) Optional soft delete missing keys (close current row + mark deleted)
    if cfg.soft_delete_missing:
        # Current rows not in source -> expire and mark deleted
        missing_set = {
            cfg.is_current_col: "false",
            cfg.effective_to_col: "current_timestamp()",
            cfg.soft_delete_column: "true",
            cfg.soft_delete_ts_column: "current_timestamp()",
        }

        (
            delta_t.alias("t")
            .merge(src.alias("s"), current_match)
            .whenNotMatchedBySourceUpdate(set=missing_set)
            .execute()
        )

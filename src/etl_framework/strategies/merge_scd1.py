from __future__ import annotations

from typing import List
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from etl_framework.hashing import add_hashdiff
from etl_framework.config import TargetConfig

#Uses hashdiff to update only when changed
#Soft delete uses whenNotMatchedBySourceUpdate(...) to set is_deleted = true
def _build_match_condition(keys: List[str], match_condition: str | None) -> str:
    if match_condition and match_condition.strip():
        return match_condition
    if not keys:
        raise ValueError("merge_scd1 requires keys or match_condition.")
    return " AND ".join([f"t.{k} = s.{k}" for k in keys])


def run_merge_scd1(spark: SparkSession, df: DataFrame, target: TargetConfig) -> None:
    try:
        from delta.tables import DeltaTable
    except Exception as e:
        raise RuntimeError("DeltaTable API not available (Databricks or delta-spark required).") from e

    cfg = target.merge_scd1

    if not target.table and not target.path:
        raise ValueError("Target must define table or path for merge_scd1.")

    match_cond = _build_match_condition(cfg.keys, cfg.match_condition)

    # Ensure target exists
    if target.table:
        if not spark.catalog.tableExists(target.table):
            df.write.format("delta").mode("overwrite").saveAsTable(target.table)
        delta_t = DeltaTable.forName(spark, target.table)
    else:
        # path-based existence
        hconf = spark.sparkContext._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hconf)
        p = spark._jvm.org.apache.hadoop.fs.Path(target.path)
        if not fs.exists(p):
            df.write.format("delta").mode("overwrite").save(target.path)
        delta_t = DeltaTable.forPath(spark, target.path)

    staged = df

    # Hash-diff
    hash_col = "_hashdiff"
    if cfg.hashdiff_enabled:
        cols = cfg.hashdiff_columns[:] if cfg.hashdiff_columns else [c for c in df.columns if c not in cfg.keys]
        if not cols:
            raise ValueError("hashdiff_columns resolved to empty set.")
        staged = add_hashdiff(staged, cols, out_col=hash_col)

    s = staged.alias("s")
    t = delta_t.alias("t")
    mb = t.merge(s, match_cond)

    # UPDATE only when changed (if hashdiff enabled)
    if cfg.hashdiff_enabled:
        update_condition = f"t.{hash_col} <> s.{hash_col} OR t.{hash_col} IS NULL"
        if cfg.update_columns:
            set_map = {c: f"s.{c}" for c in cfg.update_columns}
            set_map[hash_col] = f"s.{hash_col}"
            # also un-delete if record reappears
            if cfg.soft_delete:
                set_map[cfg.soft_delete_column] = "false"
                set_map[cfg.soft_delete_ts_column] = "cast(NULL as timestamp)"
            mb = mb.whenMatchedUpdate(condition=update_condition, set=set_map)
        else:
            mb = mb.whenMatchedUpdateAll(condition=update_condition)
    else:
        if cfg.update_columns:
            set_map = {c: f"s.{c}" for c in cfg.update_columns}
            mb = mb.whenMatchedUpdate(set=set_map)
        else:
            mb = mb.whenMatchedUpdateAll()

    # INSERT
    if cfg.insert_columns:
        ins_map = {c: f"s.{c}" for c in cfg.insert_columns}
        if cfg.hashdiff_enabled:
            ins_map[hash_col] = f"s.{hash_col}"
        if cfg.soft_delete:
            ins_map[cfg.soft_delete_column] = "false"
            ins_map[cfg.soft_delete_ts_column] = "cast(NULL as timestamp)"
        mb = mb.whenNotMatchedInsert(values=ins_map)
    else:
        mb = mb.whenNotMatchedInsertAll()

    # SOFT DELETE missing
    if cfg.soft_delete:
        mb = mb.whenNotMatchedBySourceUpdate(set={
            cfg.soft_delete_column: "true",
            cfg.soft_delete_ts_column: "current_timestamp()"
        })

    mb.execute()

#Note: whenNotMatchedBySourceUpdate requires a reasonably recent Delta version (Databricks supports it; 
# open-source Delta does too in modern releases). If you run into a version issue, you can replace this 
# with a second merge pass or a left-anti update step.
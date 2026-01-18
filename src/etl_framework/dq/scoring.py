from __future__ import annotations

from pyspark.sql import DataFrame, functions as F

def add_scoring_columns(
    results_df: DataFrame,
    severity_weights: dict,
    status_multipliers: dict
) -> DataFrame:
    # Map severity/status to numbers using create_map
    sev_map = F.create_map([F.lit(x) for kv in severity_weights.items() for x in kv])
    st_map = F.create_map([F.lit(x) for kv in status_multipliers.items() for x in kv])

    scored = (
        results_df
        .withColumn("_sev_w", sev_map[F.col("severity")])
        .withColumn("_st_m", st_map[F.col("status")])
        .withColumn("dq_penalty", F.col("_sev_w") * F.col("_st_m"))
        .drop("_sev_w", "_st_m")
    )
    return scored

def compute_dataset_score(scored_results_df: DataFrame) -> DataFrame:
    # Higher is worse; convert to 0..100 score
    agg = scored_results_df.agg(F.sum("dq_penalty").alias("total_penalty"))
    return (
        agg.withColumn("dq_score", F.greatest(F.lit(0), F.lit(100) - F.col("total_penalty")))
           .withColumn(
               "dq_grade",
               F.when(F.col("dq_score") >= 95, F.lit("A"))
                .when(F.col("dq_score") >= 85, F.lit("B"))
                .when(F.col("dq_score") >= 70, F.lit("C"))
                .when(F.col("dq_score") >= 55, F.lit("D"))
                .otherwise(F.lit("F"))
           )
    )

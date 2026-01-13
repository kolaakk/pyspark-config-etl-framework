from __future__ import annotations

from typing import List
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_hashdiff(df: DataFrame, cols: List[str], out_col: str = "_hashdiff") -> DataFrame:
    """
    Stable hash across multiple columns. Null-safe stringification.
    """
    exprs = [F.coalesce(F.col(c).cast("string"), F.lit("∅")) for c in cols]
    return df.withColumn(out_col, F.sha2(F.concat_ws("||", *exprs), 256))

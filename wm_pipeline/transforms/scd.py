"""Change detection with row hashes and SCD Type 2 staging for MERGE."""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def row_hash(*cols: str):
    """sha2 over a fixed column order with an explicit null marker, so a NULL and an
    empty string, or ('ab','c') and ('a','bc'), never hash the same."""
    return F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in cols]), 256)


def latest_per_key(df: DataFrame, key: str, order_col: str = "updated_at") -> DataFrame:
    """Incremental extracts can carry the same key twice (overlap window); keep the newest."""
    w = Window.partitionBy(key).orderBy(F.col(order_col).desc(), F.col("_load_ts").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def stage_scd2(source: DataFrame, current: DataFrame, key: str) -> DataFrame:
    """Build the MERGE source for SCD2 ("staged union" pattern).

    source:  latest incoming rows, with _row_hash and effective_from.
    current: the target's is_current = true rows (key, _row_hash).

    Output = every source row with merge_key = key   (matches -> closes the old version,
                                                      or inserts a brand-new key)
           + changed rows again with merge_key = NULL (never matches -> inserts the new version)

    A single MERGE cannot both update and insert for the same key, which is why
    the changed rows are staged twice.
    """
    changed = (source.alias("s")
               .join(current.select(key, F.col("_row_hash").alias("_cur_hash")).alias("c"), key, "inner")
               .filter(F.col("_row_hash") != F.col("_cur_hash"))
               .drop("_cur_hash"))
    return (source.withColumn("merge_key", F.col(key))
            .unionByName(changed.withColumn("merge_key", F.lit(None).cast(source.schema[key].dataType))))

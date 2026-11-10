"""Trade validation, version de-duplication and split adjustment."""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

TRADE_COLUMNS = ["trade_id", "account_id", "security_id", "trade_date", "settle_date", "side",
                 "quantity", "price", "commission", "net_amount", "trade_status", "version",
                 "source_system", "exec_ts"]

# Lower number wins when two sources carry the same trade version: the end-of-day
# custodian file is the book of record, the intraday Kafka stream is early but provisional.
SOURCE_PRIORITY = {"FILE": 1, "STREAM": 2}


def validate_trades(trades: DataFrame, accounts: DataFrame, securities: DataFrame):
    """Split trades into (valid, rejected). Rejected rows carry reject_reason.

    accounts: DataFrame with account_id; securities: DataFrame with security_id.
    Referential checks use left_anti joins (the null-safe replacement for NOT IN), with
    the small dimension side broadcast so the trade side is never shuffled.
    """
    rule = (
        F.when(F.col("_corrupt_record").isNotNull(), "MALFORMED_RECORD")
        .when(F.col("trade_id").isNull(), "MISSING_TRADE_ID")
        .when(F.col("account_id").isNull(), "MISSING_ACCOUNT")
        .when(F.col("security_id").isNull(), "MISSING_SECURITY")
        # A null makes isin() return null, not false, so test it explicitly.
        .when(F.col("side").isNull() | ~F.col("side").isin("BUY", "SELL"), "INVALID_SIDE")
        .when(F.col("quantity").isNull() | (F.col("quantity") <= 0), "INVALID_QUANTITY")
        .when(F.col("price").isNull() | (F.col("price") <= 0), "INVALID_PRICE")
        .when(F.col("trade_date").isNull(), "MISSING_TRADE_DATE")
        .when(F.col("settle_date") < F.col("trade_date"), "SETTLE_BEFORE_TRADE")
    )
    checked = trades.withColumn("reject_reason", rule)
    structurally_ok = checked.filter(F.col("reject_reason").isNull())

    orphan_acct = (structurally_ok.join(F.broadcast(accounts.select("account_id").distinct()),
                                        "account_id", "left_anti")
                   .withColumn("reject_reason", F.lit("ORPHAN_ACCOUNT")))
    known_acct = structurally_ok.join(F.broadcast(accounts.select("account_id").distinct()),
                                      "account_id", "left_semi")
    unknown_sec = (known_acct.join(F.broadcast(securities.select("security_id").distinct()),
                                   "security_id", "left_anti")
                   .withColumn("reject_reason", F.lit("UNKNOWN_SECURITY")))
    valid = known_acct.join(F.broadcast(securities.select("security_id").distinct()),
                            "security_id", "left_semi").drop("reject_reason")

    rejected = (checked.filter(F.col("reject_reason").isNotNull())
                .unionByName(orphan_acct).unionByName(unknown_sec))
    return valid, rejected


def latest_trade_versions(trades: DataFrame) -> DataFrame:
    """Keep one row per trade_id: highest version, then the more trusted source, then
    the latest execution time. Deterministic, so a retry produces the same winner."""
    priority = F.create_map(*[x for k, v in SOURCE_PRIORITY.items() for x in (F.lit(k), F.lit(v))])
    w = Window.partitionBy("trade_id").orderBy(
        F.col("version").desc(), priority[F.col("record_source")].asc(), F.col("exec_ts").desc())
    return (trades.withColumn("_rn", F.row_number().over(w))
            .filter("_rn = 1").drop("_rn"))


def split_adjust(trades: DataFrame, splits: DataFrame) -> DataFrame:
    """Restate pre-split trades in post-split terms (quantity x ratio, price / ratio).

    splits: security_id, ex_date, ratio. Several splits on one security multiply, so
    the factor is exp(sum(log(ratio))) over every split with ex_date after the trade.
    Cost basis (qty x price) is unchanged by a split, which is the point.
    """
    factors = (trades.select("trade_id", "security_id", "trade_date")
               .join(F.broadcast(splits), "security_id")
               .where(F.col("trade_date") < F.col("ex_date"))
               .groupBy("trade_id")
               .agg(F.exp(F.sum(F.log(F.col("ratio").cast("double")))).alias("split_factor")))
    return (trades.join(factors, "trade_id", "left")
            .withColumn("split_factor", F.coalesce(F.round("split_factor", 8), F.lit(1.0)).cast("decimal(18,8)"))
            .withColumn("quantity", (F.col("quantity") * F.col("split_factor")).cast("decimal(18,4)"))
            .withColumn("price", (F.col("price") / F.col("split_factor")).cast("decimal(18,6)")))

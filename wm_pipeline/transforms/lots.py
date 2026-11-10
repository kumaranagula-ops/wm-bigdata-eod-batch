"""FIFO tax-lot accounting with window functions (no Python loops, no UDFs).

Idea: line up every BUY of an (account, security) on a running-quantity axis, and every
SELL on its own running axis. FIFO means the n-th share sold is the n-th share bought,
so a sell consumes exactly the buy lots whose quantity intervals overlap its interval.

    buys   [0 ---- 100)[100 ---- 150)[150 ----------- 300)
    sells  [0 ------------ 120)[120 -- 160)
    -> sell#1 relieves 100 from lot#1 and 20 from lot#2
    -> sell#2 relieves 30 from lot#2 and 10 from lot#3
    -> open: 140 shares left in lot#3

The join is a range (non-equi) join, but it also has equi-keys (account_id,
security_id), so Spark still hash-partitions on those keys instead of running a
cartesian product.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

KEYS = ["account_id", "security_id"]


def lot_ledger(trades: DataFrame):
    """Return (buys, sells), each with cum_start/cum_end on its own running axis.

    trades must be final, split-adjusted, non-cancelled: account_id, security_id,
    trade_id, trade_date, exec_ts, side, quantity, price, commission.
    """
    # Ordering by trade_id last makes the running sum deterministic on ties
    # (rowsBetween, not the default RANGE frame, so tied rows are not summed together).
    w = (Window.partitionBy(*KEYS).orderBy("trade_date", "exec_ts", "trade_id")
         .rowsBetween(Window.unboundedPreceding, Window.currentRow))

    def axis(side: str) -> DataFrame:
        df = trades.filter(F.col("side") == side)
        return (df.withColumn("cum_end", F.sum("quantity").over(w))
                  .withColumn("cum_start", F.col("cum_end") - F.col("quantity")))

    buys = axis("BUY").select(
        *KEYS, F.col("trade_id").alias("lot_id"), F.col("trade_date").alias("acquired_date"),
        F.col("quantity").alias("lot_qty"),
        # Commission is part of cost basis on a purchase.
        ((F.col("quantity") * F.col("price") + F.col("commission")) / F.col("quantity"))
        .cast("decimal(18,6)").alias("cost_per_share"),
        F.col("cum_start").alias("b_start"), F.col("cum_end").alias("b_end"))
    sells = axis("SELL").select(
        *KEYS, F.col("trade_id").alias("sell_trade_id"), F.col("trade_date").alias("sold_date"),
        # ...and reduces proceeds on a sale.
        ((F.col("quantity") * F.col("price") - F.col("commission")) / F.col("quantity"))
        .cast("decimal(18,6)").alias("proceeds_per_share"),
        F.col("cum_start").alias("s_start"), F.col("cum_end").alias("s_end"))
    return buys, sells


def term_of(days_col, long_term_days: int):
    # IRS rule: more than one year held = long-term capital gain.
    return F.when(days_col > long_term_days, "LONG_TERM").otherwise("SHORT_TERM")


def fifo_matches(buys: DataFrame, sells: DataFrame, long_term_days: int = 365) -> DataFrame:
    """One row per (sell, lot) pair with the quantity relieved and the realized gain."""
    b, s = buys.alias("b"), sells.alias("s")
    cond = [F.col("b.account_id") == F.col("s.account_id"),
            F.col("b.security_id") == F.col("s.security_id"),
            F.col("b.b_start") < F.col("s.s_end"),
            F.col("s.s_start") < F.col("b.b_end")]
    m = b.join(s, cond, "inner")
    qty = (F.least("b.b_end", "s.s_end") - F.greatest("b.b_start", "s.s_start")).cast("decimal(18,4)")
    days = F.datediff("s.sold_date", "b.acquired_date")
    return m.select(
        F.col("b.account_id").alias("account_id"), F.col("b.security_id").alias("security_id"),
        "s.sell_trade_id", "b.lot_id", "b.acquired_date", "s.sold_date",
        qty.alias("quantity"),
        (qty * F.col("b.cost_per_share")).cast("decimal(18,2)").alias("cost_basis"),
        (qty * F.col("s.proceeds_per_share")).cast("decimal(18,2)").alias("proceeds"),
        days.alias("holding_days"),
        term_of(days, long_term_days).alias("term"),
    ).withColumn("realized_gl", (F.col("proceeds") - F.col("cost_basis")).cast("decimal(18,2)"))


def open_lots(buys: DataFrame, sells: DataFrame, as_of_date: str, long_term_days: int = 365) -> DataFrame:
    """Remaining quantity of every lot after all sells: the part of [b_start, b_end)
    beyond the total quantity sold on that account/security."""
    sold = sells.groupBy(*KEYS).agg(F.max("s_end").alias("total_sold"))
    lots = (buys.join(sold, KEYS, "left")
            .withColumn("total_sold", F.coalesce("total_sold", F.lit(0)).cast("decimal(18,4)"))
            .filter(F.col("b_end") > F.col("total_sold"))
            .withColumn("remaining_qty",
                        (F.col("b_end") - F.greatest("b_start", "total_sold")).cast("decimal(18,4)")))
    days = F.datediff(F.lit(as_of_date).cast("date"), "acquired_date")
    return lots.select(
        *KEYS, "lot_id", "acquired_date", "remaining_qty", "cost_per_share",
        (F.col("remaining_qty") * F.col("cost_per_share")).cast("decimal(18,2)").alias("cost_basis"),
        days.alias("holding_days"), term_of(days, long_term_days).alias("term"))


def oversold(buys: DataFrame, sells: DataFrame) -> DataFrame:
    """Account/security pairs that sold more than they ever bought (a books error)."""
    bought = buys.groupBy(*KEYS).agg(F.max("b_end").alias("total_bought"))
    sold = sells.groupBy(*KEYS).agg(F.max("s_end").alias("total_sold"))
    return (sold.join(bought, KEYS, "left")
            .filter(F.col("total_sold") > F.coalesce("total_bought", F.lit(0))))

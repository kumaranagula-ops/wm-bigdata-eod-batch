"""Silver -> gold books and records for one as-of date (= business date).

  tax_lots_open        FIFO open lots with unrealized gain/loss, short vs long term
  positions_daily      lots rolled up to account x security, valued at the latest close
  realized_gains       YTD sells matched to the lots they relieved (FIFO), with term
  cash_balances_daily  trade-date and settlement-date cash (bookkeeping view)
  account_valuation_daily  one row per account: holdings + cash + P&L + estimated tax,
                       asset-class split (pivot), FA/branch as of the date (SCD2 join)

Gold is recomputed from silver for the date and written with dynamic partition
overwrite, so a rerun or a backfill of any date is idempotent.

Performance choices worth mentioning:
  * small dimensions (securities, FAs, branches, splits, prices) are broadcast, so
    the large trade/lot side is never shuffled for those joins
  * buys/sells are persisted because three outputs reuse them, then unpersisted
  * AQE is on: shuffle partitions are coalesced, and the Private Wealth team's
    trust accounts (the skewed key) get split if a join partition blows up
"""
from pyspark import StorageLevel
from pyspark.sql import Window
from pyspark.sql import functions as F

from wm_pipeline.context import insert_overwrite
from wm_pipeline.jobs import make_context
from wm_pipeline.quality import Checks
from wm_pipeline.transforms.lots import fifo_matches, lot_ledger, open_lots, oversold
from wm_pipeline.transforms.trades import split_adjust

ASSET_CLASSES = ["EQUITY", "ETF", "FIXED_INCOME", "MUTUAL_FUND"]


def run(args) -> int:
    ctx = make_context("gold_book_of_record", args)
    spark, bd = ctx.spark, ctx.business_date
    tax = ctx.cfg["tax"]
    lt_days = tax["long_term_holding_days"]
    silver = lambda t: spark.table(ctx.table("silver", t))  # noqa: E731
    gold = lambda t: ctx.table("gold", t)  # noqa: E731
    as_of = F.lit(bd)
    dq = Checks(ctx)

    with ctx.audit():
        # SCD2 "as of" join: the account version valid on the business date.
        acct = (silver("dim_account")
                .filter((F.col("effective_from") <= as_of) & (F.col("effective_to") >= as_of))
                .select("account_id", "account_number", "party_id", "account_type", "tax_status",
                        "fa_id", "status"))
        acct.cache()
        sec = F.broadcast(silver("dim_security").select("security_id", "cusip", "ticker", "asset_class"))
        fa = F.broadcast(silver("dim_financial_advisor").select("fa_id", "fa_name", "branch_id"))
        branch = F.broadcast(silver("dim_branch").select("branch_id", "branch_name", "region"))

        trades = (silver("fact_trade")
                  .filter((F.col("trade_date") <= as_of) & (F.col("trade_status") != "CANCEL"))
                  .select("trade_id", "account_id", "security_id", "trade_date", "settle_date", "exec_ts",
                          "side", "quantity", "price", "commission", "net_amount"))
        splits = (silver("ref_corporate_action")
                  .filter((F.col("event_type") == "SPLIT") & (F.col("ex_date") <= as_of))
                  .select("security_id", "ex_date", (F.col("ratio_new") / F.col("ratio_old")).alias("ratio")))
        adjusted = split_adjust(trades, splits)

        buys, sells = lot_ledger(adjusted)
        buys = buys.persist(StorageLevel.MEMORY_AND_DISK)
        sells = sells.persist(StorageLevel.MEMORY_AND_DISK)

        # Latest close on or before the as-of date (handles a vendor holiday or a late file).
        price = F.broadcast(
            silver("fact_price").filter(F.col("price_date") <= as_of)
            .groupBy("security_id")
            .agg(F.max_by("close_price", "price_date").alias("close_price"),
                 F.max("price_date").alias("price_date")))

        # ------------------------------------------------------------ open lots
        lots = (open_lots(buys, sells, bd, lt_days)
                .join(price, "security_id", "left")
                .withColumn("market_value", (F.col("remaining_qty") * F.col("close_price")).cast("decimal(18,2)"))
                .withColumn("unrealized_gl", (F.col("market_value") - F.col("cost_basis")).cast("decimal(18,2)"))
                .withColumn("as_of_date", as_of)
                .cache())
        insert_overwrite(lots, gold("tax_lots_open"))

        # ------------------------------------------------------------ positions
        positions = (lots.groupBy("account_id", "security_id")
                     .agg(F.sum("remaining_qty").cast("decimal(18,4)").alias("quantity"),
                          F.sum("cost_basis").cast("decimal(18,2)").alias("cost_basis"),
                          F.sum("market_value").cast("decimal(18,2)").alias("market_value"),
                          F.sum("unrealized_gl").cast("decimal(18,2)").alias("unrealized_gl"),
                          F.first("close_price").alias("close_price"),
                          F.first("price_date").alias("price_date"))
                     .withColumn("avg_cost", (F.col("cost_basis") / F.col("quantity")).cast("decimal(18,6)"))
                     .join(sec, "security_id", "left")
                     .join(acct.select("account_id", "account_number", "party_id", "fa_id"), "account_id", "left")
                     .withColumn("as_of_date", as_of)
                     .cache())
        insert_overwrite(positions, gold("positions_daily"))
        ctx.log.info("positions plan:")
        positions.explain("formatted")  # goes to the driver stdout in the YARN logs

        # ------------------------------------------------------------ realized gains (YTD)
        realized = (fifo_matches(buys, sells, lt_days)
                    .filter((F.year("sold_date") == F.year(as_of)) & (F.col("sold_date") <= as_of))
                    .join(acct.select("account_id", "tax_status"), "account_id", "left")
                    .withColumn("is_taxable", F.col("tax_status") == "TAXABLE")
                    .withColumn("as_of_date", as_of)
                    .cache())
        insert_overwrite(realized, gold("realized_gains"))

        # ------------------------------------------------------------ cash (TD vs SD)
        flows = (silver("fact_cash_activity").filter(F.col("txn_date") <= as_of)
                 .select("account_id", F.col("txn_date").alias("td"), F.col("txn_date").alias("sd"), "amount")
                 .unionByName(trades.select("account_id", F.col("trade_date").alias("td"),
                                            F.col("settle_date").alias("sd"),
                                            F.col("net_amount").alias("amount"))))
        daily = flows.groupBy("account_id", "td").agg(F.sum("amount").alias("day_flow"))
        # Running balance by date: explicit ROWS frame (the default RANGE frame with
        # orderBy groups ties together and surprises people).
        w = (Window.partitionBy("account_id").orderBy("td")
             .rowsBetween(Window.unboundedPreceding, Window.currentRow))
        last = Window.partitionBy("account_id").orderBy(F.col("td").desc())
        td_balance = (daily.withColumn("running_td", F.sum("day_flow").over(w))
                      .withColumn("_rn", F.row_number().over(last)).filter("_rn = 1")
                      .select("account_id", F.col("running_td").alias("cash_balance_td"),
                              F.col("td").alias("last_activity_date"),
                              F.when(F.col("td") == as_of, F.col("day_flow")).otherwise(F.lit(0)).alias("net_flow_today")))
        sd_balance = (flows.filter(F.col("sd") <= as_of).groupBy("account_id")
                      .agg(F.sum("amount").alias("cash_balance_sd")))
        cash = (td_balance.join(sd_balance, "account_id", "left")
                .select("account_id",
                        F.col("cash_balance_td").cast("decimal(18,2)").alias("cash_balance_td"),
                        F.coalesce("cash_balance_sd", F.lit(0)).cast("decimal(18,2)").alias("cash_balance_sd"),
                        F.col("net_flow_today").cast("decimal(18,2)").alias("net_flow_today"),
                        "last_activity_date", as_of.alias("as_of_date"))
                .cache())
        insert_overwrite(cash, gold("cash_balances_daily"))

        # ------------------------------------------------------------ account valuation
        # pivot with an explicit value list: no extra job to discover distinct values.
        by_class = (positions.groupBy("account_id").pivot("asset_class", ASSET_CLASSES)
                    .agg(F.sum("market_value"))
                    .withColumnsRenamed({"EQUITY": "equity_mv", "ETF": "etf_mv",
                                         "FIXED_INCOME": "fixed_income_mv", "MUTUAL_FUND": "mutual_fund_mv"}))
        totals = positions.groupBy("account_id").agg(F.sum("market_value").alias("securities_mv"),
                                                     F.sum("unrealized_gl").alias("unrealized_gl"))
        gains = (realized.groupBy("account_id", "is_taxable")
                 .agg(F.sum(F.when(F.col("term") == "SHORT_TERM", F.col("realized_gl")).otherwise(0)).alias("st"),
                      F.sum(F.when(F.col("term") == "LONG_TERM", F.col("realized_gl")).otherwise(0)).alias("lt"))
                 # Simplified estimate: net ST and net LT gains per account at flat rates;
                 # losses offset gains within the bucket, IRAs/Roths owe nothing now.
                 .withColumn("est_tax", F.when(
                     F.col("is_taxable"),
                     F.greatest(F.col("st"), F.lit(0)) * tax["short_term_rate"]
                     + F.greatest(F.col("lt"), F.lit(0)) * tax["long_term_rate"]).otherwise(0))
                 .drop("is_taxable"))
        zero = F.lit(0).cast("decimal(18,2)")
        valuation = (acct.join(totals, "account_id", "left")
                     .join(cash.select("account_id", F.col("cash_balance_td").alias("cash_balance")), "account_id", "left")
                     # Closed accounts with nothing left are not part of the book.
                     .filter((F.col("status") == "ACTIVE") | F.col("securities_mv").isNotNull())
                     .join(by_class, "account_id", "left")
                     .join(gains, "account_id", "left")
                     .join(fa, "fa_id", "left")
                     .join(branch, "branch_id", "left")
                     .select("account_id", "account_number", "party_id", "account_type", "tax_status",
                             "fa_id", "fa_name", "branch_id", "branch_name", "region",
                             F.coalesce(F.col("securities_mv").cast("decimal(18,2)"), zero).alias("securities_mv"),
                             F.coalesce(F.col("cash_balance").cast("decimal(18,2)"), zero).alias("cash_balance"),
                             (F.coalesce("securities_mv", zero) + F.coalesce("cash_balance", zero))
                             .cast("decimal(18,2)").alias("total_value"),
                             F.coalesce(F.col("unrealized_gl").cast("decimal(18,2)"), zero).alias("unrealized_gl"),
                             F.coalesce(F.col("st").cast("decimal(18,2)"), zero).alias("realized_st_ytd"),
                             F.coalesce(F.col("lt").cast("decimal(18,2)"), zero).alias("realized_lt_ytd"),
                             F.coalesce(F.col("est_tax").cast("decimal(18,2)"), zero).alias("est_tax_ytd"),
                             *[F.coalesce(F.col(c).cast("decimal(18,2)"), zero).alias(c)
                               for c in ["equity_mv", "etf_mv", "fixed_income_mv", "mutual_fund_mv"]],
                             as_of.alias("as_of_date")))
        insert_overwrite(valuation, gold("account_valuation_daily"))

        # ------------------------------------------------------------ data quality
        dq.expect_empty("no_oversold_positions", "silver.fact_trade", oversold(buys, sells))
        dq.expect_empty("positions_have_account", gold("positions_daily"),
                        positions.filter(F.col("account_number").isNull()))
        dq.expect_empty("positions_have_price", gold("positions_daily"),
                        positions.filter(F.col("close_price").isNull()), severity="WARN")
        dq.expect_empty("stale_prices", gold("positions_daily"),
                        positions.filter(F.col("price_date") < F.date_sub(as_of, 3)), severity="WARN",
                        details="close older than 3 days")
        dq.expect_empty("lots_tie_out_to_positions", gold("positions_daily"),
                        lots.groupBy("account_id", "security_id").agg(F.sum("remaining_qty").alias("q"))
                        .join(positions.select("account_id", "security_id", "quantity"), ["account_id", "security_id"])
                        .filter(F.col("q") != F.col("quantity")))
        dq.publish()

        ctx.metrics.update({
            "rows_in": trades.count(),
            "open_lots": lots.count(), "positions": positions.count(),
            "realized_rows": realized.count(), "accounts_valued": valuation.count(),
            "rows_out": positions.count(),
            "firm_aum": str(valuation.agg(F.sum("total_value")).first()[0]),
        })
        for df in (buys, sells, lots, positions, realized, cash, acct):
            df.unpersist()
    return 0

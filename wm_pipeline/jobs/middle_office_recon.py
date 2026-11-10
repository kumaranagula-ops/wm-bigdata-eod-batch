"""Middle-office reconciliation: our books vs the custodian's holdings statement.

The custodian identifies holdings by account number and CUSIP, our books by internal
account_id and security_id, so both sides are first mapped to the same keys. A full
outer join then classifies every difference:

  QTY_MISMATCH          both sides hold it, quantities differ
  MISSING_AT_CUSTODIAN  in our books only (unsettled trade? booking error?)
  MISSING_IN_BOOKS      at the custodian only (missed trade? corporate action?)

Breaks are business exceptions for the operations team, not pipeline failures, so
they are written to wm_audit.recon_breaks and reported; the batch fails only if the
break rate is abnormal (usually a sign of a bad file, not bad trades).
"""
from pyspark.sql import functions as F

from wm_pipeline.context import insert_overwrite
from wm_pipeline.jobs import make_context
from wm_pipeline.quality import Checks

MAX_BREAK_PCT = 2.0


def run(args) -> int:
    ctx = make_context("middle_office_recon", args)
    spark, bd = ctx.spark, ctx.business_date
    dq = Checks(ctx)

    with ctx.audit():
        book = (spark.table(ctx.table("gold", "positions_daily")).filter(F.col("as_of_date") == bd)
                .select("account_number", "cusip", "account_id", "security_id",
                        F.col("quantity").alias("book_quantity")))
        custodian = (spark.table(ctx.table("bronze", "custodian_positions_raw"))
                     .filter((F.col("business_date") == bd) & F.col("_corrupt_record").isNull())
                     .groupBy(F.col("custodian_account").alias("account_number"),
                              F.col("security_cusip").alias("cusip"))
                     .agg(F.sum("quantity").alias("custodian_quantity")))

        recon = book.join(custodian, ["account_number", "cusip"], "full_outer")
        diff = F.coalesce("book_quantity", F.lit(0)) - F.coalesce("custodian_quantity", F.lit(0))
        breaks = (recon.withColumn("quantity_diff", diff.cast("decimal(18,4)"))
                  .withColumn("break_type",
                              F.when(F.col("book_quantity").isNull(), "MISSING_IN_BOOKS")
                              .when(F.col("custodian_quantity").isNull(), "MISSING_AT_CUSTODIAN")
                              .when(F.col("quantity_diff") != 0, "QTY_MISMATCH"))
                  .filter(F.col("break_type").isNotNull()))

        # Fill internal ids for custodian-only rows so ops can look them up.
        acct_map = spark.table(ctx.table("silver", "dim_account")).filter("is_current") \
            .select("account_number", F.col("account_id").alias("_acct_id"))
        sec_map = spark.table(ctx.table("silver", "dim_security")).select("cusip", F.col("security_id").alias("_sec_id"))
        breaks = (breaks.join(F.broadcast(acct_map), "account_number", "left")
                  .join(F.broadcast(sec_map), "cusip", "left")
                  .withColumns({"account_id": F.coalesce("account_id", "_acct_id"),
                                "security_id": F.coalesce("security_id", "_sec_id"),
                                "detected_at": F.current_timestamp(),
                                "business_date": F.lit(bd)})
                  .cache())
        insert_overwrite(breaks, ctx.table("audit", "recon_breaks"))

        n_book, n_breaks = book.count(), breaks.count()
        by_type = {r["break_type"]: r["count"] for r in breaks.groupBy("break_type").count().collect()}
        break_pct = 100.0 * n_breaks / max(n_book, 1)
        for btype, n in sorted(by_type.items()):
            dq.expect_value(f"recon_{btype.lower()}", ctx.table("audit", "recon_breaks"), n, "WARN")
        dq.expect_value("recon_break_rate_normal", ctx.table("audit", "recon_breaks"),
                        int(break_pct > MAX_BREAK_PCT), "CRITICAL",
                        f"breaks={n_breaks} positions={n_book} pct={break_pct:.3f}")
        dq.publish()

        ctx.metrics.update({"rows_in": n_book, "rows_out": n_breaks, "breaks_by_type": by_type})
        ctx.log.info("reconciliation: %d positions, %d breaks %s", n_book, n_breaks, by_type)
        breaks.unpersist()
    return 0

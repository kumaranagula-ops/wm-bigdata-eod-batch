"""Vendor market data (security master + EOD prices) -> wm_bronze.

Source is S3 (s3a://) when WM_MARKET_SOURCE=s3, otherwise the HDFS landing zone, so the
project runs without an AWS account. Same code path either way, only the base path
changes: that is the point of keeping storage locations in config.

S3 notes for the interview:
  * s3a:// (hadoop-aws) is the open-source connector; s3:// is EMRFS on EMR only.
  * Credentials come from the default provider chain (here environment variables
    passed to the YARN containers). In production: IAM instance profile / IRSA,
    never keys in code.
  * Prices are Hive-style partitioned folders (price_date=YYYY-MM-DD), so reading
    one date only lists and reads that folder (partition pruning).
"""
from pyspark.sql import functions as F

from wm_pipeline import schemas
from wm_pipeline.context import env, insert_overwrite
from wm_pipeline.jobs import make_context


def market_base(ctx) -> str:
    if (env("WM_MARKET_SOURCE", "hdfs") or "hdfs").lower() == "s3":
        bucket = env("WM_S3_BUCKET", required=True)
        return f"s3a://{bucket}/{env('WM_S3_PREFIX', 'wm/market')}"
    return ctx.cfg["hdfs"]["landing_market"]


def run(args) -> int:
    ctx = make_context("bronze_market", args)
    spark, bd = ctx.spark, ctx.business_date
    base = market_base(ctx)
    ctx.log.info("market data source: %s", base)

    with ctx.audit():
        sec = (spark.read.schema(schemas.SECURITY_MASTER_CSV).option("header", True)
               .csv(f"{base}/security_master/"))
        sec = ctx.add_audit_columns(sec).withColumn("load_date", F.lit(bd))
        insert_overwrite(sec, ctx.table("bronze", "security_master_raw"))

        # Reload a 10-day window of prices each run: vendors restate recent closes.
        prices = (spark.read.schema(schemas.PRICES_JSON + ", price_date DATE")
                  .json(f"{base}/prices/")
                  .filter(F.col("price_date").between(F.date_sub(F.lit(bd), 10), F.lit(bd))))
        prices = ctx.add_audit_columns(prices).withColumn("price_date", F.col("price_date").cast("string"))
        insert_overwrite(prices, ctx.table("bronze", "prices_raw"))

        n_sec, n_px = sec.count(), prices.count()
        missing_today = prices.filter(F.col("price_date") == bd).count() == 0
        ctx.metrics.update({"security_master.rows": n_sec, "prices.rows": n_px,
                            "prices_missing_for_business_date": missing_today,
                            "rows_in": n_sec + n_px, "rows_out": n_sec + n_px})
        if missing_today:
            ctx.log.warning("no vendor prices for %s yet; gold will use the latest earlier close", bd)
    return 0

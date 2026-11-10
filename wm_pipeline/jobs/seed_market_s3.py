"""One-off: upload the generated vendor market data to S3 (plays the market-data vendor).

Run by `make seed-s3` in a local-mode Spark container, using s3a. The security master
is written as CSV (as vendors usually send it) and prices as JSON Lines under
price_date=... folders, so the bronze job exercises two formats and partition
discovery on S3.
"""
from wm_pipeline import schemas
from wm_pipeline.context import env
from wm_pipeline.jobs import make_context


def run(args) -> int:
    ctx = make_context("seed_market_s3", args)
    spark = ctx.spark
    src = args.options.get("src", "/opt/wm/data/market")
    dest = f"s3a://{env('WM_S3_BUCKET', required=True)}/{env('WM_S3_PREFIX', 'wm/market')}"

    sec = spark.read.schema(schemas.SECURITY_MASTER_CSV).option("header", True).csv(f"file://{src}/security_master/")
    # The bronze reader expects CSV for the master (as vendors usually send), so keep CSV.
    sec.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{dest}/security_master/")
    prices = spark.read.schema(schemas.PRICES_JSON + ", price_date DATE").json(f"file://{src}/prices/")
    prices.repartition("price_date").write.mode("overwrite").partitionBy("price_date").json(f"{dest}/prices/")
    ctx.log.info("uploaded %d securities and %d prices to %s", sec.count(), prices.count(), dest)
    return 0

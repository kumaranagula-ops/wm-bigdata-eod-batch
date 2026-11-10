"""NAS feeds (already copied to the HDFS landing zone by Airflow) -> wm_bronze.*

Shows the file-reading options that matter in production:
  * explicit schemas, no inferSchema (no extra scan, no leading-zero surprises)
  * mode=PERMISSIVE + columnNameOfCorruptRecord: bad rows are kept and flagged, not lost
  * pipe delimiter and a non-ISO dateFormat for the cash file
  * JSON Lines with a nested struct for corporate actions
  * audit columns (_source_file, _load_ts, _batch_id) for lineage back to the file
  * dynamic partition overwrite: rerunning a date replaces only that date
"""
from pyspark.sql import functions as F

from wm_pipeline import schemas
from wm_pipeline.context import insert_overwrite
from wm_pipeline.jobs import make_context


def run(args) -> int:
    ctx = make_context("bronze_nas_files", args)
    spark, bd = ctx.spark, ctx.business_date
    ymd = bd.replace("-", "")
    landing = f"{ctx.cfg['hdfs']['landing_nas']}/{bd}"

    with ctx.audit():
        def csv(path, schema, **opts):
            reader = (spark.read.schema(schema)
                      .option("header", True)
                      .option("mode", "PERMISSIVE")
                      .option("columnNameOfCorruptRecord", "_corrupt_record")
                      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss"))
            for k, v in opts.items():
                reader = reader.option(k, v)
            # Cache before filtering on _corrupt_record: Spark refuses queries that only
            # reference the corrupt-record column of a raw CSV/JSON scan.
            return ctx.add_audit_columns(reader.csv(path)).cache()

        feeds = {
            "trades_raw": csv(f"{landing}/trades_{ymd}.csv", schemas.TRADES_CSV, dateFormat="yyyy-MM-dd"),
            "cash_activity_raw": csv(f"{landing}/cash_activity_{ymd}.psv", schemas.CASH_PSV,
                                     sep="|", dateFormat="MM/dd/yyyy"),
            "custodian_positions_raw": csv(f"{landing}/custodian_positions_{ymd}.csv",
                                           schemas.CUSTODIAN_POSITIONS_CSV, dateFormat="yyyy-MM-dd"),
            "corporate_actions_raw": ctx.add_audit_columns(
                spark.read.schema(schemas.CORPORATE_ACTIONS_JSON)
                .option("mode", "PERMISSIVE")
                .option("columnNameOfCorruptRecord", "_corrupt_record")
                .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
                .json(f"{landing}/corporate_actions_{ymd}.json")).cache(),
        }

        for table, df in feeds.items():
            out = df.withColumn("business_date", F.lit(bd))
            insert_overwrite(out, ctx.table("bronze", table))
            total = df.count()
            corrupt = df.filter(F.col("_corrupt_record").isNotNull()).count()
            ctx.metrics[f"{table}.rows"] = total
            ctx.metrics[f"{table}.corrupt"] = corrupt
            ctx.log.info("%s: %d rows (%d malformed, kept for quarantine)", table, total, corrupt)
            df.unpersist()

        ctx.metrics["rows_in"] = sum(v for k, v in ctx.metrics.items() if k.endswith(".rows"))
        ctx.metrics["rows_out"] = ctx.metrics["rows_in"]
    return 0

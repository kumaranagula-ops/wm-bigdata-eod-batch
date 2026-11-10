"""Gold marts -> Snowflake (WM_DB.GOLD) with the Spark Snowflake connector.

  * Key-pair auth: the private key file is shipped to the YARN driver with --files
    and passed as pem_private_key; no password anywhere.
  * Idempotent: a preaction deletes the as-of date before appending it, so reruns
    and backfills never duplicate a day (the connector runs both in one session).
  * column_mapping=name matches columns by name, not position.
  * The connector stages data in Snowflake internal stage and runs COPY INTO, which
    is far faster than JDBC row inserts.
"""
from pathlib import Path

from pyspark.sql import functions as F

from wm_pipeline.context import env
from wm_pipeline.jobs import make_context

SF_FORMAT = "net.snowflake.spark.snowflake"


def private_key_body(path: str) -> str:
    lines = Path(path).read_text().strip().splitlines()
    return "".join(ln for ln in lines if not ln.startswith("-----"))


def run(args) -> int:
    ctx = make_context("publish_snowflake", args)
    spark, bd = ctx.spark, ctx.business_date
    key_file = args.options.get("key_file", "sf_key.p8")  # name given in --files ...#sf_key.p8
    opts = {
        "sfURL": f"{env('WM_SF_ACCOUNT', required=True).lower()}.snowflakecomputing.com",
        "sfUser": env("WM_SF_USER", required=True),
        "pem_private_key": private_key_body(key_file),
        "sfRole": env("WM_SF_ROLE", "SYSADMIN"),
        "sfWarehouse": env("WM_SF_WAREHOUSE", required=True),
        "sfDatabase": env("WM_SF_DATABASE", "WM_DB"),
        "sfSchema": env("WM_SF_SCHEMA", "GOLD"),
        "column_mapping": "name",
    }

    with ctx.audit():
        total = 0
        for hive_table, sf_table in ctx.cfg["snowflake_tables"].items():
            df = spark.table(ctx.table("gold", hive_table)).filter(F.col("as_of_date") == bd) \
                      .withColumn("as_of_date", F.col("as_of_date").cast("date"))
            n = df.count()
            (df.write.format(SF_FORMAT).options(**opts)
             .option("dbtable", sf_table)
             .option("preactions", f"DELETE FROM {sf_table} WHERE AS_OF_DATE = '{bd}'")
             .mode("append")
             .save())
            ctx.metrics[f"{sf_table}.rows"] = n
            ctx.log.info("published %s rows for %s to %s.%s.%s", n, bd, opts["sfDatabase"], opts["sfSchema"], sf_table)
            total += n
        ctx.metrics["rows_in"] = ctx.metrics["rows_out"] = total
    return 0

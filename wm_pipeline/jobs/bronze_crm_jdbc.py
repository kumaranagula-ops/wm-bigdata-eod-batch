"""MySQL CRM tables -> wm_bronze.crm_*_raw, incremental by updated_at watermark.

This is the job Sqoop used to do (`sqoop import --incremental lastmodified
--check-column updated_at --split-by account_id -m 4`), written with Spark JDBC:

  * Watermark per table AND business date in lake.wm_audit.jdbc_watermark. The low
    watermark is the previous date's high watermark minus an overlap window (late
    commits in the source), so a rerun of the same date pulls the same window.
  * Parallel read: partitionColumn + lowerBound/upperBound + numPartitions turn one
    query into N range queries (one per task). Bounds only set the stride; they do
    not filter rows.
  * The filter is pushed into a subquery in dbtable (query= cannot be combined with
    partitionColumn).
  * fetchsize raised from the driver default to cut round trips.
"""
import datetime as dt

from pyspark.sql import functions as F

from wm_pipeline.context import env, insert_overwrite
from wm_pipeline.jobs import make_context

EPOCH = dt.datetime(1900, 1, 1)


def run(args) -> int:
    ctx = make_context("bronze_crm_jdbc", args)
    spark, bd, jcfg = ctx.spark, ctx.business_date, ctx.cfg["jdbc"]
    wm_table = ctx.table("audit_iceberg", "jdbc_watermark")
    props = {
        "user": env("WM_MYSQL_USER", required=True),
        "password": env("WM_MYSQL_PASSWORD", required=True),
        "driver": jcfg["driver"],
        "fetchsize": str(jcfg["fetchsize"]),
    }

    def jdbc_query(sql: str):
        return spark.read.jdbc(jcfg["url"], f"({sql}) q", properties=props)

    with ctx.audit():
        total = 0
        for table, tcfg in jcfg["tables"].items():
            prev = spark.sql(f"""
                SELECT max(high_watermark) AS hw FROM {wm_table}
                WHERE source_table = '{table}' AND business_date < '{bd}'""").first()["hw"]
            low = (prev - dt.timedelta(minutes=jcfg["overlap_minutes"])) if prev else EPOCH
            where = f"updated_at > '{low:%Y-%m-%d %H:%M:%S}'"

            reader_opts = {}
            if tcfg["partition_column"]:
                col = tcfg["partition_column"]
                b = jdbc_query(f"SELECT min({col}) lo, max({col}) hi FROM {table} WHERE {where}").first()
                if b["lo"] is not None:
                    reader_opts = dict(column=col, lowerBound=int(b["lo"]), upperBound=int(b["hi"]) + 1,
                                       numPartitions=tcfg["num_partitions"])
            df = spark.read.jdbc(jcfg["url"], f"(SELECT * FROM {table} WHERE {where}) src",
                                 properties=props, **reader_opts)

            out = ctx.add_audit_columns(df, with_file=False).withColumn("extract_date", F.lit(bd)).cache()
            n = out.count()
            insert_overwrite(out, ctx.table("bronze", f"crm_{table}_raw"))
            high = out.agg(F.max("updated_at")).first()[0] or prev or low
            out.unpersist()

            # Record the window only after the data landed (no gap if the write fails).
            spark.sql(f"""
                MERGE INTO {wm_table} t
                USING (SELECT '{table}' AS source_table, '{bd}' AS business_date,
                              TIMESTAMP '{low:%Y-%m-%d %H:%M:%S}' AS low_watermark,
                              TIMESTAMP '{high:%Y-%m-%d %H:%M:%S}' AS high_watermark,
                              CAST({n} AS BIGINT) AS rows_extracted,
                              current_timestamp() AS updated_at) s
                ON t.source_table = s.source_table AND t.business_date = s.business_date
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *""")
            ctx.metrics[f"{table}.rows"] = n
            ctx.metrics[f"{table}.window"] = f"{low} -> {high}"
            ctx.metrics[f"{table}.partitions"] = reader_opts.get("numPartitions", 1)
            ctx.log.info("%s: %d rows, window %s -> %s, %s JDBC partitions",
                         table, n, low, high, reader_opts.get("numPartitions", 1))
            total += n
        ctx.metrics["rows_in"] = ctx.metrics["rows_out"] = total
    return 0

"""Kafka topic wm.intraday.trades -> wm_bronze.trades_stream_raw (Structured Streaming).

Run with trigger(availableNow=True): it consumes everything published since the last
checkpoint, in bounded micro-batches, then stops. That turns a streaming source into
an incremental batch step Airflow can schedule, and it is exactly-once with respect to
Kafka offsets because progress lives in the HDFS checkpoint.

  * The checkpoint location is per stream and must never be shared or deleted, or
    the job either reprocesses or skips data.
  * maxOffsetsPerTrigger bounds each micro-batch (back-pressure).
  * foreachBatch lets each micro-batch use the normal batch writer (Hive insertInto).
  * Kafka is at-least-once on the producer side, so duplicates are expected here and
    removed in silver (latest version per trade_id).
"""
from pyspark.sql import functions as F

from wm_pipeline import schemas
from wm_pipeline.context import env
from wm_pipeline.jobs import make_context


def run(args) -> int:
    ctx = make_context("bronze_trades_stream", args)
    spark, kcfg = ctx.spark, ctx.cfg["kafka"]
    target = ctx.table("bronze", "trades_stream_raw")
    checkpoint = f"{ctx.cfg['hdfs']['checkpoints']}/trades_stream_raw"
    columns = spark.table(target).columns

    with ctx.audit():
        raw = (spark.readStream.format("kafka")
               .option("kafka.bootstrap.servers", env("WM_KAFKA_BOOTSTRAP", kcfg["bootstrap_servers"]))
               .option("subscribe", kcfg["topic"])
               .option("startingOffsets", "earliest")
               .option("maxOffsetsPerTrigger", kcfg["max_offsets_per_trigger"])
               .option("failOnDataLoss", "false")
               .load())

        events = (raw.select(F.from_json(F.col("value").cast("string"), schemas.TRADE_EVENT_JSON).alias("e"),
                             F.col("partition").alias("kafka_partition"), F.col("offset").alias("kafka_offset"))
                  .select("e.*", "kafka_partition", "kafka_offset")
                  .withColumns({"_load_ts": F.current_timestamp(),
                                "_batch_id": F.lit(ctx.batch_id),
                                "event_date": F.date_format("event_ts", "yyyy-MM-dd")}))

        counts = {"batches": 0, "rows": 0}

        def write_batch(batch_df, batch_id):
            n = batch_df.count()
            if n:
                batch_df.select(*columns).write.insertInto(target)  # append
            counts["batches"] += 1
            counts["rows"] += n
            ctx.log.info("micro-batch %s: %d events", batch_id, n)

        query = (events.writeStream
                 .foreachBatch(write_batch)
                 .option("checkpointLocation", checkpoint)
                 .trigger(availableNow=True)
                 .start())
        query.awaitTermination()

        ctx.metrics.update({"micro_batches": counts["batches"], "rows_in": counts["rows"],
                            "rows_out": counts["rows"], "checkpoint": checkpoint})
        ctx.log.info("stream caught up: %d events in %d micro-batches", counts["rows"], counts["batches"])
    return 0

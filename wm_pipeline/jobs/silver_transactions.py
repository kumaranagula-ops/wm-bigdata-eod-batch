"""Bronze trades (file + Kafka), cash and corporate actions -> silver facts.

Trade pipeline for one business date:
  1. union the custodian file and the Kafka stream by column name
  2. drop exact duplicate rows (re-sent file lines, redelivered Kafka events)
  3. validate: malformed, missing keys, bad quantity/price, orphan account, unknown security
     -> rejects go to silver.quarantine with a reason (not silently dropped)
  4. keep the latest version per trade_id (corrections v2 beat originals, CANCEL wins)
  5. MERGE into silver.fact_trade, only replacing an existing row with a newer version
  6. reconcile counts: rows in = merged + rejected + duplicates + superseded versions
"""
from pyspark.sql import functions as F

from wm_pipeline.jobs import make_context
from wm_pipeline.quality import Checks
from wm_pipeline.transforms.trades import TRADE_COLUMNS, latest_trade_versions, validate_trades


def quarantine(ctx, df, source_table: str, key_col: str):
    raw = F.coalesce(F.col("_corrupt_record"), F.to_json(F.struct(*[c for c in df.columns if not c.startswith("_")]))) \
        if "_corrupt_record" in df.columns else F.to_json(F.struct(*df.columns))
    return df.select(F.lit(source_table).alias("source_table"),
                     F.col(key_col).cast("string").alias("record_key"),
                     F.col("reject_reason"), raw.alias("raw_record"),
                     F.current_timestamp().alias("_load_ts"),
                     F.lit(ctx.business_date).alias("business_date"))


def run(args) -> int:
    ctx = make_context("silver_transactions", args)
    spark, bd = ctx.spark, ctx.business_date
    silver = lambda t: ctx.table("silver", t)  # noqa: E731
    dq = Checks(ctx)

    with ctx.audit():
        # ---------------------------------------------------------------- trades
        file_trades = (spark.table(ctx.table("bronze", "trades_raw")).filter(F.col("business_date") == bd)
                       .select(*TRADE_COLUMNS, "_corrupt_record").withColumn("record_source", F.lit("FILE")))
        stream_trades = (spark.table(ctx.table("bronze", "trades_stream_raw")).filter(F.col("event_date") == bd)
                         .select(*TRADE_COLUMNS).withColumn("record_source", F.lit("STREAM")))
        # unionByName + allowMissingColumns: the stream has no _corrupt_record column.
        incoming = file_trades.unionByName(stream_trades, allowMissingColumns=True).cache()
        n_in = incoming.count()

        deduped = incoming.dropDuplicates(TRADE_COLUMNS + ["record_source"])
        n_exact_dupes = n_in - deduped.count()

        accounts = spark.table(silver("dim_account")).select("account_id")
        securities = spark.table(silver("dim_security")).select("security_id")
        valid, rejected = validate_trades(deduped, accounts, securities)
        valid = valid.cache()
        n_valid = valid.count()

        latest = latest_trade_versions(valid)
        merged_src = latest.select(*TRADE_COLUMNS, "record_source",
                                   F.lit(bd).alias("business_date"), F.lit(ctx.batch_id).alias("_batch_id"))
        merged_src.createOrReplaceTempView("trades_src")
        n_latest = merged_src.count()
        spark.sql(f"""
            MERGE INTO {silver('fact_trade')} t
            USING trades_src s
            ON t.trade_id = s.trade_id
            WHEN MATCHED AND (s.version > t.version
                              OR (s.version = t.version AND s.record_source = 'FILE' AND t.record_source = 'STREAM'))
                THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *""")

        q_trades = quarantine(ctx, rejected, "trades", "trade_id")

        # ---------------------------------------------------------------- cash
        cash = (spark.table(ctx.table("bronze", "cash_activity_raw")).filter(F.col("business_date") == bd)
                .dropDuplicates(["txn_id"]))
        cash_rule = (F.when(F.col("_corrupt_record").isNotNull(), "MALFORMED_RECORD")
                     .when(F.col("txn_date").isNull(), "MISSING_TXN_DATE")
                     .when(F.col("amount").isNull(), "MISSING_AMOUNT")
                     .when(F.col("txn_type").isNull() | ~F.col("txn_type").isin("DEPOSIT", "WITHDRAWAL", "DIVIDEND", "INTEREST", "FEE"),
                           "INVALID_TXN_TYPE"))
        cash = cash.withColumn("reject_reason", cash_rule)
        cash_ok = (cash.filter(F.col("reject_reason").isNull())
                   .join(F.broadcast(accounts), "account_id", "left_semi"))
        cash_orphan = (cash.filter(F.col("reject_reason").isNull())
                       .join(F.broadcast(accounts), "account_id", "left_anti")
                       .withColumn("reject_reason", F.lit("ORPHAN_ACCOUNT")))
        q_cash = quarantine(ctx, cash.filter(F.col("reject_reason").isNotNull()).unionByName(cash_orphan),
                            "cash_activity", "txn_id")
        (cash_ok.select("txn_id", "account_id", "txn_date", "txn_type", "amount", "currency",
                        F.when(F.col("security_id") != "", F.col("security_id")).alias("security_id"),
                        "description",
                        F.lit(bd).alias("business_date"), F.lit(ctx.batch_id).alias("_batch_id"))
         .createOrReplaceTempView("cash_src"))
        spark.sql(f"""
            MERGE INTO {silver('fact_cash_activity')} t USING cash_src s ON t.txn_id = s.txn_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *""")

        # ---------------------------------------------------------------- corporate actions
        ca = spark.table(ctx.table("bronze", "corporate_actions_raw")).filter(F.col("business_date") == bd)
        ca_bad = ca.filter(F.col("_corrupt_record").isNotNull() | F.col("ca_id").isNull()) \
                   .withColumn("reject_reason", F.lit("MALFORMED_RECORD"))
        q_ca = quarantine(ctx, ca_bad, "corporate_actions", "ca_id")
        (ca.filter(F.col("_corrupt_record").isNull() & F.col("ca_id").isNotNull())
         .dropDuplicates(["ca_id"])
         # Flatten the nested struct for silver (struct.field access).
         .select("ca_id", "security_id", "event_type", "ex_date", "pay_date",
                 F.col("terms.ratio_new").alias("ratio_new"), F.col("terms.ratio_old").alias("ratio_old"),
                 F.col("terms.cash_rate").alias("cash_rate"), "announced_ts",
                 F.lit(ctx.batch_id).alias("_batch_id"))
         .createOrReplaceTempView("ca_src"))
        spark.sql(f"""
            MERGE INTO {silver('ref_corporate_action')} t USING ca_src s ON t.ca_id = s.ca_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *""")

        # ---------------------------------------------------------------- quarantine (idempotent)
        all_q = q_trades.unionByName(q_cash).unionByName(q_ca).cache()
        n_q = all_q.count()
        (all_q.select(*spark.table(silver("quarantine")).columns)
         .writeTo(silver("quarantine")).overwritePartitions())
        reasons = {r["k"]: r["n"] for r in all_q.groupBy(F.concat_ws(":", "source_table", "reject_reason").alias("k"))
                   .count().withColumnRenamed("count", "n").collect()}
        n_trade_rejects = q_trades.count()

        # ---------------------------------------------------------------- reconciliation + DQ
        n_superseded = n_valid - n_latest
        unexplained = n_in - (n_latest + n_trade_rejects + n_exact_dupes + n_superseded)
        dq.expect_value("trade_rowcount_reconciles", silver("fact_trade"), abs(unexplained), "CRITICAL",
                        f"in={n_in} merged={n_latest} rejected={n_trade_rejects} "
                        f"dupes={n_exact_dupes} superseded={n_superseded}")
        reject_pct = 100.0 * n_trade_rejects / max(n_in, 1)
        dq.expect_value("trade_reject_pct_within_threshold", silver("quarantine"),
                        int(reject_pct > ctx.cfg["dq"]["max_quarantine_pct"]), "CRITICAL",
                        f"reject_pct={reject_pct:.2f} threshold={ctx.cfg['dq']['max_quarantine_pct']}")
        fact = spark.table(silver("fact_trade"))
        dq.expect_empty("fact_trade_unique_trade_id", silver("fact_trade"),
                        fact.groupBy("trade_id").count().filter("count > 1"))
        dq.expect_empty("fact_trade_net_amount_consistent", silver("fact_trade"),
                        fact.filter(F.col("business_date") == bd).filter(
                            F.abs(F.col("net_amount") - F.when(F.col("side") == "BUY",
                                  -(F.col("quantity") * F.col("price") + F.col("commission")))
                                  .otherwise(F.col("quantity") * F.col("price") - F.col("commission"))) > 0.05),
                        severity="WARN", details="net = -(qty*price+comm) for buys, qty*price-comm for sells")
        dq.publish()

        ctx.metrics.update({"rows_in": n_in, "rows_out": n_latest, "rows_rejected": n_q,
                            "trades.exact_duplicates": n_exact_dupes, "trades.superseded_versions": n_superseded,
                            "reject_reasons": reasons})
        for df in (incoming, valid, all_q):
            df.unpersist()
    return 0

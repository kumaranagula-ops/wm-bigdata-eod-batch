"""Bronze CRM + market extracts -> silver dimensions (Iceberg MERGE).

  dim_branch, dim_financial_advisor, dim_party, dim_security : SCD Type 1 (overwrite
      changed attributes, history not needed), change detected with a row hash.
  dim_account : SCD Type 2. FA, account type, tax status and status changes are
      audit-relevant: a position on a past date must roll up to the advisor who
      serviced the account on that date.
  fact_price  : upsert by (security_id, price_date).
"""
from __future__ import annotations

from pyspark.sql import functions as F

from wm_pipeline.jobs import make_context
from wm_pipeline.quality import Checks
from wm_pipeline.transforms.scd import latest_per_key, row_hash, stage_scd2

SCD2_TRACKED = ["account_number", "party_id", "account_type", "tax_status", "fa_id", "open_date",
                "close_date", "status", "base_currency", "cost_basis_method"]


def merge_scd1(ctx, source_df, target: str, key: str, hash_cols: list[str]) -> int:
    src = source_df.withColumn("_row_hash", row_hash(*hash_cols)).withColumn("_batch_id", F.lit(ctx.batch_id))
    src = src.select(*ctx.spark.table(target).columns)
    src.createOrReplaceTempView("src")
    ctx.spark.sql(f"""
        MERGE INTO {target} t USING src s ON t.{key} = s.{key}
        WHEN MATCHED AND t._row_hash <> s._row_hash THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *""")
    return src.count()


def run(args) -> int:
    ctx = make_context("silver_reference", args)
    spark, bd = ctx.spark, ctx.business_date
    bronze = lambda t: spark.table(ctx.table("bronze", t))  # noqa: E731
    silver = lambda t: ctx.table("silver", t)  # noqa: E731

    with ctx.audit():
        # ---------------- SCD1 dimensions from today's CRM extract
        branch = latest_per_key(bronze("crm_branch_raw").filter(F.col("extract_date") == bd), "branch_id")
        ctx.metrics["dim_branch"] = merge_scd1(
            ctx, branch, silver("dim_branch"), "branch_id",
            ["branch_name", "city", "state", "region"])

        fa = latest_per_key(bronze("crm_financial_advisor_raw").filter(F.col("extract_date") == bd), "fa_id")
        fa = fa.withColumn("fa_name", F.concat_ws(" ", "first_name", "last_name"))
        ctx.metrics["dim_financial_advisor"] = merge_scd1(
            ctx, fa, silver("dim_financial_advisor"), "fa_id",
            ["fa_code", "fa_name", "branch_id", "title", "hire_date", "status"])

        party = latest_per_key(bronze("crm_party_raw").filter(F.col("extract_date") == bd), "party_id")
        party = party.withColumns({
            "party_name": F.when(F.col("party_type") == "ORG", F.col("org_name"))
                           .otherwise(F.concat_ws(" ", "first_name", "last_name")),
            # Governance: analytics never needs the full e-mail address.
            "email_masked": F.regexp_replace("email", r"(^.).*(@.*$)", "$1***$2"),
        })
        ctx.metrics["dim_party"] = merge_scd1(
            ctx, party, silver("dim_party"), "party_id",
            ["party_type", "party_name", "date_of_birth", "tax_id_last4", "risk_profile",
             "kyc_status", "country", "state", "email_masked"])

        # ---------------- SCD2 account dimension
        acct = latest_per_key(bronze("crm_account_raw").filter(F.col("extract_date") == bd), "account_id")
        target = silver("dim_account")
        current = spark.table(target).filter("is_current")
        known = current.select("account_id")
        acct = (acct.withColumn("_row_hash", row_hash(*SCD2_TRACKED))
                .join(known.withColumn("_known", F.lit(True)), "account_id", "left")
                # First time we see an account its history starts at open_date; a change
                # takes effect on the day it was made in the source.
                .withColumn("effective_from",
                            F.when(F.col("_known"), F.to_date("updated_at")).otherwise(F.col("open_date")))
                .withColumns({
                    "effective_to": F.lit("9999-12-31").cast("date"),
                    "is_current": F.lit(True),
                    # Deterministic surrogate key: same account version -> same key on rerun.
                    "account_sk": F.xxhash64("account_id", "effective_from"),
                    "_batch_id": F.lit(ctx.batch_id)})
                .drop("_known"))
        staged = stage_scd2(acct, current, "account_id")
        cols = spark.table(target).columns
        staged.select(*cols, "merge_key").createOrReplaceTempView("staged")
        spark.sql(f"""
            MERGE INTO {target} t
            USING staged s
            ON t.account_id = s.merge_key AND t.is_current = true
            WHEN MATCHED AND t._row_hash <> s._row_hash THEN
                UPDATE SET t.is_current = false, t.effective_to = date_sub(s.effective_from, 1)
            WHEN NOT MATCHED THEN INSERT ({', '.join(cols)}) VALUES ({', '.join('s.' + c for c in cols)})""")
        changed = staged.filter(F.col("merge_key").isNull()).count()
        ctx.metrics["dim_account.incoming"] = acct.count()
        ctx.metrics["dim_account.new_versions"] = changed

        # ---------------- securities and prices
        sec = latest_per_key(bronze("security_master_raw").filter(F.col("load_date") == bd)
                             .withColumn("updated_at", F.col("_load_ts")), "security_id")
        ctx.metrics["dim_security"] = merge_scd1(
            ctx, sec, silver("dim_security"), "security_id",
            ["cusip", "ticker", "security_name", "asset_class", "sector", "currency", "is_tax_exempt", "issuer"])

        px = (bronze("prices_raw").filter(F.col("price_date") <= bd)
              .filter(F.col("price_date") >= F.date_format(F.date_sub(F.lit(bd), 10), "yyyy-MM-dd"))
              .select("security_id", F.col("price_date").cast("date").alias("price_date"),
                      "close_price", "currency", "source", F.lit(ctx.batch_id).alias("_batch_id"))
              .dropDuplicates(["security_id", "price_date"]))
        px.createOrReplaceTempView("px")
        spark.sql(f"""
            MERGE INTO {silver('fact_price')} t USING px s
            ON t.security_id = s.security_id AND t.price_date = s.price_date
            WHEN MATCHED AND t.close_price <> s.close_price THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *""")

        # ---------------- data quality on the dimensions
        dq = Checks(ctx)
        dim = spark.table(target)
        dq.expect_empty("dim_account_one_current_row_per_key", target,
                        dim.filter("is_current").groupBy("account_id").count().filter("count > 1"))
        dq.expect_empty("dim_account_no_overlapping_versions", target,
                        dim.filter(F.col("effective_to") < F.col("effective_from")))
        dq.expect_empty("dim_account_fa_exists", target,
                        dim.filter("is_current").join(spark.table(silver("dim_financial_advisor")), "fa_id", "left_anti"))
        dq.expect_empty("dim_security_cusip_unique", silver("dim_security"),
                        spark.table(silver("dim_security")).groupBy("cusip").count().filter("count > 1"))
        dq.expect_empty("accounts_with_terminated_fa", target,
                        dim.filter("is_current AND status = 'ACTIVE'")
                        .join(spark.table(silver("dim_financial_advisor")).filter("status = 'TERMINATED'"), "fa_id"),
                        severity="WARN", details="book should be reassigned before the FA leaves")
        dq.publish()
        ctx.metrics["rows_out"] = sum(v for k, v in ctx.metrics.items() if isinstance(v, int))
    return 0

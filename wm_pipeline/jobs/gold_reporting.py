"""Run resources/hive/gold_reporting.hql (HiveQL) through Spark SQL for the as-of date.

The HiveQL stays a plain .hql file the reporting team can also run in beeline;
Spark just executes it faster than Hive-on-MapReduce and in the same DAG.
"""
from pyspark.sql import functions as F

from wm_pipeline.context import resource, sql_statements
from wm_pipeline.jobs import make_context


def run(args) -> int:
    ctx = make_context("gold_reporting", args)
    spark, bd = ctx.spark, ctx.business_date
    with ctx.audit():
        spark.sql(f"SET hivevar:as_of_date={bd}")
        for stmt in sql_statements(resource("hive/gold_reporting.hql")):
            spark.sql(stmt)
        for t in ("advisor_book_daily", "client_portfolio_summary"):
            n = spark.table(ctx.table("gold", t)).filter(F.col("as_of_date") == bd).count()
            ctx.metrics[f"{t}.rows"] = n
        ctx.metrics["rows_out"] = ctx.metrics["advisor_book_daily.rows"] + ctx.metrics["client_portfolio_summary.rows"]
        top = (spark.table(ctx.table("gold", "advisor_book_daily"))
               .filter((F.col("as_of_date") == bd) & (F.col("agg_level") == "FA"))
               .orderBy(F.col("aum").desc()).limit(5).collect())  # 5 rows: safe to collect
        ctx.log.info("top advisors by AUM: %s", [(r["fa_name"], str(r["aum"])) for r in top])
    return 0

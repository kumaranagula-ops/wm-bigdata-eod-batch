"""Create databases and tables (idempotent: IF NOT EXISTS everywhere).

Hive tables come from resources/hive/*.hql (the same files work in beeline); Iceberg
silver tables from resources/iceberg/*.sql. Runs first in every DAG run, so a fresh
cluster or a dropped table heals itself.
"""
from wm_pipeline.context import resource, sql_statements
from wm_pipeline.jobs import make_context

FILES = ["hive/01_bronze.hql", "hive/02_gold_audit.hql", "iceberg/01_silver.sql"]


def run(args) -> int:
    ctx = make_context("setup_catalog", args)
    n = 0
    for f in FILES:
        for stmt in sql_statements(resource(f)):
            ctx.spark.sql(stmt)
            n += 1
    ctx.log.info("executed %d DDL statements", n)
    with ctx.audit():
        ctx.metrics["ddl_statements"] = n
    return 0

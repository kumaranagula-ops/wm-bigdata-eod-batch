"""Small data-quality framework: checks -> wm_audit.dq_results -> fail on CRITICAL.

Each check is a DataFrame of offending rows; we only count them, so checks stay cheap.
Severity CRITICAL stops the batch (bad data must not reach the books), WARN is logged
and visible in dq_results for the morning data-ops review.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

from pyspark.sql import DataFrame

from wm_pipeline.context import JobContext, insert_overwrite


@dataclass
class CheckResult:
    check_name: str
    table_name: str
    severity: str
    failed_count: int
    details: str = ""

    @property
    def status(self) -> str:
        return "PASS" if self.failed_count == 0 else ("FAIL" if self.severity == "CRITICAL" else "WARN")


class DataQualityError(RuntimeError):
    pass


class Checks:
    def __init__(self, ctx: JobContext):
        self.ctx = ctx
        self.results: list[CheckResult] = []

    def expect_empty(self, name: str, table: str, offending: DataFrame, severity: str = "CRITICAL",
                     details: str = "") -> int:
        n = offending.count()
        self.results.append(CheckResult(name, table, severity, n, details))
        self.ctx.log.info("DQ %-40s %-8s failed=%d", name, severity, n)
        return n

    def expect_value(self, name: str, table: str, failed_count: int, severity: str = "WARN",
                     details: str = "") -> None:
        self.results.append(CheckResult(name, table, severity, int(failed_count), details))
        self.ctx.log.info("DQ %-40s %-8s failed=%d %s", name, severity, failed_count, details)

    def publish(self) -> None:
        """Write results (idempotent per business_date + job) and raise on CRITICAL."""
        spark = self.ctx.spark
        now = dt.datetime.utcnow()
        rows = [(r.check_name, r.table_name, r.severity, r.failed_count, r.status, r.details, now,
                 self.ctx.business_date, self.ctx.job_name) for r in self.results]
        if rows:
            df = spark.createDataFrame(rows, "check_name STRING, table_name STRING, severity STRING, "
                                             "failed_count BIGINT, status STRING, details STRING, "
                                             "checked_at TIMESTAMP, business_date STRING, job_name STRING")
            insert_overwrite(df, self.ctx.table("audit", "dq_results"))
        failed = [r for r in self.results if r.status == "FAIL"]
        self.ctx.metrics["dq_failed_checks"] = len(failed)
        if failed and self.ctx.cfg["dq"]["fail_on_critical"]:
            raise DataQualityError("; ".join(f"{r.check_name}={r.failed_count}" for r in failed))

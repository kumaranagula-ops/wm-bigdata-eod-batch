"""Shared job plumbing: config, SparkSession, logging and run-control auditing.

Every job gets a JobContext. Wrapping the job body in `with ctx.audit():` writes one
row to wm_audit.run_control at the end (SUCCESS or FAILED, with row counts), which is
the run-history table the support team reads first when a batch breaks.
"""
from __future__ import annotations

import contextlib
import datetime as dt
import json
import logging
import os
import pkgutil
import sys
from dataclasses import dataclass, field
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"


def get_logger(name: str) -> logging.Logger:
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, stream=sys.stdout)
    return logging.getLogger(name)


def load_config(path: str | None = None) -> dict:
    """pipeline.json is shipped with --files, so in YARN cluster mode it sits in the
    driver's working directory. Locally it falls back to conf/pipeline.json."""
    candidates = [path, "pipeline.json", str(Path(__file__).resolve().parents[1] / "conf" / "pipeline.json")]
    for c in candidates:
        if c and Path(c).exists():
            return json.loads(Path(c).read_text())
    raise FileNotFoundError(f"pipeline.json not found in {candidates}")


def resource(name: str) -> str:
    """Read a SQL/HQL file bundled inside the wm_pipeline package (works from the zip)."""
    data = pkgutil.get_data("wm_pipeline", f"resources/{name}")
    if data is None:
        raise FileNotFoundError(name)
    return data.decode("utf-8")


def sql_statements(text: str) -> list[str]:
    """Split a .sql/.hql file into statements; drops full-line comments."""
    lines = [ln for ln in text.splitlines() if not ln.strip().startswith("--")]
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]


def build_spark(app_name: str, extra_conf: dict | None = None) -> SparkSession:
    """Runtime SQL configs only. Static ones (memory, cores, packages) belong to
    spark-submit, because getOrCreate() silently ignores them once a session exists."""
    builder = SparkSession.builder.appName(app_name).enableHiveSupport()
    conf = {
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        # Reruns replace only the partitions they write (idempotent daily loads).
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
        "hive.exec.dynamic.partition": "true",
        "hive.exec.dynamic.partition.mode": "nonstrict",
        "spark.sql.parquet.compression.codec": "snappy",
        # Spark 3 default EXCEPTION throws SparkUpgradeException on a date like 13/45/2026
        # (the old parser might have read it differently). CORRECTED = strict new parser:
        # unparseable -> NULL, and PERMISSIVE mode routes the row to _corrupt_record.
        "spark.sql.legacy.timeParserPolicy": "CORRECTED",
    }
    conf.update(extra_conf or {})
    for k, v in conf.items():
        builder = builder.config(k, v)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


@dataclass
class JobContext:
    job_name: str
    business_date: str
    spark: SparkSession
    cfg: dict
    log: logging.Logger
    metrics: dict = field(default_factory=dict)

    @property
    def batch_id(self) -> str:
        return f"{self.job_name}:{self.business_date}:{self.spark.sparkContext.applicationId}"

    @property
    def bd(self) -> dt.date:
        return dt.date.fromisoformat(self.business_date)

    def db(self, layer: str) -> str:
        return self.cfg["databases"][layer]

    def table(self, layer: str, name: str) -> str:
        return f"{self.db(layer)}.{name}"

    def add_audit_columns(self, df: DataFrame, with_file: bool = True) -> DataFrame:
        cols = {"_load_ts": F.current_timestamp(), "_batch_id": F.lit(self.batch_id)}
        if with_file:
            cols["_source_file"] = F.input_file_name()
        return df.withColumns(cols)

    @contextlib.contextmanager
    def audit(self):
        started = dt.datetime.utcnow()
        status, message = "SUCCESS", ""
        try:
            yield self
        except Exception as exc:  # noqa: BLE001 - re-raised below
            status, message = "FAILED", f"{type(exc).__name__}: {exc}"[:1000]
            raise
        finally:
            try:
                self._write_run_control(started, status, message)
            except Exception as audit_exc:  # never hide the real failure
                self.log.error("could not write run_control: %s", audit_exc)

    def _write_run_control(self, started, status, message):
        m = self.metrics
        row = [(
            self.batch_id, self.job_name, status,
            int(m.get("rows_in", 0)), int(m.get("rows_out", 0)), int(m.get("rows_rejected", 0)),
            started, dt.datetime.utcnow(), message, json.dumps(m, default=str)[:4000],
            self.business_date,
        )]
        cols = ["run_id", "job_name", "status", "rows_in", "rows_out", "rows_rejected",
                "started_at", "ended_at", "message", "metrics_json", "business_date"]
        df = self.spark.createDataFrame(row, cols)
        target = self.table("audit", "run_control")
        df.select(*self.spark.table(target).columns).write.insertInto(target)
        self.log.info("run_control %s %s %s", self.job_name, status, m)


def insert_overwrite(df: DataFrame, table: str) -> None:
    """insertInto matches by POSITION, so always reorder to the table's columns first.
    With partitionOverwriteMode=dynamic only the partitions present in df are replaced."""
    spark = df.sparkSession
    df.select(*spark.table(table).columns).write.insertInto(table, overwrite=True)


def env(name: str, default: str | None = None, required: bool = False) -> str | None:
    value = os.environ.get(name, default)
    if required and not value:
        raise RuntimeError(f"environment variable {name} is required")
    return value

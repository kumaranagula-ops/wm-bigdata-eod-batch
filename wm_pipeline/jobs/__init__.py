"""One module per batch step. Each exposes run(args) -> int."""
from __future__ import annotations

from wm_pipeline.context import JobContext, build_spark, get_logger, load_config


def make_context(job_name: str, args, extra_conf: dict | None = None) -> JobContext:
    cfg = load_config(args.config)
    spark = build_spark(f"wm_{job_name}_{args.business_date}", extra_conf)
    return JobContext(job_name=job_name, business_date=args.business_date, spark=spark,
                      cfg=cfg, log=get_logger(f"wm.{job_name}"))

"""Single spark-submit entry point for every WM batch job.

    spark-submit ... --py-files wm_pipeline.zip --files pipeline.json \
        jobs/run.py --job silver_transactions --business-date 2026-11-09

One entry script keeps the Airflow DAG simple: only --job changes between tasks.
"""
import argparse
import importlib
import sys


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--job", required=True, help="module name in wm_pipeline.jobs")
    ap.add_argument("--business-date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--config", default=None, help="path to pipeline.json")
    ap.add_argument("--option", action="append", default=[], help="job option key=value")
    args = ap.parse_args()
    args.options = dict(o.split("=", 1) for o in args.option)
    module = importlib.import_module(f"wm_pipeline.jobs.{args.job}")
    return module.run(args) or 0


if __name__ == "__main__":
    sys.exit(main())

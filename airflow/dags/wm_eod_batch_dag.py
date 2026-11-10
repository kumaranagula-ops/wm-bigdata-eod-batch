"""
Wealth Management end-of-day (EOD) batch: books and records on Hadoop.

    wait_for_nas_feed ─► land_to_hdfs ─┬─► bronze_nas_files ─────┐
                                       ├─► bronze_trades_stream ─┤
    ensure_catalog ────────────────────┼─► bronze_crm_jdbc ──┐   │
                                       └─► bronze_market ────┴─► silver_reference ─► silver_transactions
                                                                                          │
              ┌───────────────────────────────────────────────────────────────────────────┘
              ▼
    gold_book_of_record ─┬─► gold_reporting (HiveQL) ─► publish_snowflake ─► batch_complete
                         └─► middle_office_recon ───────────────────────────┘

Airflow only orchestrates; every heavy step is a spark-submit to YARN in cluster mode,
so the driver runs on the cluster, not on the Airflow worker.

Run for a date (the DAG is paused on creation; trigger manually for the demo):
    airflow dags trigger wm_eod_batch --conf '{"business_date": "2026-11-09"}'
Without conf it uses {{ ds }}, the logical date of the scheduled run (Mon-Fri 22:00 UTC).

YARN has 4 GB here, enough for one Spark application at a time, so Spark tasks share
the `wm_yarn` pool (1 slot). The graph still shows which steps are independent; on a
real cluster you raise the pool size or point them at a bigger YARN queue.
"""
from __future__ import annotations

import configparser
import datetime as dt
import os
import subprocess
from pathlib import Path

import requests

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.operators.empty import EmptyOperator
from airflow.sensors.python import PythonSensor

WEBHDFS = "http://namenode:9870/webhdfs/v1"
HDFS_USER = "root"
NAS_INBOUND = Path("/opt/airflow/wm_nas/inbound")
MARKET_LOCAL = Path("/opt/airflow/wm_market")
HDFS_LANDING = "/data/wm/landing"

WM = Path("/opt/airflow/wm")
APP = WM / "dist" / "run.py"
PY_FILES = WM / "dist" / "wm_pipeline.zip"
CONFIG = WM / "conf" / "pipeline.json"

SPARK_VENV = "/home/airflow/.venvs/spark-tools"
SPARK_HOME = f"{SPARK_VENV}/lib/python3.11/site-packages/pyspark"

PACKAGES = {
    "mysql": "com.mysql:mysql-connector-j:8.4.0",
    "kafka": "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
    "s3": "org.apache.hadoop:hadoop-aws:3.3.4",
    "snowflake": "net.snowflake:spark-snowflake_2.12:3.0.0",
}
POOL = "wm_yarn"
BUSINESS_DATE = "{{ dag_run.conf.get('business_date') or ds }}"


# ----------------------------------------------------------------------------- helpers
def aws_env() -> dict:
    """Read the default AWS profile mounted at ~/.aws (read-only) for s3a."""
    creds = configparser.ConfigParser()
    creds.read(Path.home() / ".aws" / "credentials")
    if "default" not in creds:
        raise RuntimeError("WM_MARKET_SOURCE=s3 but no [default] profile in ~/.aws/credentials")
    return {"AWS_ACCESS_KEY_ID": creds["default"]["aws_access_key_id"],
            "AWS_SECRET_ACCESS_KEY": creds["default"]["aws_secret_access_key"]}


def spark_submit(job: str, business_date: str, packages: tuple[str, ...] = (),
                 secret_env: dict | None = None, extra_files: tuple[str, ...] = (),
                 options: dict | None = None, executor_memory: str = "768m",
                 extra_conf: dict | None = None) -> None:
    """spark-submit to YARN in cluster mode and stream its output into the task log.

    Secrets go to the YARN containers as environment variables (spark.yarn.appMasterEnv.*
    for the driver, spark.executorEnv.* for executors). Spark redacts conf keys matching
    secret/password/token in the UI; the values never appear in the command line we log.
    """
    secret_env = secret_env or {}
    cmd = [
        f"{SPARK_VENV}/bin/spark-submit",
        "--master", "yarn", "--deploy-mode", "cluster",
        "--properties-file", "/opt/spark-conf/spark-defaults.conf",
        "--name", f"wm_{job}_{business_date}",
        "--conf", "spark.driver.memory=768m",
        "--conf", f"spark.executor.memory={executor_memory}",
        "--conf", "spark.yarn.appMasterEnv.PYSPARK_PYTHON=/usr/bin/python3",
        "--conf", "spark.yarn.maxAppAttempts=1",
        "--conf", "spark.jars.ivy=/tmp/.ivy2",
        "--py-files", str(PY_FILES),
        "--files", ",".join([str(CONFIG), *extra_files]),
    ]
    for k, v in (extra_conf or {}).items():
        cmd += ["--conf", f"{k}={v}"]
    if packages:
        cmd += ["--packages", ",".join(PACKAGES[p] for p in packages)]
    for name in secret_env:
        cmd += ["--conf", f"spark.yarn.appMasterEnv.{name}=${{{name}}}",
                "--conf", f"spark.executorEnv.{name}=${{{name}}}"]
    cmd += [str(APP), "--job", job, "--business-date", business_date]
    for k, v in (options or {}).items():
        cmd += ["--option", f"{k}={v}"]

    printable = " ".join(cmd)
    print(f"+ {printable}")
    # Expand ${VAR} placeholders from env inside a shell so secrets stay out of the log.
    env = {**os.environ, "SPARK_HOME": SPARK_HOME, **secret_env}
    proc = subprocess.Popen(["bash", "-c", printable], env=env, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True)
    for line in proc.stdout:
        print(line.rstrip())
    if proc.wait() != 0:
        raise RuntimeError(f"spark job {job} failed with exit code {proc.returncode}")


def webhdfs(method: str, path: str, op: str, **params) -> requests.Response:
    r = requests.request(method, f"{WEBHDFS}{path}", params={"op": op, "user.name": HDFS_USER, **params},
                         allow_redirects=False, timeout=60)
    if op != "CREATE":
        r.raise_for_status()
    return r


def put_file(local: Path, hdfs_path: str) -> None:
    first = webhdfs("PUT", hdfs_path, "CREATE", overwrite="true")
    if first.status_code != 307:
        raise RuntimeError(f"NameNode did not redirect for {hdfs_path}: {first.status_code} {first.text}")
    with local.open("rb") as body:
        requests.put(first.headers["Location"], data=body, timeout=600).raise_for_status()


default_args = {
    "owner": "wm-data-engineering",
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=2),
    "execution_timeout": dt.timedelta(minutes=30),
}


@dag(
    dag_id="wm_eod_batch",
    description="Wealth management EOD: NAS/MySQL/S3/Kafka -> Hive bronze -> Iceberg silver -> Hive gold -> Snowflake",
    schedule="0 22 * * 1-5",
    start_date=dt.datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    dagrun_timeout=dt.timedelta(hours=3),
    default_args=default_args,
    render_template_as_native_obj=False,
    tags=["wealth-management", "spark", "yarn", "hive", "iceberg", "kafka", "snowflake"],
)
def wm_eod_batch():
    def feed_ready(business_date: str) -> bool:
        marker = NAS_INBOUND / business_date / "_SUCCESS"
        print(f"waiting for {marker}: {'found' if marker.exists() else 'not yet'}")
        return marker.exists()

    # The custodian writes _SUCCESS last; never start on a half-written feed.
    wait_for_nas_feed = PythonSensor(
        task_id="wait_for_nas_feed", python_callable=feed_ready,
        op_kwargs={"business_date": BUSINESS_DATE},
        mode="reschedule", poke_interval=60, timeout=60 * 60 * 2)

    @task
    def land_to_hdfs(business_date: str) -> dict:
        """NAS -> HDFS landing zone (what `hdfs dfs -put` / DistCp would do)."""
        src = NAS_INBOUND / business_date
        dest = f"{HDFS_LANDING}/nas/{business_date}"
        webhdfs("PUT", dest, "MKDIRS")
        files = sorted(p for p in src.iterdir() if p.is_file())
        for f in files:
            put_file(f, f"{dest}/{f.name}")
            print(f"landed {f.name} ({f.stat().st_size:,} bytes) -> {dest}")
        landed = {"nas_files": len(files)}
        if os.environ.get("WM_MARKET_SOURCE", "hdfs").lower() != "s3":
            n = 0
            for f in sorted(MARKET_LOCAL.rglob("*")):
                if f.is_file():
                    rel = f.relative_to(MARKET_LOCAL).as_posix()
                    webhdfs("PUT", f"{HDFS_LANDING}/market/{Path(rel).parent.as_posix()}", "MKDIRS")
                    put_file(f, f"{HDFS_LANDING}/market/{rel}")
                    n += 1
            landed["market_files"] = n
            print(f"market data source = hdfs: landed {n} vendor files")
        return landed

    def spark_task(task_id: str, job: str, **kwargs):
        @task(task_id=task_id, pool=POOL)
        def _run(business_date: str):
            spark_submit(job, business_date, **kwargs)
        return _run(BUSINESS_DATE)

    @task(task_id="bronze_crm_jdbc", pool=POOL)
    def bronze_crm_jdbc(business_date: str):
        spark_submit("bronze_crm_jdbc", business_date, packages=("mysql",),
                     secret_env={"WM_MYSQL_USER": os.environ["WM_MYSQL_USER"],
                                 "WM_MYSQL_PASSWORD": os.environ["WM_MYSQL_PASSWORD"]})

    @task(task_id="bronze_market", pool=POOL)
    def bronze_market(business_date: str):
        env = {k: os.environ.get(k, "") for k in ("WM_MARKET_SOURCE", "WM_S3_BUCKET", "WM_S3_PREFIX")}
        packages: tuple[str, ...] = ()
        conf: dict = {}
        if env["WM_MARKET_SOURCE"].lower() == "s3":
            env.update(aws_env())
            region = os.environ.get("WM_AWS_REGION", "ap-south-1")
            packages = ("s3",)
            conf = {"spark.hadoop.fs.s3a.endpoint": f"s3.{region}.amazonaws.com",
                    "spark.hadoop.fs.s3a.aws.credentials.provider":
                        "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"}
        spark_submit("bronze_market", business_date, packages=packages, secret_env=env, extra_conf=conf)

    @task(task_id="publish_snowflake", pool=POOL)
    def publish_snowflake(business_date: str):
        if os.environ.get("WM_SNOWFLAKE_ENABLED", "false").lower() != "true":
            raise AirflowSkipException("WM_SNOWFLAKE_ENABLED is not true; gold stays in Hive only")
        keys = ("WM_SF_ACCOUNT", "WM_SF_USER", "WM_SF_ROLE", "WM_SF_WAREHOUSE", "WM_SF_DATABASE", "WM_SF_SCHEMA")
        key_file = os.environ.get("WM_SF_PRIVATE_KEY_FILE", "/home/airflow/.snowflake/rsa_key.p8")
        spark_submit("publish_snowflake", business_date, packages=("snowflake",),
                     secret_env={k: os.environ[k] for k in keys},
                     extra_files=(f"{key_file}#sf_key.p8",), options={"key_file": "sf_key.p8"})

    ensure_catalog = spark_task("ensure_catalog", "setup_catalog")
    landed = land_to_hdfs(BUSINESS_DATE)
    b_files = spark_task("bronze_nas_files", "bronze_nas_files")
    b_stream = spark_task("bronze_trades_stream", "bronze_trades_stream", packages=("kafka",))
    b_crm = bronze_crm_jdbc(BUSINESS_DATE)
    b_market = bronze_market(BUSINESS_DATE)
    s_ref = spark_task("silver_reference", "silver_reference")
    s_txn = spark_task("silver_transactions", "silver_transactions")
    g_book = spark_task("gold_book_of_record", "gold_book_of_record", executor_memory="1g")
    g_report = spark_task("gold_reporting", "gold_reporting")
    recon = spark_task("middle_office_recon", "middle_office_recon")
    publish = publish_snowflake(BUSINESS_DATE)
    done = EmptyOperator(task_id="batch_complete", trigger_rule="none_failed")

    wait_for_nas_feed >> landed
    [landed, ensure_catalog] >> b_files
    [landed, ensure_catalog] >> b_market
    ensure_catalog >> [b_crm, b_stream]
    [b_crm, b_market] >> s_ref
    [s_ref, b_files, b_stream] >> s_txn >> g_book
    g_book >> [g_report, recon]
    g_report >> publish
    [publish, recon] >> done


wm_eod_batch()

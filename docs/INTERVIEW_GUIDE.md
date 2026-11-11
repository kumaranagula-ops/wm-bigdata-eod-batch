# Interview Guide: WM EOD Batch on Hadoop

How to explain this project, the wealth-management domain behind it, and where each
job-description bullet and each PySpark concept from your keyword guide shows up.
It's modeled on a typical wealth-management firm's nightly batch, not on any firm's
actual internal design. Say "a typical WM book-of-records pipeline" in the interview.

---

## 1. The 60-second pitch

> "I built an end-of-day books-and-records batch for a wealth-management business on a
> Hadoop stack. Every evening it ingests four kinds of sources: custodian and
> middle-office files from a NAS share, client and account master data from a MySQL CRM
> over JDBC, vendor market data from S3, and intraday trade events from Kafka. Spark on
> YARN lands them in Hive bronze tables, cleans and de-duplicates them into Iceberg
> silver tables with SCD2 history on accounts, and builds gold marts: positions, FIFO tax
> lots, realized and unrealized gains split short-term and long-term, cash on trade-date
> and settle-date basis, and AUM per financial advisor. A middle-office job reconciles our
> positions against the custodian's statement, and the marts are published to Snowflake.
> Airflow orchestrates it, with a file sensor, a run-control table, data-quality gates
> and idempotent reruns."

---

## 2. Wealth-management domain primer

**Who is who**

| Term | Meaning | In the project |
|---|---|---|
| Party | Any legal entity: a person (IND) or organisation (ORG: trust, foundation) | `party` (MySQL), `dim_party` |
| Customer / client / household | A party that owns accounts; a household groups related parties | `client_portfolio_summary` is per party |
| Account | Brokerage, IRA, Roth IRA, Advisory (fee-based), Trust; has a tax status | `account`, `dim_account` (SCD2) |
| Financial Advisor (FA) | Services the client relationship; their "book" is the accounts they cover | `financial_advisor`, `advisor_book_daily` |
| Branch / region | FA hierarchy for management reporting | `branch`, rollups in HiveQL |
| AUM | Assets under management = market value of holdings + cash | `account_valuation_daily.total_value` |

**Front, middle, back office**

- **Front office**: FAs and the order management system (OMS). Orders become trades.
  The Kafka stream here = OMS execution events, intraday, provisional.
- **Middle office**: trade capture checks, P&L, risk, **reconciliation** (books vs custodian),
  break investigation. = `middle_office_recon`.
- **Back office / operations**: settlement, custody, **books and records**, statements,
  tax reporting (1099-B). = the gold layer.

**Trade lifecycle**: order → execution → allocation → confirmation → **settlement**.
US equities settle **T+1** since 28 May 2024 (T+2 before). The generator uses that rule.
Corrections arrive as a new version of the same trade ID; a busted trade arrives as CANCEL.

**Books and records concepts**

- **Position**: quantity of a security held by an account on a date, with cost and market value.
- **Tax lot**: each purchase is a lot with its own date and cost per share. When you sell,
  a **lot relief method** decides which lots are sold: FIFO (default), LIFO, specific ID,
  highest cost. Here FIFO.
- **Cost basis**: what you paid, **including commission**. Sale proceeds are net of commission.
- **Realized gain/loss**: proceeds − cost basis of the lots relieved. Taxable in the year sold.
- **Unrealized gain/loss**: market value − cost basis of lots still held. Not taxable yet.
- **Short-term vs long-term**: held ≤ 1 year = short-term (ordinary income rates);
  > 1 year = long-term (lower rates). The job uses 35% / 15% as a simple estimate.
- **Tax status**: TAXABLE accounts owe tax on gains; IRA (tax-deferred) and Roth (tax-exempt)
  do not, so their realized gains are flagged `is_taxable = false`.
- **Municipal bond interest** is generally federally tax-exempt (`is_tax_exempt` on the security).
- **Trade-date vs settlement-date cash**: TD balance counts a trade the day it executes; SD
  balance only once cash actually moves. Statements and margin use different ones.
- **Corporate actions**: a 2-for-1 **split** doubles quantity and halves price; cost basis is
  unchanged. Cash dividends and interest show up as cash activity.

Things simplified on purpose (say so if asked): wash-sale rule, multi-currency, fractional
shares, lot selection other than FIFO, short selling and margin, fee billing.

---

## 3. Walk through the flow (with files)

| Step | What happens | File |
|---|---|---|
| Sensor | Waits for `_SUCCESS`, which the custodian writes last, so we never read a half-written feed | DAG `wait_for_nas_feed` |
| Landing | NAS files → HDFS `/data/wm/landing/nas/<date>/` over WebHDFS (what `hdfs dfs -put` / DistCp does) | DAG `land_to_hdfs` |
| Catalog | Idempotent DDL for Hive and Iceberg tables | [setup_catalog.py](../wm_pipeline/jobs/setup_catalog.py), [01_bronze.hql](../wm_pipeline/resources/hive/01_bronze.hql) |
| Bronze files | Explicit schemas, PERMISSIVE + `_corrupt_record`, pipe delimiter, custom date format, JSON with nested struct, audit columns | [bronze_nas_files.py](../wm_pipeline/jobs/bronze_nas_files.py) |
| Bronze JDBC | Incremental MySQL extract: watermark per table and date, overlap window, parallel range reads | [bronze_crm_jdbc.py](../wm_pipeline/jobs/bronze_crm_jdbc.py) |
| Bronze S3 | Vendor security master + prices from `s3a://`, partition pruning on `price_date=` | [bronze_market.py](../wm_pipeline/jobs/bronze_market.py) |
| Bronze Kafka | Structured Streaming with `availableNow`, HDFS checkpoint, `foreachBatch` | [bronze_trades_stream.py](../wm_pipeline/jobs/bronze_trades_stream.py) |
| Silver reference | SCD1 MERGE on hash change for branch/FA/party/security; **SCD2** for accounts; PII masking | [silver_reference.py](../wm_pipeline/jobs/silver_reference.py), [scd.py](../wm_pipeline/transforms/scd.py) |
| Silver transactions | Union file + stream, drop exact dupes, validate, quarantine with reasons, latest version per trade, MERGE, row-count reconciliation | [silver_transactions.py](../wm_pipeline/jobs/silver_transactions.py), [trades.py](../wm_pipeline/transforms/trades.py) |
| Gold book of record | Split adjustment, **FIFO lots via window functions**, positions, realized/unrealized, TD/SD cash, account valuation with SCD2 as-of join and pivot | [gold_book_of_record.py](../wm_pipeline/jobs/gold_book_of_record.py), [lots.py](../wm_pipeline/transforms/lots.py) |
| Gold reporting | **HiveQL**: advisor book with branch/region/firm subtotals and DENSE_RANK; client summary with allocation, segment, NTILE decile | [gold_reporting.hql](../wm_pipeline/resources/hive/gold_reporting.hql) |
| Recon | Books vs custodian, full outer join on mapped keys, break types | [middle_office_recon.py](../wm_pipeline/jobs/middle_office_recon.py) |
| Publish | Snowflake connector, key-pair auth, delete-then-append per date | [publish_snowflake.py](../wm_pipeline/jobs/publish_snowflake.py) |
| Audit | Every job writes `wm_audit.run_control`; DQ results per job; breaks | [context.py](../wm_pipeline/context.py), [quality.py](../wm_pipeline/quality.py) |

### How the FIFO trick works (good whiteboard question)

Put each account/security's buys on a running-quantity axis and its sells on another.
FIFO means the n-th share sold is the n-th share bought, so a sell relieves exactly the
buy lots whose intervals overlap it:

```
buys   [0 ---- 100)[100 -- 150)[150 ----------- 300)
sells  [0 ------------ 120)[120 - 160)
sell 1 = 100 from lot 1 + 20 from lot 2;  sell 2 = 30 from lot 2 + 10 from lot 3
open   = lot 3 has 140 left
```

That is a running `sum() over (partition by account, security order by date rows between
unbounded preceding and current row)` and a range join that still has equi-keys, so
Spark hash-partitions on (account, security) instead of doing a cartesian product. No
UDFs and no Python loops; it scales like any other join. Proven in
[tests/test_transforms.py](../tests/test_transforms.py).

### SCD2 with one MERGE (the "staged union")

A MERGE can't close and insert for the same key in one pass. So changed rows are staged
twice: once with `merge_key = account_id` (matches the current row, closes it with
`effective_to = new_from − 1`) and once with `merge_key = NULL` (never matches, inserts the
new version). New accounts appear once and insert. Facts join with
`effective_from <= as_of <= effective_to`, so a position on 11-06 still belongs to the FA
who had the account that day, even after the 11-09 reassignment.

---

## 4. Job description → where it is

| JD bullet | Your answer and evidence |
|---|---|
| 1. Robust ETL pipelines with Hadoop tools | HDFS landing, Hive Metastore, YARN cluster mode, medallion bronze/silver/gold, idempotent partition overwrite, run control |
| 2. Large-scale Spark/Hive jobs | All jobs in `wm_pipeline/jobs`; HiveQL reporting file; Hive external partitioned tables; Iceberg MERGE |
| 3. Workflows in Airflow | `wm_eod_batch`: sensor in reschedule mode, TaskFlow, pool for YARN capacity, retries, `dagrun_timeout`, templated `business_date`, skip when Snowflake is disabled |
| 4. Complex SQL/HiveQL | `gold_reporting.hql` (CTEs, window functions, UNION ALL rollups, NTILE), the SCD2 and fact MERGE statements, the FIFO range join |
| 5. Ingest from Kafka, Sqoop, APIs, files | Kafka Structured Streaming; JDBC incremental = Sqoop replacement; NAS CSV/PSV/JSON; S3 vendor files |
| 6. Data quality, lineage, governance | `quality.py` with CRITICAL/WARN, quarantine table with reasons, row-count reconciliation, audit columns back to the source file, run_control, PII masking (email, last-4 tax id), secrets outside code |
| 7. Monitor, troubleshoot, tune | YARN UI, Spark History Server, `explain("formatted")` in logs, AQE, broadcast joins, caching where reused, the skewed Private Wealth accounts, `make status` |
| 8. Documentation and best practices | This guide, README, docstrings that explain *why*, unit tests, config separated from code, one entry point, package shipped with `--py-files` |

**Sqoop question**: "Sqoop was retired to the Apache Attic in 2021. The same
`--incremental lastmodified --split-by -m 4` pattern is Spark JDBC with a watermark,
`partitionColumn`, bounds and `numPartitions`, which also lets me transform in the same job."

---

## 5. PySpark keyword guide → where it is used

| Module / keyword | Where |
|---|---|
| **M2** driver/executor, YARN, cluster deploy mode, `spark-submit`, `--py-files`, `--packages`, `--conf`, memoryOverhead sizing | DAG `spark_submit()`; one app at a time on 4 GB YARN (driver 768m + 2 executors) |
| **M2** lazy evaluation, actions vs transformations, lineage, cache when reused | `cache()`/`persist(MEMORY_AND_DISK)` on buys/sells/lots; counts only where needed |
| **M4** explicit `StructType`/DDL schemas, DECIMAL money, `header`, `sep`, `dateFormat`, `timestampFormat` | [schemas.py](../wm_pipeline/schemas.py), bronze_nas_files |
| **M4** `mode=PERMISSIVE` + `columnNameOfCorruptRecord`, cache before filtering on it | bronze_nas_files, silver quarantine |
| **M4** `withColumns`, `withColumnsRenamed`, `when/otherwise`, `coalesce`, `isin` + null trap, `dropDuplicates` | silver_transactions, trades.py |
| **M5** temp views + `spark.sql`, CTEs, CASE, window functions, `row_number` with tie-breaker, `rowsBetween`, `dense_rank`, `ntile`, `max_by` | trades.py (`latest_trade_versions`), lots.py, gold_book_of_record (cash running total), gold_reporting.hql |
| **M5** dates: `datediff`, `date_sub`, `to_date`, `year`, session time zone UTC | lots.py (holding days), context.py |
| **M5** complex types: nested struct → flatten `terms.ratio_new` | silver_transactions (corporate actions) |
| **M6** join types: inner, left, full outer (recon), left_semi, **left_anti** (orphans), broadcast hints, range join with equi-keys | trades.py, middle_office_recon, lots.py |
| **M6** aggregations, **pivot with explicit values**, `unionByName(allowMissingColumns)` | gold_book_of_record (asset-class pivot), silver_transactions |
| **M6** skew: 25 heavy trust accounts; AQE skew join; broadcast small side; salting as the fallback | data generator + gold job; discuss in tuning answer |
| **M7** CSV/JSON/Parquet, external vs managed tables, `partitionBy`, dynamic partition overwrite, `insertInto` column order trap | context.py `insert_overwrite`, Hive DDL |
| **M7** JDBC `partitionColumn/lowerBound/upperBound/numPartitions`, `fetchsize`, subquery in `dbtable` | bronze_crm_jdbc |
| **M7** `s3a://`, credentials provider chain, partition pruning | bronze_market |
| **M7** incremental: watermark + overlap, idempotent reruns, MERGE upserts | bronze_crm_jdbc, silver MERGEs |
| **M8** Spark UI / History Server, `explain("formatted")`, PushedFilters/PartitionFilters | positions plan in driver log; YARN UI 8088, history 19888 |
| **M9** AQE coalesce/skew, `autoBroadcastJoinThreshold`, partition sizing | context.py `build_spark` |
| **M11** medallion, data contracts (explicit schemas), SCD1/SCD2, CDC dedup by version, soft deletes (CANCEL), late data, quarantine, row-count reconciliation, sha2 row hash, deterministic surrogate key (`xxhash64`) | silver jobs, scd.py, quality.py |
| **M11** table formats: Iceberg for silver (MERGE), Hive Parquet for gold | 01_silver.sql vs 02_gold_audit.hql |
| **M12** modules not notebooks, config file, `--business-date` parameter, logging, secrets from env/key files, pytest with session fixture, small edge-case fixtures | jobs/run.py, conf/pipeline.json, tests/ |
| **M12** Airflow: Spark submit from Airflow, `{{ ds }}` templating, retries, idempotency, backfill by date, sensor/SLA | DAG |

Spark version note: the cluster runs **Spark 3.5.3**, while your guide describes Spark 4
defaults. The biggest difference: **ANSI mode is off by default in 3.5**, so a bad cast
returns NULL instead of failing. That is why validation here checks nulls explicitly
after parsing.

---

## 6. Design decisions you should be able to defend

| Decision | Why | Alternative and when you'd pick it |
|---|---|---|
| Iceberg for silver, Hive Parquet for gold | Silver needs ACID MERGE (upserts, SCD2, corrections). Gold is snapshot-per-date, so plain partition overwrite is enough, and Hive/beeline/BI can read it | Delta Lake on Databricks; Hudi for very high-frequency upserts |
| Recompute gold for the date from silver | Simple, idempotent, any date can be backfilled | At real scale: incremental positions = yesterday's positions + today's trades, with periodic full rebuilds |
| Kafka with `availableNow` instead of always-on streaming | The business process is EOD; this gives exactly-once offsets with a schedulable task | Continuous stream for intraday positions/risk dashboards |
| File is book of record, stream is provisional | Custodian/middle office confirms trades; OMS events can be corrected | Event-sourced books if the OMS is the golden source |
| JDBC watermark per business date | A rerun of a date re-extracts the same window (idempotent), and the overlap catches late commits | CDC (Debezium → Kafka) when the source changes too often for polling |
| Airflow pool with 1 slot | 4 GB YARN fits one app; the DAG still shows the parallelism | Bigger cluster: more slots, YARN queues per team |
| Quarantine instead of failing | Ops can fix and replay bad rows; the batch fails only when the reject rate is abnormal (threshold) | Fail-fast for regulatory feeds with strict contracts |
| Recon breaks are WARN, not failures | Breaks are business exceptions to investigate, not pipeline errors; only an abnormal break rate fails | Hard stop before client statements are produced |

---

## 7. Tuning stories (use them for "tell me about a performance problem")

Each story follows **situation → what I saw → what I changed → how I verified**. Tell one,
keep the other two for follow-ups.

### Tuning story 1: skewed accounts in the FIFO tax-lot job

- **Situation**: `gold_book_of_record` builds FIFO tax lots for every account and
  security. The Private Wealth team's 25 trust accounts trade about 15x more than
  everyone else ([generate_data.py](../scripts/generate_data.py) plants this on purpose).
- **What to look for**: the joins on `account_id` put those 25 accounts into a few
  shuffle partitions. In the Spark UI Stages tab, one or two tasks run far longer than
  the median (the classic straggler pattern), and the whole stage waits for them.
  `groupBy("account_id").count()` ordered by count confirms the hot keys.
- **What I changed** ([gold_book_of_record.py](../wm_pipeline/jobs/gold_book_of_record.py)):
  1. **Broadcast every small dimension**: securities, FAs, branches, prices and splits
     (`F.broadcast(...)`). The big trades side is never shuffled to join them.
  2. **AQE skew join on** (`spark.sql.adaptive.skewJoin.enabled` in
     [context.py](../wm_pipeline/context.py)): Spark splits an oversized join partition
     into smaller tasks at runtime.
  3. **Select only needed columns and filter (`trade_date <= as_of`, not CANCEL) before
     the shuffle**, so less data moves.
  4. **No Python UDFs**: FIFO matching is done with window functions and a range join
     ([lots.py](../wm_pipeline/transforms/lots.py)), so everything stays in the JVM.
- **How I verified**: `positions.explain("formatted")` is printed to the driver log
  (`make yarn-logs JOB=gold_book_of_record`). It shows `BroadcastHashJoin` for the
  dimensions instead of `SortMergeJoin`. I compared stage times before and after, one
  change at a time.
- **Fallback if AQE isn't enough** (e.g. skew inside an aggregation, not a join): salt
  the hot keys: add `salt = rand() * N`, aggregate on `(account_id, salt)`, then
  aggregate again on `account_id`.

### Tuning story 2: making the batch fit a 4 GB YARN cluster

- **Situation**: the cluster has one NodeManager with 4 GB for YARN. The DAG has four
  bronze jobs that could run in parallel, and each is a separate Spark application.
- **The problem**: if several `spark-submit`s start at once, each gets an
  ApplicationMaster container and then **waits for executors** that don't fit.
  Applications sit in ACCEPTED or RUNNING with no executors in the YARN UI, and tasks
  run until they time out. That's a resource deadlock: every app holds part of the
  memory, and none can finish.
- **What I changed**:
  1. **Airflow pool `wm_yarn` with 1 slot**: every Spark task uses the pool, so only one
     application runs at a time. The DAG graph still shows which steps are independent.
  2. **Right-sized each application** in `spark_submit()`
     ([wm_eod_batch_dag.py](../airflow/dags/wm_eod_batch_dag.py)): driver 768m and
     executors 768m with overhead counted, and only the heavy `gold_book_of_record` job
     gets `executor_memory="1g"`.
  3. **Persist only what is reused**: buys and sells are `persist(MEMORY_AND_DISK)`
     because three outputs read them (lots, realized gains, positions), then
     `unpersist()` at the end. `MEMORY_AND_DISK` spills instead of failing when memory
     is tight.
- **How I verified**: all three business dates ran green end to end on YARN, with no
  applications stuck in ACCEPTED.
- **On a real cluster**: raise the pool size or point teams at separate YARN queues
  (capacity scheduler), and size executors at 4–5 cores with about 10% `memoryOverhead`.

### Tuning story 3: CRM extract from MySQL, from one slow read to parallel incremental reads

- **Situation**: `bronze_crm_jdbc` pulls party, account, FA and branch tables from MySQL
  every night.
- **What I saw**: a plain `spark.read.jdbc(url, "account")` reads through **one
  connection in one task**, whatever the cluster size, and pulls the whole table every
  night. With the driver's default `fetchsize`, it also makes many small round trips.
- **What I changed** ([bronze_crm_jdbc.py](../wm_pipeline/jobs/bronze_crm_jdbc.py)):
  1. **Incremental extract**: a subquery as `dbtable` with
     `WHERE updated_at > <last high watermark − 60 min>`, so only changed rows are read
     (the Sqoop `--incremental lastmodified` pattern).
  2. **Parallel read**: a bounds query (`SELECT min(id), max(id) ... WHERE <same filter>`)
     feeds `partitionColumn`, `lowerBound`, `upperBound` and `numPartitions=4`, so Spark
     opens 4 connections, each reading one id range.
  3. **Bigger `fetchsize`** from config to cut round trips.
- **How I verified**: the job logs the window, row count and number of partitions for
  each table (`ctx.metrics`). After the initial load, a day's run pulls only rows
  changed since the previous date's watermark (on day 2: the FA move, the new client
  and the risk-profile updates), not the full tables.
- **Caution I can mention**: keep `numPartitions` small enough not to overload the
  source database; a production OLTP DB has a connection limit.

---

## 8. Failure and rerun scenarios we actually ran

### How every layer is rerun-safe (the mechanisms behind all the scenarios)

The rule: **every write is either a partition overwrite keyed by the business date, or a
keyed MERGE that changes a row only when the incoming data is newer or different. Gold
is recomputed, never incremented.** So any date can be rerun, from any failed task, and
gives the same result.

| # | Layer | Mechanism | Why a rerun is safe |
|---|---|---|---|
| 1 | Bronze (files, market, CRM) | Dynamic partition overwrite | Replaces only that date's partition; other dates untouched |
| 2 | Bronze CRM (JDBC) | Watermark per table **and** per business date | A rerun re-reads the same window; the watermark moves only after the write |
| 3 | Bronze Kafka | Structured Streaming checkpoint | Resumes from the last committed offset |
| 4 | Silver trades | `dropDuplicates` → `row_number` → version-guarded MERGE | Same version again = no change; only a newer version updates |
| 5 | Silver dimensions | Hash-guarded SCD1 MERGE, staged-union SCD2 MERGE, deterministic surrogate key | Same hash = no new version, same key every run |
| 6 | Gold | Recompute from silver + partition overwrite | Not "yesterday + today", so nothing is added twice |
| 7 | Snowflake | `DELETE` preaction for the date, then append | The date is replaced, never duplicated |

#### 1. Bronze: dynamic partition overwrite

[context.py](../wm_pipeline/context.py), set once in `build_spark()` and used by every bronze
and gold write:

```python
"spark.sql.sources.partitionOverwriteMode": "dynamic",
"hive.exec.dynamic.partition": "true",
"hive.exec.dynamic.partition.mode": "nonstrict",

def insert_overwrite(df: DataFrame, table: str) -> None:
    """insertInto matches by POSITION, so always reorder to the table's columns first.
    With partitionOverwriteMode=dynamic only the partitions present in df are replaced."""
    spark = df.sparkSession
    df.select(*spark.table(table).columns).write.insertInto(table, overwrite=True)
```

```python
out = df.withColumn("business_date", F.lit(bd))      # bronze_nas_files.py
insert_overwrite(out, ctx.table("bronze", table))
```

- `overwrite=True` with **dynamic** mode replaces only the partitions present in the
  DataFrame (`business_date=2026-11-09`). The default **static** mode would wipe the
  whole table.
- `select(*table.columns)` protects against `insertInto` matching columns by position.

#### 2. Bronze JDBC: watermark per table and per business date

[bronze_crm_jdbc.py](../wm_pipeline/jobs/bronze_crm_jdbc.py):

```python
# Low watermark = the previous DATE's high watermark (strictly < bd), not my own
prev = spark.sql(f"""
    SELECT max(high_watermark) AS hw FROM {wm_table}
    WHERE source_table = '{table}' AND business_date < '{bd}'""").first()["hw"]
low = (prev - dt.timedelta(minutes=jcfg["overlap_minutes"])) if prev else EPOCH
where = f"updated_at > '{low:%Y-%m-%d %H:%M:%S}'"

df = spark.read.jdbc(jcfg["url"], f"(SELECT * FROM {table} WHERE {where}) src",
                     properties=props, **reader_opts)
out = ctx.add_audit_columns(df, with_file=False).withColumn("extract_date", F.lit(bd))
insert_overwrite(out, ctx.table("bronze", f"crm_{table}_raw"))
high = out.agg(F.max("updated_at")).first()[0] or prev or low

# Record the window only AFTER the data landed
spark.sql(f"""
    MERGE INTO {wm_table} t
    USING (SELECT '{table}' AS source_table, '{bd}' AS business_date, ...) s
    ON t.source_table = s.source_table AND t.business_date = s.business_date
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *""")
```

- `business_date < '{bd}'`: a rerun of 11-09 reads the watermark saved by 11-06, not its
  own earlier attempt, so it pulls exactly the same window.
- The watermark table is MERGEd on `(source_table, business_date)`, so it never gets
  duplicate rows.
- The watermark is saved **after** the write, so a job that dies mid-write loses nothing.
- The 60-minute overlap catches late commits; the duplicates it creates are removed in
  silver.

#### 3. Bronze Kafka: checkpoint

[bronze_trades_stream.py](../wm_pipeline/jobs/bronze_trades_stream.py):

```python
checkpoint = f"{ctx.cfg['hdfs']['checkpoints']}/trades_stream_raw"
query = (events.writeStream
         .foreachBatch(write_batch)
         .option("checkpointLocation", checkpoint)
         .trigger(availableNow=True)
         .start())
```

- Consumed offsets are stored in the HDFS checkpoint, so a rerun or retry starts after the
  last committed offset.
- Kafka is at-least-once anyway, so silver (mechanism 4) removes anything replayed twice.

#### 4. Silver trades: unique source, then version-aware MERGE

Step A, remove exact copies ([silver_transactions.py](../wm_pipeline/jobs/silver_transactions.py)):

```python
deduped = incoming.dropDuplicates(TRADE_COLUMNS + ["record_source"])
```

Step B, one row per `trade_id`, chosen deterministically ([trades.py](../wm_pipeline/transforms/trades.py)):

```python
def latest_trade_versions(trades: DataFrame) -> DataFrame:
    priority = F.create_map(...)            # FILE -> 1, STREAM -> 2
    w = Window.partitionBy("trade_id").orderBy(
        F.col("version").desc(),                         # highest version first
        priority[F.col("record_source")].asc(),          # custodian file beats stream
        F.col("exec_ts").desc())                         # final tie breaker
    return (trades.withColumn("_rn", F.row_number().over(w))
            .filter("_rn = 1").drop("_rn"))
```

Step C, upsert into Iceberg:

```sql
MERGE INTO lake.wm_silver.fact_trade t
USING trades_src s
ON t.trade_id = s.trade_id
WHEN MATCHED AND (s.version > t.version
                  OR (s.version = t.version AND s.record_source = 'FILE' AND t.record_source = 'STREAM'))
    THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

Quarantine replaces only that date's partition:

```python
all_q.select(...).writeTo("lake.wm_silver.quarantine").overwritePartitions()
```

- A MERGE fails if two source rows match one target row. Step B guarantees one row per
  `trade_id`.
- On a rerun the same trade arrives with the **same version**, so the matched condition is
  false and nothing changes. Only a real correction or cancel (a newer version) updates.
- The ordering is fully deterministic. A `row_number()` with ties could pick a different
  winner on a retry.

#### 5. Silver dimensions: hash-guarded SCD1, staged-union SCD2

SCD1 (branch, advisor, party, security) in [silver_reference.py](../wm_pipeline/jobs/silver_reference.py):

```python
def merge_scd1(ctx, source_df, target, key, hash_cols):
    src = source_df.withColumn("_row_hash", row_hash(*hash_cols))
    ...
    MERGE INTO {target} t USING src s ON t.{key} = s.{key}
    WHEN MATCHED AND t._row_hash <> s._row_hash THEN UPDATE SET *     -- only real changes
    WHEN NOT MATCHED THEN INSERT *
```

Row hash in [scd.py](../wm_pipeline/transforms/scd.py):

```python
def row_hash(*cols):
    # fixed column order + explicit null marker: NULL and '' never hash the same
    return F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in cols]), 256)
```

SCD2 on accounts: stage first, then one MERGE:

```python
def stage_scd2(source, current, key):
    changed = (source.alias("s")
               .join(current.select(key, F.col("_row_hash").alias("_cur_hash")), key, "inner")
               .filter(F.col("_row_hash") != F.col("_cur_hash")))
    return (source.withColumn("merge_key", F.col(key))                       # closes old / inserts brand-new
            .unionByName(changed.withColumn("merge_key", F.lit(None)...)))   # inserts the new version
```

```sql
MERGE INTO lake.wm_silver.dim_account t
USING staged s
ON t.account_id = s.merge_key AND t.is_current = true
WHEN MATCHED AND t._row_hash <> s._row_hash THEN
    UPDATE SET t.is_current = false, t.effective_to = date_sub(s.effective_from, 1)
WHEN NOT MATCHED THEN INSERT (...) VALUES (...)
```

```python
"account_sk": F.xxhash64("account_id", "effective_from")    # deterministic surrogate key
```

- On a rerun the incoming hash equals the current hash, so `changed` is empty, no new
  version is staged, and nothing is closed or inserted.
- `xxhash64(account_id, effective_from)` gives the same surrogate key every run. A
  sequence or `monotonically_increasing_id()` would hand out new keys on each rerun.

#### 6. Gold: recompute from silver, never increment

[gold_book_of_record.py](../wm_pipeline/jobs/gold_book_of_record.py):

```python
trades = (silver("fact_trade")
          .filter((F.col("trade_date") <= as_of) & (F.col("trade_status") != "CANCEL")) ...)
# ... FIFO lots -> positions -> realized gains -> cash -> valuation ...
insert_overwrite(lots,      gold("tax_lots_open"))
insert_overwrite(positions, gold("positions_daily"))
insert_overwrite(realized,  gold("realized_gains"))
insert_overwrite(cash,      gold("cash_balances_daily"))
insert_overwrite(valuation, gold("account_valuation_daily"))
```

- Positions are **not** "yesterday's positions + today's trades" (a rerun would add today's
  trades twice). They are rebuilt from all deduplicated silver trades up to the as-of
  date, and that date's partition is overwritten. Same input, same output.

#### 7. Snowflake: delete the date, then append

[publish_snowflake.py](../wm_pipeline/jobs/publish_snowflake.py):

```python
.option("preactions", f"DELETE FROM {sf_table} WHERE AS_OF_DATE = '{bd}'")
.mode("append")
```

- The connector runs the `DELETE` in the same session before loading, so a republish
  replaces the date instead of duplicating it.

**Proof**: 2026-11-06 ran twice and 2026-11-09 three times, with identical results
(21,275 trades = 21,275 distinct IDs, bronze 21,284 rows not doubled, 11,515 positions,
the same 5 recon breaks, Snowflake counts equal to Hive).

### Rerun 1: the same business date ran twice (idempotency proven by accident)

- **What happened**: when I unpaused the DAG, Airflow created the **scheduled** run for
  2026-11-06 automatically, on top of the manual run I had already done. The whole
  batch ran twice for the same date.
- **Result**: identical numbers: 21,275 trades = 21,275 distinct trade IDs, bronze
  stayed at 21,284 rows (not doubled), 11,515 positions, the same 5 recon breaks.
  Later 2026-11-09 also ran three times with the same results.
- **Why it was safe**: bronze and gold use dynamic partition overwrite for that date,
  silver uses a keyed, version-guarded MERGE, the JDBC watermark reads the previous
  date's window, and Kafka resumes from its checkpoint.
- **Lesson**: `catchup=False` still creates the latest scheduled run when you unpause.
  Expect it, and design every job so a rerun is harmless.

### Rerun 2: a task killed mid-run, recovered by retry

- **What happened**: I restarted the Airflow scheduler (to pick up a fixed package) while
  `bronze_trades_stream` was running. The task process was killed.
- **Recovery**: the task has `retries=1` with a 2-minute delay. The retry started on its
  own and succeeded. No one cleared anything.
- **Why no duplicates or lost events**: Kafka offsets are committed in the HDFS
  checkpoint only after a micro-batch is written, so the retry restarted from the last
  committed offset. Anything replayed twice is removed by silver's `dropDuplicates` and
  `row_number` on `trade_id`.

### Rerun 3: fix a code bug, then resume from the failed task (not from the start)

- **What happened**: `silver_transactions` failed on YARN with an `AnalysisException`
  ("Invalid call to dataType on unresolved object") from `F.nullif(...)`. Bronze had
  already succeeded.
- **Recovery steps**:
  1. Airflow log → YARN application ID → `make yarn-logs JOB=silver_transactions`
     for the Python stack trace (in cluster mode the driver log is in YARN, not Airflow).
  2. Fixed the code: `F.when(F.col("security_id") != "", F.col("security_id"))`.
  3. `make build` to redeploy the zip that `spark-submit --py-files` ships.
  4. Cleared `silver_transactions` **with Downstream** ticked. Bronze was not rerun.
- **Result**: silver, gold, recon and publish ran green; `run_control` showed SUCCESS.

Other recovery patterns (not hit, but ready):
- **Backfill**: trigger with `{"business_date": "..."}` for each date **in order**,
  because each date's JDBC window starts from the previous date's watermark.
- **Late correction**: a v2 of an old trade arrives later; the MERGE updates it and
  the next gold run reflects it, because gold recomputes from silver.
- **Source missing**: the sensor times out after 2 hours; deliver the file, clear the
  sensor with Downstream.
- **Where to look first**: `make status` (run_control + failed DQ), the Airflow task
  log, then the YARN application log and Spark History Server.

---

## 9. Major pipeline and data issues we hit (and how we fixed them)

### Issue 1: one invalid date failed the whole cash load

- **What we saw**: the cash activity file (PSV, `MM/dd/yyyy` dates) had one row with
  `13/45/2026`. Instead of that one value becoming NULL, the bronze job failed with
  `SparkUpgradeException` ("Fail to parse ... in the new parser"). One bad row in about
  71k stopped the batch.
- **Root cause**: Spark 3's date parser is stricter than Spark 2's. When a value parses
  differently between the old and new parser, Spark raises an error by default
  instead of guessing.
- **Fix**: `spark.sql.legacy.timeParserPolicy=CORRECTED` (use the new parser, invalid
  dates become NULL), plus an explicit rule in silver: a NULL `txn_date` goes to
  `silver.quarantine` with reason `MISSING_TXN_DATE`
  ([silver_transactions.py](../wm_pipeline/jobs/silver_transactions.py)).
- **Lesson**: one bad record should be quarantined with a reason, not crash the batch
  and not be silently dropped. The batch fails only when the reject rate is abnormal.

### Issue 2: duplicate and conflicting trades from Kafka and the custodian file

- **What we saw**: the same trades arrived from two sources: the OMS Kafka stream during
  the day and the custodian's end-of-day file. Kafka also **redelivered** events
  (at-least-once): 306 events, only 304 distinct. Some trades also had a corrected
  version (v2) or a same-day cancel.
- **Risk**: a plain append would double-count holdings and cash for those trades, and
  positions would break against the custodian.
- **Fix** ([trades.py](../wm_pipeline/transforms/trades.py),
  [silver_transactions.py](../wm_pipeline/jobs/silver_transactions.py)):
  1. `dropDuplicates` on all trade columns plus source removes exact redeliveries.
  2. `row_number()` per `trade_id`, ordered by `version DESC`, then source priority
     (FILE beats STREAM, because the custodian file is the book of record), then
     `exec_ts`: exactly one row per trade.
  3. A MERGE that updates only when the incoming version is newer (or the same version
     from the file replacing the stream). CANCEL is a soft delete: kept in silver,
     excluded from positions.
- **Verified**: trade count = distinct trade IDs on every date, and the silver row count
  reconciles: in = merged + rejected + duplicates + superseded versions.

### Issue 3: the deployed code disappeared from Airflow after a rebuild (setup issue)

- **What we saw**: after `make build`, Spark tasks started failing because the
  `wm_pipeline` package and job files they ship with `--py-files` were not there. On
  the Mac the files existed, but inside the Airflow container `dist/` was **empty**.
- **Root cause**: the build ran `rm -rf dist` and recreated it. The container's bind
  mount still pointed at the **old, deleted** directory, so the container saw an
  empty folder.
- **Fix**: the build now writes the zip to a temporary file and `mv`s it into the
  existing `dist/` folder (the directory is never deleted). I restarted the scheduler
  once to pick up the fixed mount.
- **Lesson**: deployment is part of the pipeline. In production this is solved by
  building a versioned artifact (wheel or zip) in CI and deploying it to a fixed
  location, never editing files in place on the scheduler.

Other data problems the pipeline handles (good for follow-up questions):
- **2-for-1 stock split on SEC00001 (2026-11-10)**: lots are adjusted with a cumulative
  split factor (`exp(sum(log(ratio)))`), so quantity doubles, price halves, cost basis
  stays the same, and recon still matches the custodian.
- **FA departure**: FA 9040 left and 59 accounts moved to FA 9012. SCD2 on accounts keeps
  history correct: 11-06 AUM stays with FA 9040, from 11-09 it moves to FA 9012.
- **Orphan and malformed records**: trades with unknown accounts or securities
  (`left_anti` join), malformed CSV and JSON lines (`_corrupt_record`), all go to
  quarantine with a reason.
- **Custodian breaks**: 3 quantity breaks, 1 missing, 1 extra per day are reported by
  the full outer join in `middle_office_recon` as WARN, for operations to investigate.

---

## 10. Likely questions, short answers

- **Client vs cluster mode?** Cluster in production: the driver runs on YARN and survives
  the Airflow worker restarting. Client for interactive debugging.
- **Why not `collect()`?** Driver OOM. Only small results are collected here (top 5 FAs).
- **`repartition` vs `coalesce`?** Repartition does a full shuffle and can increase or
  rebalance by key. Coalesce merges partitions without a shuffle and can only decrease.
- **How do you avoid duplicates on rerun?** Partition overwrite (dynamic), keyed MERGE,
  per-date watermarks, streaming checkpoints. Never blind append for facts.
- **Managed vs external tables?** Bronze is external: dropping the table keeps the raw files.
- **Why is `insertInto` dangerous?** It matches by position. `insert_overwrite()` always
  reorders to the table's columns first.
- **How do you handle schema changes from the CRM?** Explicit schemas in bronze catch
  drift. Iceberg supports adding columns in silver. A new column needs a deliberate mapping.
- **Small files?** One partition per date, AQE coalescing, `repartition` before writes at
  scale, Iceberg `rewrite_data_files` for compaction.
- **How do you secure credentials?** Env vars and key files at runtime, never in code or
  logs. Spark redacts secret-like keys in the UI. In production: Vault or AWS Secrets
  Manager, IAM roles for S3, key-pair auth for Snowflake.
- **What would you change for production?** CDC instead of polling, incremental gold,
  Great Expectations or Deequ suites, OpenLineage for lineage, alerting on run_control,
  CI that runs the tests and builds the wheel, separate dev/QA/prod configs.

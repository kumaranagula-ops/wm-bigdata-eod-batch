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

## 7. Tuning story (use it for "tell me about a performance problem")

1. **Symptom**: the gold job's lots stage had one task far slower than the median.
2. **Diagnosis**: in the Stages tab, the max task duration was far above the median, and
   `groupBy(account_id).count()` showed the Private Wealth team's 25 trust accounts carried
   a large share of the trades. That's key skew.
3. **Fixes, in order**: broadcast the small dimensions (securities, FAs, prices, splits) so the
   big side is never shuffled for them; keep **AQE skew join** on so oversized join partitions
   are split; filter and select columns before the shuffle; persist buys/sells because three
   outputs reuse them. If AQE can't help (skew in an aggregation), **salt** the hot keys:
   aggregate on (key, salt), then on key.
4. **Verify**: read `explain("formatted")` (BroadcastHashJoin instead of SortMergeJoin,
   PushedFilters on the partition column), compare stage times before and after, one change
   at a time.

Scale-up talking points: target ~128 MB partitions, size executors at 4–5 cores, raise
`memoryOverhead` for PySpark, compact small files, partition facts by month, bucket or
Z-order by account for repeated joins.

---

## 8. Failure and rerun scenarios

- **Rerun the same date**: bronze and gold overwrite their partition; silver MERGEs are
  keyed (a trade is only replaced by a newer version); JDBC re-reads the same window;
  Kafka resumes from the checkpoint. Running 2026-11-06 twice gives identical results.
- **Backfill**: trigger with `{"business_date": "..."}` for each date in order.
- **Late correction**: a v2 of an old trade arrives in a later file. The MERGE updates the
  trade, and the next gold run reflects it, because gold recomputes from silver.
- **Bad file**: the reject-rate gate fails the silver task. Look at `silver.quarantine`, fix
  upstream, rerun the date.
- **Source missing**: the sensor times out after 2 hours (reschedule mode frees the worker
  slot while waiting).
- **Where to look first**: `make status` (run_control + failed DQ), then the Airflow task log
  (full spark-submit output), then the YARN application logs and Spark History Server.

---

## 9. Likely questions, short answers

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

-- SILVER: cleaned, typed, de-duplicated, conformed. Apache Iceberg tables in the
-- "lake" catalog (backed by the same Hive Metastore), because silver needs ACID
-- MERGE for upserts, SCD2 history and late corrections. Spark SQL only (not HiveQL).

CREATE NAMESPACE IF NOT EXISTS lake.wm_silver;
CREATE NAMESPACE IF NOT EXISTS lake.wm_audit;

CREATE TABLE IF NOT EXISTS lake.wm_silver.dim_branch (
    branch_id INT, branch_name STRING, city STRING, state STRING, region STRING,
    updated_at TIMESTAMP, _row_hash STRING, _batch_id STRING
) USING iceberg TBLPROPERTIES ('format-version' = '2');

CREATE TABLE IF NOT EXISTS lake.wm_silver.dim_financial_advisor (
    fa_id INT, fa_code STRING, fa_name STRING, branch_id INT, title STRING,
    hire_date DATE, status STRING, updated_at TIMESTAMP, _row_hash STRING, _batch_id STRING
) USING iceberg TBLPROPERTIES ('format-version' = '2');

-- SCD1 with PII minimised: name kept for servicing, e-mail masked, only last 4 of tax id.
CREATE TABLE IF NOT EXISTS lake.wm_silver.dim_party (
    party_id BIGINT, party_type STRING, party_name STRING, date_of_birth DATE,
    tax_id_last4 STRING, risk_profile STRING, kyc_status STRING, country STRING,
    state STRING, email_masked STRING, updated_at TIMESTAMP, _row_hash STRING, _batch_id STRING
) USING iceberg TBLPROPERTIES ('format-version' = '2');

-- SCD2: one row per version of an account. Facts join on the version valid at the
-- business date, so a position on 2026-11-06 still rolls up to the FA it had then.
CREATE TABLE IF NOT EXISTS lake.wm_silver.dim_account (
    account_sk BIGINT, account_id BIGINT, account_number STRING, party_id BIGINT,
    account_type STRING, tax_status STRING, fa_id INT, open_date DATE, close_date DATE,
    status STRING, base_currency STRING, cost_basis_method STRING,
    effective_from DATE, effective_to DATE, is_current BOOLEAN,
    _row_hash STRING, _batch_id STRING
) USING iceberg TBLPROPERTIES ('format-version' = '2');

CREATE TABLE IF NOT EXISTS lake.wm_silver.dim_security (
    security_id STRING, cusip STRING, ticker STRING, security_name STRING,
    asset_class STRING, sector STRING, currency STRING, is_tax_exempt BOOLEAN,
    issuer STRING, _row_hash STRING, _batch_id STRING
) USING iceberg TBLPROPERTIES ('format-version' = '2');

CREATE TABLE IF NOT EXISTS lake.wm_silver.fact_price (
    security_id STRING, price_date DATE, close_price DECIMAL(18,6), currency STRING,
    source STRING, _batch_id STRING
) USING iceberg PARTITIONED BY (months(price_date)) TBLPROPERTIES ('format-version' = '2');

-- Latest version of every trade. Cancels are kept (soft delete) with trade_status CANCEL.
CREATE TABLE IF NOT EXISTS lake.wm_silver.fact_trade (
    trade_id STRING, account_id BIGINT, security_id STRING, trade_date DATE,
    settle_date DATE, side STRING, quantity DECIMAL(18,4), price DECIMAL(18,6),
    commission DECIMAL(18,2), net_amount DECIMAL(18,2), trade_status STRING,
    version INT, source_system STRING, exec_ts TIMESTAMP, record_source STRING,
    business_date STRING, _batch_id STRING
) USING iceberg PARTITIONED BY (months(trade_date)) TBLPROPERTIES ('format-version' = '2');

CREATE TABLE IF NOT EXISTS lake.wm_silver.fact_cash_activity (
    txn_id STRING, account_id BIGINT, txn_date DATE, txn_type STRING,
    amount DECIMAL(18,2), currency STRING, security_id STRING, description STRING,
    business_date STRING, _batch_id STRING
) USING iceberg PARTITIONED BY (months(txn_date)) TBLPROPERTIES ('format-version' = '2');

CREATE TABLE IF NOT EXISTS lake.wm_silver.ref_corporate_action (
    ca_id STRING, security_id STRING, event_type STRING, ex_date DATE, pay_date DATE,
    ratio_new INT, ratio_old INT, cash_rate DECIMAL(18,6), announced_ts TIMESTAMP,
    _batch_id STRING
) USING iceberg TBLPROPERTIES ('format-version' = '2');

-- Rejected rows with the reason, for operations to fix and replay.
CREATE TABLE IF NOT EXISTS lake.wm_silver.quarantine (
    source_table STRING, record_key STRING, reject_reason STRING, raw_record STRING,
    _load_ts TIMESTAMP, business_date STRING
) USING iceberg PARTITIONED BY (business_date, source_table) TBLPROPERTIES ('format-version' = '2');

-- JDBC high-water marks, one row per source table and business date, so a rerun
-- of a date re-extracts exactly the same window (idempotent).
CREATE TABLE IF NOT EXISTS lake.wm_audit.jdbc_watermark (
    source_table STRING, business_date STRING, low_watermark TIMESTAMP,
    high_watermark TIMESTAMP, rows_extracted BIGINT, updated_at TIMESTAMP
) USING iceberg TBLPROPERTIES ('format-version' = '2');

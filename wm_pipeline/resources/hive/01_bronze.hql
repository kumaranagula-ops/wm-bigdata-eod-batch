-- BRONZE: raw feeds exactly as received, plus audit columns. External tables, so
-- dropping a table never deletes the files (bronze is the replayable history).
-- Partitioned by business date: every batch writes/overwrites only its own folder.
-- Runs in Spark (setup_catalog job) and also in beeline:
--   beeline -u jdbc:hive2://localhost:10000 -f 01_bronze.hql

CREATE DATABASE IF NOT EXISTS wm_bronze COMMENT 'Raw landed feeds' LOCATION '/data/wm/bronze';

CREATE EXTERNAL TABLE IF NOT EXISTS wm_bronze.trades_raw (
    trade_id        STRING,
    account_id      BIGINT,
    security_id     STRING,
    trade_date      DATE,
    settle_date     DATE,
    side            STRING,
    quantity        DECIMAL(18,4),
    price           DECIMAL(18,6),
    commission      DECIMAL(18,2),
    net_amount      DECIMAL(18,2),
    trade_status    STRING,
    version         INT,
    source_system   STRING,
    exec_ts         TIMESTAMP,
    _corrupt_record STRING,
    _source_file    STRING,
    _load_ts        TIMESTAMP,
    _batch_id       STRING
)
COMMENT 'Custodian / middle-office end-of-day trade blotter (NAS CSV)'
PARTITIONED BY (business_date STRING)
STORED AS PARQUET
LOCATION '/data/wm/bronze/trades_raw';

CREATE EXTERNAL TABLE IF NOT EXISTS wm_bronze.cash_activity_raw (
    txn_id          STRING,
    account_id      BIGINT,
    txn_date        DATE,
    txn_type        STRING,
    amount          DECIMAL(18,2),
    currency        STRING,
    security_id     STRING,
    description     STRING,
    _corrupt_record STRING,
    _source_file    STRING,
    _load_ts        TIMESTAMP,
    _batch_id       STRING
)
COMMENT 'Cash movements: deposits, withdrawals, dividends, interest, fees (NAS pipe-delimited)'
PARTITIONED BY (business_date STRING)
STORED AS PARQUET
LOCATION '/data/wm/bronze/cash_activity_raw';

CREATE EXTERNAL TABLE IF NOT EXISTS wm_bronze.corporate_actions_raw (
    ca_id           STRING,
    security_id     STRING,
    event_type      STRING,
    ex_date         DATE,
    pay_date        DATE,
    terms           STRUCT<ratio_new: INT, ratio_old: INT, cash_rate: DECIMAL(18,6)>,
    announced_ts    TIMESTAMP,
    _corrupt_record STRING,
    _source_file    STRING,
    _load_ts        TIMESTAMP,
    _batch_id       STRING
)
COMMENT 'Corporate action announcements (NAS JSON Lines, nested terms)'
PARTITIONED BY (business_date STRING)
STORED AS PARQUET
LOCATION '/data/wm/bronze/corporate_actions_raw';

CREATE EXTERNAL TABLE IF NOT EXISTS wm_bronze.custodian_positions_raw (
    as_of_date        DATE,
    custodian_account STRING,
    security_cusip    STRING,
    quantity          DECIMAL(18,4),
    custodian_name    STRING,
    _corrupt_record   STRING,
    _source_file      STRING,
    _load_ts          TIMESTAMP,
    _batch_id         STRING
)
COMMENT 'Custodian holdings statement used for middle-office position reconciliation'
PARTITIONED BY (business_date STRING)
STORED AS PARQUET
LOCATION '/data/wm/bronze/custodian_positions_raw';

CREATE EXTERNAL TABLE IF NOT EXISTS wm_bronze.trades_stream_raw (
    trade_id        STRING,
    account_id      BIGINT,
    security_id     STRING,
    trade_date      DATE,
    settle_date     DATE,
    side            STRING,
    quantity        DECIMAL(18,4),
    price           DECIMAL(18,6),
    commission      DECIMAL(18,2),
    net_amount      DECIMAL(18,2),
    trade_status    STRING,
    version         INT,
    source_system   STRING,
    exec_ts         TIMESTAMP,
    event_ts        TIMESTAMP,
    kafka_partition INT,
    kafka_offset    BIGINT,
    _load_ts        TIMESTAMP,
    _batch_id       STRING
)
COMMENT 'Intraday OMS trade events from Kafka (Structured Streaming, availableNow)'
PARTITIONED BY (event_date STRING)
STORED AS PARQUET
LOCATION '/data/wm/bronze/trades_stream_raw';

CREATE EXTERNAL TABLE IF NOT EXISTS wm_bronze.crm_branch_raw (
    branch_id    INT,
    branch_name  STRING,
    city         STRING,
    state        STRING,
    region       STRING,
    updated_at   TIMESTAMP,
    _load_ts     TIMESTAMP,
    _batch_id    STRING
)
PARTITIONED BY (extract_date STRING)
STORED AS PARQUET
LOCATION '/data/wm/bronze/crm_branch_raw';

CREATE EXTERNAL TABLE IF NOT EXISTS wm_bronze.crm_financial_advisor_raw (
    fa_id       INT,
    fa_code     STRING,
    first_name  STRING,
    last_name   STRING,
    branch_id   INT,
    title       STRING,
    hire_date   DATE,
    status      STRING,
    email       STRING,
    updated_at  TIMESTAMP,
    _load_ts    TIMESTAMP,
    _batch_id   STRING
)
PARTITIONED BY (extract_date STRING)
STORED AS PARQUET
LOCATION '/data/wm/bronze/crm_financial_advisor_raw';

CREATE EXTERNAL TABLE IF NOT EXISTS wm_bronze.crm_party_raw (
    party_id      BIGINT,
    party_type    STRING,
    first_name    STRING,
    last_name     STRING,
    org_name      STRING,
    date_of_birth DATE,
    tax_id_last4  STRING,
    risk_profile  STRING,
    kyc_status    STRING,
    country       STRING,
    state         STRING,
    email         STRING,
    created_at    TIMESTAMP,
    updated_at    TIMESTAMP,
    _load_ts      TIMESTAMP,
    _batch_id     STRING
)
PARTITIONED BY (extract_date STRING)
STORED AS PARQUET
LOCATION '/data/wm/bronze/crm_party_raw';

CREATE EXTERNAL TABLE IF NOT EXISTS wm_bronze.crm_account_raw (
    account_id        BIGINT,
    account_number    STRING,
    party_id          BIGINT,
    account_type      STRING,
    tax_status        STRING,
    fa_id             INT,
    open_date         DATE,
    close_date        DATE,
    status            STRING,
    base_currency     STRING,
    cost_basis_method STRING,
    created_at        TIMESTAMP,
    updated_at        TIMESTAMP,
    _load_ts          TIMESTAMP,
    _batch_id         STRING
)
PARTITIONED BY (extract_date STRING)
STORED AS PARQUET
LOCATION '/data/wm/bronze/crm_account_raw';

CREATE EXTERNAL TABLE IF NOT EXISTS wm_bronze.security_master_raw (
    security_id    STRING,
    cusip          STRING,
    ticker         STRING,
    security_name  STRING,
    asset_class    STRING,
    sector         STRING,
    currency       STRING,
    is_tax_exempt  BOOLEAN,
    issuer         STRING,
    _source_file   STRING,
    _load_ts       TIMESTAMP,
    _batch_id      STRING
)
COMMENT 'Vendor security master (S3)'
PARTITIONED BY (load_date STRING)
STORED AS PARQUET
LOCATION '/data/wm/bronze/security_master_raw';

CREATE EXTERNAL TABLE IF NOT EXISTS wm_bronze.prices_raw (
    security_id   STRING,
    close_price   DECIMAL(18,6),
    currency      STRING,
    source        STRING,
    _source_file  STRING,
    _load_ts      TIMESTAMP,
    _batch_id     STRING
)
COMMENT 'Vendor end-of-day closing prices (S3, partitioned by price_date)'
PARTITIONED BY (price_date STRING)
STORED AS PARQUET
LOCATION '/data/wm/bronze/prices_raw';

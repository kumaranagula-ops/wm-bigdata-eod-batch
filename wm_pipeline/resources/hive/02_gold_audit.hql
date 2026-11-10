-- GOLD: business marts, one partition per as_of_date (snapshot tables).
-- AUDIT: run history, data-quality results, reconciliation breaks.
-- Plain Hive Parquet tables so HiveServer2 / beeline and BI tools can query them.

CREATE DATABASE IF NOT EXISTS wm_gold COMMENT 'Books and records marts';
CREATE DATABASE IF NOT EXISTS wm_audit COMMENT 'Run control, DQ and reconciliation';

-- Position = what an account holds in a security on a date (quantity, cost, value).
CREATE TABLE IF NOT EXISTS wm_gold.positions_daily (
    account_id      BIGINT,
    account_number  STRING,
    party_id        BIGINT,
    fa_id           INT,
    security_id     STRING,
    cusip           STRING,
    ticker          STRING,
    asset_class     STRING,
    quantity        DECIMAL(18,4),
    avg_cost        DECIMAL(18,6),
    cost_basis      DECIMAL(18,2),
    close_price     DECIMAL(18,6),
    price_date      DATE,
    market_value    DECIMAL(18,2),
    unrealized_gl   DECIMAL(18,2)
)
PARTITIONED BY (as_of_date STRING)
STORED AS PARQUET;

-- Tax lot = one purchase that is still (partly) held. FIFO relieves the oldest lots first.
CREATE TABLE IF NOT EXISTS wm_gold.tax_lots_open (
    account_id      BIGINT,
    security_id     STRING,
    lot_id          STRING,
    acquired_date   DATE,
    remaining_qty   DECIMAL(18,4),
    cost_per_share  DECIMAL(18,6),
    cost_basis      DECIMAL(18,2),
    close_price     DECIMAL(18,6),
    market_value    DECIMAL(18,2),
    unrealized_gl   DECIMAL(18,2),
    holding_days    INT,
    term            STRING
)
PARTITIONED BY (as_of_date STRING)
STORED AS PARQUET;

-- Realized gain/loss = sell proceeds minus the cost of the lots it relieved (year to date).
CREATE TABLE IF NOT EXISTS wm_gold.realized_gains (
    account_id      BIGINT,
    security_id     STRING,
    sell_trade_id   STRING,
    lot_id          STRING,
    acquired_date   DATE,
    sold_date       DATE,
    quantity        DECIMAL(18,4),
    cost_basis      DECIMAL(18,2),
    proceeds        DECIMAL(18,2),
    realized_gl     DECIMAL(18,2),
    holding_days    INT,
    term            STRING,
    tax_status      STRING,
    is_taxable      BOOLEAN
)
PARTITIONED BY (as_of_date STRING)
STORED AS PARQUET;

-- Bookkeeping cash: trade-date (TD) vs settlement-date (SD) balance.
CREATE TABLE IF NOT EXISTS wm_gold.cash_balances_daily (
    account_id          BIGINT,
    cash_balance_td     DECIMAL(18,2),
    cash_balance_sd     DECIMAL(18,2),
    net_flow_today      DECIMAL(18,2),
    last_activity_date  DATE
)
PARTITIONED BY (as_of_date STRING)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS wm_gold.account_valuation_daily (
    account_id        BIGINT,
    account_number    STRING,
    party_id          BIGINT,
    account_type      STRING,
    tax_status        STRING,
    fa_id             INT,
    fa_name           STRING,
    branch_id         INT,
    branch_name       STRING,
    region            STRING,
    securities_mv     DECIMAL(18,2),
    cash_balance      DECIMAL(18,2),
    total_value       DECIMAL(18,2),
    unrealized_gl     DECIMAL(18,2),
    realized_st_ytd   DECIMAL(18,2),
    realized_lt_ytd   DECIMAL(18,2),
    est_tax_ytd       DECIMAL(18,2),
    equity_mv         DECIMAL(18,2),
    etf_mv            DECIMAL(18,2),
    fixed_income_mv   DECIMAL(18,2),
    mutual_fund_mv    DECIMAL(18,2)
)
PARTITIONED BY (as_of_date STRING)
STORED AS PARQUET;

-- Built by gold_reporting.hql (HiveQL), one row per FA plus branch/region/firm subtotals.
CREATE TABLE IF NOT EXISTS wm_gold.advisor_book_daily (
    agg_level        STRING,
    region           STRING,
    branch_name      STRING,
    fa_id            INT,
    fa_name          STRING,
    num_accounts     BIGINT,
    num_clients      BIGINT,
    aum              DECIMAL(22,2),
    unrealized_gl    DECIMAL(22,2),
    realized_gl_ytd  DECIMAL(22,2),
    rank_in_region   INT
)
PARTITIONED BY (as_of_date STRING)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS wm_gold.client_portfolio_summary (
    party_id          BIGINT,
    num_accounts      BIGINT,
    primary_fa_id     INT,
    total_value       DECIMAL(22,2),
    cash_balance      DECIMAL(22,2),
    unrealized_gl     DECIMAL(22,2),
    realized_gl_ytd   DECIMAL(22,2),
    est_tax_ytd       DECIMAL(22,2),
    pct_equity        DECIMAL(9,4),
    pct_etf           DECIMAL(9,4),
    pct_fixed_income  DECIMAL(9,4),
    pct_mutual_fund   DECIMAL(9,4),
    pct_cash          DECIMAL(9,4),
    client_segment    STRING,
    wealth_decile     INT
)
PARTITIONED BY (as_of_date STRING)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS wm_audit.run_control (
    run_id         STRING,
    job_name       STRING,
    status         STRING,
    rows_in        BIGINT,
    rows_out       BIGINT,
    rows_rejected  BIGINT,
    started_at     TIMESTAMP,
    ended_at       TIMESTAMP,
    message        STRING,
    metrics_json   STRING
)
PARTITIONED BY (business_date STRING)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS wm_audit.dq_results (
    check_name     STRING,
    table_name     STRING,
    severity       STRING,
    failed_count   BIGINT,
    status         STRING,
    details        STRING,
    checked_at     TIMESTAMP
)
PARTITIONED BY (business_date STRING, job_name STRING)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS wm_audit.recon_breaks (
    account_id        BIGINT,
    account_number    STRING,
    security_id       STRING,
    cusip             STRING,
    book_quantity     DECIMAL(18,4),
    custodian_quantity DECIMAL(18,4),
    quantity_diff     DECIMAL(18,4),
    break_type        STRING,
    detected_at       TIMESTAMP
)
PARTITIONED BY (business_date STRING)
STORED AS PARQUET;

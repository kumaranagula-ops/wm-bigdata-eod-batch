-- CRM / client-onboarding system of record (the OLTP side of wealth management).
-- Spark extracts these tables incrementally over JDBC (watermark on updated_at),
-- the same job Sqoop used to do on Hadoop clusters.
CREATE DATABASE IF NOT EXISTS wm_crm;
USE wm_crm;

CREATE TABLE IF NOT EXISTS branch (
    branch_id    INT          PRIMARY KEY,
    branch_name  VARCHAR(100) NOT NULL,
    city         VARCHAR(60)  NOT NULL,
    state        CHAR(2)      NOT NULL,
    region       VARCHAR(20)  NOT NULL,
    updated_at   DATETIME     NOT NULL,
    INDEX ix_branch_updated (updated_at)
);

CREATE TABLE IF NOT EXISTS financial_advisor (
    fa_id       INT          PRIMARY KEY,
    fa_code     VARCHAR(20)  NOT NULL UNIQUE,
    first_name  VARCHAR(60)  NOT NULL,
    last_name   VARCHAR(60)  NOT NULL,
    branch_id   INT          NOT NULL,
    title       VARCHAR(60),
    hire_date   DATE,
    status      VARCHAR(20)  NOT NULL,          -- ACTIVE / TERMINATED
    email       VARCHAR(120),
    updated_at  DATETIME     NOT NULL,
    INDEX ix_fa_updated (updated_at),
    FOREIGN KEY (branch_id) REFERENCES branch (branch_id)
);

-- A party is any legal entity the firm has a relationship with: a person or an
-- organisation (trust, foundation, company). Customers are parties that own accounts.
CREATE TABLE IF NOT EXISTS party (
    party_id      BIGINT       PRIMARY KEY,
    party_type    VARCHAR(3)   NOT NULL,        -- IND / ORG
    first_name    VARCHAR(60),
    last_name     VARCHAR(60),
    org_name      VARCHAR(120),
    date_of_birth DATE,
    tax_id_last4  CHAR(4),                      -- never store the full SSN/TIN in analytics
    risk_profile  VARCHAR(20)  NOT NULL,        -- CONSERVATIVE .. AGGRESSIVE (suitability)
    kyc_status    VARCHAR(20)  NOT NULL,        -- VERIFIED / PENDING
    country       CHAR(2)      NOT NULL,
    state         CHAR(2),
    email         VARCHAR(120),
    created_at    DATETIME     NOT NULL,
    updated_at    DATETIME     NOT NULL,
    INDEX ix_party_updated (updated_at)
);

CREATE TABLE IF NOT EXISTS account (
    account_id        BIGINT       PRIMARY KEY,
    account_number    VARCHAR(20)  NOT NULL UNIQUE,
    party_id          BIGINT       NOT NULL,
    account_type      VARCHAR(20)  NOT NULL,    -- BROKERAGE / IRA / ROTH_IRA / ADVISORY / TRUST
    tax_status        VARCHAR(20)  NOT NULL,    -- TAXABLE / TAX_DEFERRED / TAX_EXEMPT
    fa_id             INT          NOT NULL,    -- servicing financial advisor
    open_date         DATE         NOT NULL,
    close_date        DATE,
    status            VARCHAR(20)  NOT NULL,    -- ACTIVE / CLOSED
    base_currency     CHAR(3)      NOT NULL,
    cost_basis_method VARCHAR(10)  NOT NULL,    -- FIFO (default lot relief method)
    created_at        DATETIME     NOT NULL,
    updated_at        DATETIME     NOT NULL,
    INDEX ix_account_updated (updated_at),
    FOREIGN KEY (party_id) REFERENCES party (party_id),
    FOREIGN KEY (fa_id) REFERENCES financial_advisor (fa_id)
);

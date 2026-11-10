-- Gold reporting marts in plain HiveQL, built from wm_gold.account_valuation_daily.
--
-- In the DAG this file runs through Spark SQL (gold_reporting job), which substitutes
-- ${hivevar:as_of_date}. The same file works in beeline against HiveServer2:
--   beeline -u jdbc:hive2://localhost:10000 --hivevar as_of_date=2026-11-09 -f gold_reporting.hql
--
-- Only portable HiveQL here (CTEs, window functions, UNION ALL). GROUPING SETS would be
-- shorter, but GROUPING__ID numbering differs between Hive versions and Spark.

-- 1) Advisor book: AUM per FA, with branch, region and firm subtotals and a ranking.
INSERT OVERWRITE TABLE wm_gold.advisor_book_daily PARTITION (as_of_date = '${hivevar:as_of_date}')
SELECT agg_level, region, branch_name, fa_id, fa_name, num_accounts, num_clients,
       aum, unrealized_gl, realized_gl_ytd, rank_in_region
FROM (
    SELECT 'FA' AS agg_level, region, branch_name, fa_id, fa_name,
           COUNT(DISTINCT account_id) AS num_accounts,
           COUNT(DISTINCT party_id)   AS num_clients,
           CAST(SUM(total_value) AS DECIMAL(22,2))                       AS aum,
           CAST(SUM(unrealized_gl) AS DECIMAL(22,2))                     AS unrealized_gl,
           CAST(SUM(realized_st_ytd + realized_lt_ytd) AS DECIMAL(22,2)) AS realized_gl_ytd,
           CAST(DENSE_RANK() OVER (PARTITION BY region ORDER BY SUM(total_value) DESC) AS INT) AS rank_in_region
    FROM wm_gold.account_valuation_daily
    WHERE as_of_date = '${hivevar:as_of_date}'
    GROUP BY region, branch_name, fa_id, fa_name

    UNION ALL

    SELECT 'BRANCH', region, branch_name, CAST(NULL AS INT), CAST(NULL AS STRING),
           COUNT(DISTINCT account_id), COUNT(DISTINCT party_id),
           CAST(SUM(total_value) AS DECIMAL(22,2)),
           CAST(SUM(unrealized_gl) AS DECIMAL(22,2)),
           CAST(SUM(realized_st_ytd + realized_lt_ytd) AS DECIMAL(22,2)),
           CAST(DENSE_RANK() OVER (PARTITION BY region ORDER BY SUM(total_value) DESC) AS INT)
    FROM wm_gold.account_valuation_daily
    WHERE as_of_date = '${hivevar:as_of_date}'
    GROUP BY region, branch_name

    UNION ALL

    SELECT 'REGION', region, CAST(NULL AS STRING), CAST(NULL AS INT), CAST(NULL AS STRING),
           COUNT(DISTINCT account_id), COUNT(DISTINCT party_id),
           CAST(SUM(total_value) AS DECIMAL(22,2)),
           CAST(SUM(unrealized_gl) AS DECIMAL(22,2)),
           CAST(SUM(realized_st_ytd + realized_lt_ytd) AS DECIMAL(22,2)),
           CAST(NULL AS INT)
    FROM wm_gold.account_valuation_daily
    WHERE as_of_date = '${hivevar:as_of_date}'
    GROUP BY region

    UNION ALL

    SELECT 'FIRM', CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS INT), CAST(NULL AS STRING),
           COUNT(DISTINCT account_id), COUNT(DISTINCT party_id),
           CAST(SUM(total_value) AS DECIMAL(22,2)),
           CAST(SUM(unrealized_gl) AS DECIMAL(22,2)),
           CAST(SUM(realized_st_ytd + realized_lt_ytd) AS DECIMAL(22,2)),
           CAST(NULL AS INT)
    FROM wm_gold.account_valuation_daily
    WHERE as_of_date = '${hivevar:as_of_date}'
) book;

-- 2) Client (household) summary: total wealth, allocation, tax drag, segment.
WITH acct AS (
    SELECT * FROM wm_gold.account_valuation_daily WHERE as_of_date = '${hivevar:as_of_date}'
),
primary_fa AS (
    -- The FA servicing the client's largest account. The account_id tie breaker
    -- keeps row_number deterministic across reruns.
    SELECT party_id, fa_id,
           ROW_NUMBER() OVER (PARTITION BY party_id ORDER BY total_value DESC, account_id) AS rn
    FROM acct
),
agg AS (
    SELECT party_id,
           COUNT(*)                                AS num_accounts,
           SUM(total_value)                        AS total_value,
           SUM(cash_balance)                       AS cash_balance,
           SUM(unrealized_gl)                      AS unrealized_gl,
           SUM(realized_st_ytd + realized_lt_ytd)  AS realized_gl_ytd,
           SUM(est_tax_ytd)                        AS est_tax_ytd,
           SUM(equity_mv)                          AS equity_mv,
           SUM(etf_mv)                             AS etf_mv,
           SUM(fixed_income_mv)                    AS fixed_income_mv,
           SUM(mutual_fund_mv)                     AS mutual_fund_mv
    FROM acct
    GROUP BY party_id
)
INSERT OVERWRITE TABLE wm_gold.client_portfolio_summary PARTITION (as_of_date = '${hivevar:as_of_date}')
SELECT a.party_id,
       a.num_accounts,
       p.fa_id,
       CAST(a.total_value AS DECIMAL(22,2)),
       CAST(a.cash_balance AS DECIMAL(22,2)),
       CAST(a.unrealized_gl AS DECIMAL(22,2)),
       CAST(a.realized_gl_ytd AS DECIMAL(22,2)),
       CAST(a.est_tax_ytd AS DECIMAL(22,2)),
       CAST(CASE WHEN a.total_value > 0 THEN a.equity_mv / a.total_value ELSE 0 END AS DECIMAL(9,4)),
       CAST(CASE WHEN a.total_value > 0 THEN a.etf_mv / a.total_value ELSE 0 END AS DECIMAL(9,4)),
       CAST(CASE WHEN a.total_value > 0 THEN a.fixed_income_mv / a.total_value ELSE 0 END AS DECIMAL(9,4)),
       CAST(CASE WHEN a.total_value > 0 THEN a.mutual_fund_mv / a.total_value ELSE 0 END AS DECIMAL(9,4)),
       CAST(CASE WHEN a.total_value > 0 THEN a.cash_balance / a.total_value ELSE 0 END AS DECIMAL(9,4)),
       CASE WHEN a.total_value >= 5000000 THEN 'ULTRA_HIGH_NET_WORTH'
            WHEN a.total_value >= 1000000 THEN 'HIGH_NET_WORTH'
            WHEN a.total_value >= 250000  THEN 'AFFLUENT'
            ELSE 'MASS_AFFLUENT' END,
       -- Global window (no PARTITION BY) = one task; fine for ~1.5k clients, not for 50M.
       CAST(NTILE(10) OVER (ORDER BY a.total_value DESC) AS INT)
FROM agg a
JOIN primary_fa p ON p.party_id = a.party_id AND p.rn = 1;

/********************************************************************************************
 SCRIPT NAME  : OPERATION CLEARWATER — FRAUD DETECTION & RISK INTELLIGENCE
 DATABASE     : NorthAxis Bank | Gold Layer Data Warehouse
 ENVIRONMENT  : Microsoft SQL Server 2019+
 AUTHOR       : Risk Intelligence Division — NAB-RI-2024-09
 CREATED      : Q3 2024

 DELIVERABLES :
     1. Transaction Overview & Baseline KPIs
     2. Anomaly Detection & Velocity Checks
     3. Customer Risk Profiling
     4. Merchant & Channel Risk Scoring
     5. Fraud Risk Scoring Model (Composite Watchlist)
     6. Executive Risk Report & Recommendations

 SCHEMA       : dbo.*
 TABLES       : fact_transactions, dim_customer, dim_account,
                dim_merchant, dim_location, dim_date

 HOW TO RUN:
     Step 1 — Execute the FOUNDATION block to materialise temp tables.
     Step 2 — Run each deliverable independently, or execute the full script as a batch.
     Step 3 — The CLEANUP block at the end drops all temp tables.

 OPTIMISATION NOTES:
     - is_flagged and is_off_hours pre-cast once in #flagged_base
     - Portfolio stats computed once in #portfolio_stats
     - dim_date spine materialised once in #date_spine
     - Watchlist scored once in #watchlist — shared by 5.1 and 5.2
     - Fraud exposure materialised once in #exec_summary — shared by 6.1 and 6.5
     - MoM LAG() materialised once per query via nested CTE
     - Velocity check includes teaching version (self-join) and production version (LAG)

 PRODUCTION INDEX RECOMMENDATIONS:
     CREATE INDEX ix_ft_customer_date    ON fact_transactions (customer_key, transaction_date, transaction_datetime)
     CREATE INDEX ix_ft_date_key         ON fact_transactions (date_key)
     CREATE INDEX ix_ft_merchant_key     ON fact_transactions (merchant_key)
     CREATE INDEX ix_ft_location_key     ON fact_transactions (location_key)
     CREATE INDEX ix_ft_flagged          ON fact_transactions (is_flagged) INCLUDE (amount_usd, customer_key)
*********************************************************************************************/


/********************************************************************************************
 FOUNDATION — Run this block before executing any deliverable.
*********************************************************************************************/

-- Pre-cast is_flagged and is_off_hours once — eliminates repeated inline casts downstream
DROP TABLE IF EXISTS #flagged_base;
SELECT
    *,
    CAST(is_flagged   AS INT)   AS is_flagged_int,
    CAST(is_off_hours AS INT)   AS is_off_hours_int
INTO #flagged_base
FROM fact_transactions;


-- Portfolio-level stats — computed once, joined wherever needed
DROP TABLE IF EXISTS #portfolio_stats;
SELECT
    COUNT(*)            AS total_rows,
    AVG(amount_usd)     AS mean_amount,
    STDEV(amount_usd)   AS std_dev_amount,
    SUM(CAST(is_flagged AS INT)) AS total_flagged
INTO #portfolio_stats
FROM fact_transactions;


-- Date spine — joined repeatedly across D1, D2, D6
DROP TABLE IF EXISTS #date_spine;
SELECT
    date_key,
    year_number,
    month_number,
    month_name,
    quarter_name,
    day_of_week,
    is_weekend
INTO #date_spine
FROM dim_date;


/********************************************************************************************
 DELIVERABLE 1 — TRANSACTION OVERVIEW & BASELINE KPIs
*********************************************************************************************/

-- 1.1  Portfolio-Level Summary
-- RESULT: 195,276 transactions | $355M total volume | std dev $5,873 — high spread signals anomaly risk

SELECT
    COUNT(*)                                        AS total_transaction_lines,
    COUNT(DISTINCT transaction_id)                  AS total_distinct_transactions,
    COUNT(DISTINCT customer_key)                    AS total_customers,
    COUNT(DISTINCT account_key)                     AS total_accounts,
    ROUND(SUM(amount_usd), 2)                       AS total_volume_usd,
    ROUND(AVG(amount_usd), 2)                       AS avg_transaction_amount,
    ROUND(MIN(amount_usd), 2)                       AS min_transaction_amount,
    ROUND(MAX(amount_usd), 2)                       AS max_transaction_amount,
    ROUND(MAX(ps.std_dev_amount), 2)                AS std_dev_amount
FROM #flagged_base
CROSS JOIN #portfolio_stats ps;


-- 1.2  Volume & Value by Channel
-- RESULT: Mobile Banking leads at 39.9% of transactions and 41.1% of volume

SELECT
    channel,
    COUNT(*)                                        AS transaction_count,
    COUNT(DISTINCT customer_key)                    AS unique_customers,
    ROUND(SUM(amount_usd), 2)                       AS total_volume_usd,
    ROUND(AVG(amount_usd), 2)                       AS avg_transaction_amount,
    ROUND(COUNT(*) * 100.0
        / SUM(COUNT(*)) OVER (), 2)                 AS transaction_pct,
    ROUND(SUM(amount_usd) * 100.0
        / SUM(SUM(amount_usd)) OVER (), 2)          AS volume_pct
FROM #flagged_base
GROUP BY channel
ORDER BY total_volume_usd DESC;


-- 1.3  Monthly Trend with MoM Change
-- RESULT: Jan–Apr flat at ~$20M/month. May spikes 129% MoM to $46M — fraud window opens.
-- LAG value materialised once in inner CTE — not evaluated twice

WITH monthly AS (
    SELECT
        d.year_number,
        d.month_number,
        d.month_name,
        d.quarter_name,
        COUNT(*)                                    AS total_transactions,
        COUNT(DISTINCT f.customer_key)              AS active_customers,
        ROUND(SUM(f.amount_usd), 2)                 AS total_volume_usd,
        ROUND(AVG(f.amount_usd), 2)                 AS avg_transaction_amount
    FROM #flagged_base f
    JOIN #date_spine d ON f.date_key = d.date_key
    GROUP BY
        d.year_number, d.month_number,
        d.month_name,  d.quarter_name
),
with_lag AS (
    SELECT *,
        LAG(total_volume_usd) OVER (
            ORDER BY year_number, month_number
        )                                           AS prev_month_volume
    FROM monthly
)
SELECT
    year_number,
    month_number,
    month_name,
    quarter_name,
    total_transactions,
    active_customers,
    total_volume_usd,
    avg_transaction_amount,
    ROUND(total_volume_usd - prev_month_volume, 2)  AS mom_volume_change,
    ROUND((total_volume_usd - prev_month_volume)
        * 100.0 / NULLIF(prev_month_volume, 0), 2) AS mom_change_pct
FROM with_lag
ORDER BY year_number, month_number;


-- 1.4  KYC Status Distribution
-- RESULT: 80.7% Verified | 11.4% Pending | 8.0% Expired — Expired customers avg $2,046/txn vs $1,799 Verified

SELECT
    c.kyc_status,
    COUNT(DISTINCT c.customer_key)                  AS customer_count,
    COUNT(f.transaction_key)                        AS transaction_count,
    ROUND(SUM(f.amount_usd), 2)                     AS total_volume_usd,
    ROUND(AVG(f.amount_usd), 2)                     AS avg_transaction_amount,
    ROUND(COUNT(DISTINCT c.customer_key) * 100.0
        / SUM(COUNT(DISTINCT c.customer_key)) OVER (), 2)
                                                    AS pct_of_customers
FROM dim_customer c
LEFT JOIN #flagged_base f ON c.customer_key = f.customer_key
GROUP BY c.kyc_status
ORDER BY total_volume_usd DESC;


/********************************************************************************************
 DELIVERABLE 2 — ANOMALY DETECTION & VELOCITY CHECKS
*********************************************************************************************/

-- 2.1  Off-Hours vs Business Hours Comparison
-- RESULT: Off-hours avg $4,897 vs $1,295 business hours — 3.8x higher average transaction value

SELECT
    CASE
        WHEN transaction_hour BETWEEN 1  AND 4  THEN 'Off-Hours (1AM-4AM)'
        WHEN transaction_hour BETWEEN 8  AND 18 THEN 'Business Hours (8AM-6PM)'
        ELSE                                         'Transition Hours'
    END                                             AS time_window,
    COUNT(*)                                        AS total_transactions,
    COUNT(DISTINCT customer_key)                    AS unique_customers,
    ROUND(SUM(amount_usd), 2)                       AS total_volume_usd,
    ROUND(AVG(amount_usd), 2)                       AS avg_transaction_amount,
    ROUND(COUNT(*) * 100.0
        / SUM(COUNT(*)) OVER (), 2)                 AS pct_of_transactions
FROM #flagged_base
GROUP BY
    CASE
        WHEN transaction_hour BETWEEN 1  AND 4  THEN 'Off-Hours (1AM-4AM)'
        WHEN transaction_hour BETWEEN 8  AND 18 THEN 'Business Hours (8AM-6PM)'
        ELSE                                         'Transition Hours'
    END
ORDER BY total_volume_usd DESC;


-- 2.2  Velocity Check — Rapid Repeat Transactions
-- RESULT: Top offenders show 887 rapid pairs (self-join) / 101 rapid transactions (LAG)
--         Same customers surface in both — ranking is consistent, counts differ by design.
--
-- TWO VERSIONS ARE PROVIDED INTENTIONALLY:
--
-- TEACHING VERSION (Self-Join) — run this to understand the detection logic.
--   Pairs every transaction against every other for the same customer on the same day,
--   then filters pairs where the gap is under 60 minutes. Easy to read and audit.
--   Limitation: O(n²) cost — acceptable at 195K rows, problematic at 10M+.
--
-- PRODUCTION VERSION (LAG) — run this in any real environment.
--   Uses LAG() to compare each transaction only against the one immediately before it.
--   O(n) — one sequential pass per customer. Scales to any volume without degradation.
--   Tradeoff: catches consecutive gaps only. Non-consecutive pairs (txn 1 and txn 3
--   within 60 min but txn 2 outside) are missed. For velocity fraud patterns —
--   card testing, account draining — consecutive is almost always the signature.

-- TEACHING VERSION
WITH rapid_transactions AS (
    SELECT
        f1.customer_key,
        f1.transaction_id                           AS txn_a,
        f2.transaction_id                           AS txn_b,
        f1.transaction_datetime                     AS time_a,
        f2.transaction_datetime                     AS time_b,
        f1.amount_usd                               AS amount_a,
        f2.amount_usd                               AS amount_b,
        DATEDIFF(MINUTE,
            CAST(f1.transaction_datetime AS DATETIME),
            CAST(f2.transaction_datetime AS DATETIME)) AS minutes_apart
    FROM #flagged_base f1
    JOIN #flagged_base f2
        ON  f1.customer_key       = f2.customer_key
        AND f1.transaction_date   = f2.transaction_date
        AND f1.transaction_id    != f2.transaction_id
        AND f2.transaction_datetime > f1.transaction_datetime
        AND DATEDIFF(MINUTE,
                CAST(f1.transaction_datetime AS DATETIME),
                CAST(f2.transaction_datetime AS DATETIME)) <= 60
),
velocity_summary AS (
    SELECT
        customer_key,
        COUNT(*)                                    AS rapid_pair_count,
        MIN(minutes_apart)                          AS min_gap_minutes,
        ROUND(AVG(CAST(minutes_apart AS FLOAT)), 1) AS avg_gap_minutes,
        ROUND(SUM(amount_a + amount_b) / 2.0, 2)   AS approx_volume_usd
    FROM rapid_transactions
    GROUP BY customer_key
    HAVING COUNT(*) >= 5
)
SELECT TOP 50
    vs.customer_key,
    c.full_name,
    c.country,
    c.kyc_status,
    vs.rapid_pair_count,
    vs.min_gap_minutes,
    vs.avg_gap_minutes,
    vs.approx_volume_usd
FROM velocity_summary vs
JOIN dim_customer c ON vs.customer_key = c.customer_key
ORDER BY vs.rapid_pair_count DESC;


-- PRODUCTION VERSION
WITH transaction_gaps AS (
    SELECT
        customer_key,
        transaction_id,
        transaction_datetime,
        amount_usd,
        transaction_date,
        LAG(transaction_datetime) OVER (
            PARTITION BY customer_key
            ORDER BY transaction_datetime
        )                                           AS prev_transaction_datetime,
        LAG(amount_usd) OVER (
            PARTITION BY customer_key
            ORDER BY transaction_datetime
        )                                           AS prev_amount_usd
    FROM #flagged_base
),
gaps_calculated AS (
    SELECT
        customer_key,
        transaction_id,
        transaction_datetime,
        prev_transaction_datetime,
        amount_usd,
        prev_amount_usd,
        DATEDIFF(
            MINUTE,
            CAST(prev_transaction_datetime AS DATETIME),
            CAST(transaction_datetime AS DATETIME)
        )                                           AS minutes_since_last_txn
    FROM transaction_gaps
    WHERE prev_transaction_datetime IS NOT NULL
),
velocity_flags AS (
    SELECT
        customer_key,
        COUNT(*)                                    AS rapid_txn_count,
        MIN(minutes_since_last_txn)                 AS min_gap_minutes,
        ROUND(AVG(CAST(minutes_since_last_txn AS FLOAT)), 1)
                                                    AS avg_gap_minutes,
        ROUND(SUM(amount_usd), 2)                   AS rapid_window_volume_usd
    FROM gaps_calculated
    WHERE minutes_since_last_txn <= 60
    GROUP BY customer_key
    HAVING COUNT(*) >= 5
)
SELECT TOP 50
    vf.customer_key,
    c.full_name,
    c.country,
    c.kyc_status,
    vf.rapid_txn_count,
    vf.min_gap_minutes,
    vf.avg_gap_minutes,
    vf.rapid_window_volume_usd
FROM velocity_flags vf
JOIN dim_customer c ON vf.customer_key = c.customer_key
ORDER BY vf.rapid_txn_count DESC;


-- 2.3  Amount Outlier Detection — Z-Score
-- RESULT: Top 100 outliers are exclusively Wire Transfers through Shell Merchants
--         into high-risk countries. Z-scores cluster at 8.1–8.2 — 8x above portfolio mean.
-- mean and std_dev sourced from #portfolio_stats — no re-scan of fact table

SELECT TOP 100
    f.transaction_id,
    f.customer_key,
    c.full_name,
    f.transaction_datetime,
    f.transaction_type,
    f.channel,
    f.amount_usd,
    ROUND(ps.mean_amount, 2)                        AS mean_amount,
    ROUND(ps.std_dev_amount, 2)                     AS std_dev_amount,
    ROUND((f.amount_usd - ps.mean_amount)
        / NULLIF(ps.std_dev_amount, 0), 2)          AS z_score,
    m.merchant_name,
    m.merchant_category,
    m.is_shell_merchant,
    l.country                                       AS transaction_country,
    l.is_high_risk_country
FROM #flagged_base f
CROSS JOIN #portfolio_stats ps
JOIN dim_customer c ON f.customer_key = c.customer_key
JOIN dim_merchant m ON f.merchant_key = m.merchant_key
JOIN dim_location l ON f.location_key = l.location_key
WHERE (f.amount_usd - ps.mean_amount)
    / NULLIF(ps.std_dev_amount, 0) > 3
ORDER BY z_score DESC;


-- 2.4  Daily Spike Detection — 7-Day Rolling Average
-- RESULT: Apr 30 is the only SPIKE DETECTED day at 2.04x rolling avg —
--         the ramp-up day immediately before the May fraud window opens.
-- spike_ratio computed once in inner CTE — not evaluated twice

WITH daily_volume AS (
    SELECT
        f.transaction_date,
        d.day_of_week,
        d.is_weekend,
        COUNT(*)                                    AS transaction_count,
        ROUND(SUM(f.amount_usd), 2)                 AS daily_volume_usd
    FROM #flagged_base f
    JOIN #date_spine d ON f.date_key = d.date_key
    GROUP BY f.transaction_date, d.day_of_week, d.is_weekend
),
with_rolling AS (
    SELECT *,
        ROUND(AVG(daily_volume_usd) OVER (
            ORDER BY transaction_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 2)                                       AS rolling_7d_avg_usd,
        ROUND(daily_volume_usd / NULLIF(
            AVG(daily_volume_usd) OVER (
                ORDER BY transaction_date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ), 0), 2)                               AS spike_ratio
    FROM daily_volume
)
SELECT
    transaction_date,
    day_of_week,
    is_weekend,
    transaction_count,
    daily_volume_usd,
    rolling_7d_avg_usd,
    spike_ratio,
    CASE
        WHEN spike_ratio >= 2.0 THEN 'SPIKE DETECTED'
        WHEN spike_ratio >= 1.5 THEN 'Elevated'
        ELSE                         'Normal'
    END                                             AS spike_flag
FROM with_rolling
ORDER BY spike_ratio DESC;


/********************************************************************************************
 DELIVERABLE 3 — CUSTOMER RISK PROFILING
*********************************************************************************************/

-- 3.1  Account Takeover Detection
-- RESULT: Top customers show 94–105x spend multiplier post-May with hour shifts up to 6.7hrs
--         and transactions spanning 20 distinct countries — coordinated takeover signature.
-- Baseline folded in as early_period CTE — no standalone query needed

WITH early_period AS (
    SELECT
        customer_key,
        ROUND(AVG(amount_usd), 2)                   AS early_avg_spend,
        ROUND(AVG(CAST(transaction_hour AS FLOAT)), 1)
                                                    AS early_avg_hour,
        COUNT(*)                                    AS early_txn_count,
        ROUND(STDEV(amount_usd), 2)                 AS early_spend_stddev
    FROM #flagged_base
    WHERE transaction_date < '2024-05-01'
    GROUP BY customer_key
),
late_period AS (
    SELECT
        f.customer_key,
        ROUND(AVG(f.amount_usd), 2)                 AS late_avg_spend,
        ROUND(AVG(CAST(f.transaction_hour AS FLOAT)), 1)
                                                    AS late_avg_hour,
        COUNT(*)                                    AS late_txn_count,
        COUNT(DISTINCT l.country)                   AS distinct_countries_late
    FROM #flagged_base f
    JOIN dim_location l ON f.location_key = l.location_key
    WHERE f.transaction_date >= '2024-05-01'
    GROUP BY f.customer_key
)
SELECT TOP 100
    e.customer_key,
    c.full_name,
    c.country                                       AS home_country,
    c.kyc_status,
    c.customer_segment,
    c.preferred_channel,
    e.early_avg_spend,
    lp.late_avg_spend,
    ROUND(lp.late_avg_spend
        / NULLIF(e.early_avg_spend, 0), 1)          AS spend_multiplier,
    e.early_avg_hour,
    lp.late_avg_hour,
    ROUND(ABS(lp.late_avg_hour - e.early_avg_hour), 1)
                                                    AS hour_shift,
    e.early_txn_count,
    lp.late_txn_count,
    lp.distinct_countries_late,
    CASE
        WHEN lp.late_avg_spend / NULLIF(e.early_avg_spend, 0) >= 5
         AND ABS(lp.late_avg_hour - e.early_avg_hour) >= 4
                                                    THEN 'HIGH RISK — Likely Takeover'
        WHEN lp.late_avg_spend / NULLIF(e.early_avg_spend, 0) >= 3
                                                    THEN 'MEDIUM RISK — Investigate'
        ELSE                                             'Normal Behaviour'
    END                                             AS takeover_flag
FROM early_period e
JOIN late_period  lp ON e.customer_key  = lp.customer_key
JOIN dim_customer c  ON e.customer_key  = c.customer_key
WHERE e.early_txn_count  >= 3
  AND lp.late_txn_count  >= 5
ORDER BY spend_multiplier DESC;


-- 3.2  Geographic Anomaly — Customer vs Transaction Country
-- RESULT: Multi-jurisdiction fraud concentrated in Venezuela, Belarus, Russia, North Korea,
--         Myanmar, Cuba, Iran, Syria, Somalia, Sudan — all starting May 2024 simultaneously.

SELECT
    f.customer_key,
    c.full_name,
    c.country                                       AS registered_country,
    l.country                                       AS transaction_country,
    l.is_high_risk_country,
    COUNT(*)                                        AS foreign_transaction_count,
    ROUND(SUM(f.amount_usd), 2)                     AS foreign_volume_usd,
    ROUND(AVG(f.amount_usd), 2)                     AS avg_foreign_txn_value,
    MIN(f.transaction_date)                         AS first_foreign_txn,
    MAX(f.transaction_date)                         AS last_foreign_txn
FROM #flagged_base f
JOIN dim_customer c ON f.customer_key = c.customer_key
JOIN dim_location l ON f.location_key = l.location_key
WHERE l.country            != c.country
  AND l.is_high_risk_country = 1
GROUP BY
    f.customer_key, c.full_name,
    c.country,      l.country,
    l.is_high_risk_country
HAVING COUNT(*) >= 3
ORDER BY foreign_volume_usd DESC;


/********************************************************************************************
 DELIVERABLE 4 — MERCHANT & CHANNEL RISK SCORING
*********************************************************************************************/

-- 4.1  Merchant Risk Profile
-- RESULT: 8 shell merchants with 100% flagged rates — all registered in sanctioned countries.
--         Rank 9 onwards drops below 4% — no grey area between legitimate and shell merchants.

SELECT
    m.merchant_key,
    m.merchant_name,
    m.merchant_category,
    m.risk_rating,
    m.is_shell_merchant,
    m.country                                       AS merchant_country,
    COUNT(*)                                        AS total_transactions,
    COUNT(DISTINCT f.customer_key)                  AS unique_customers,
    ROUND(SUM(f.amount_usd), 2)                     AS total_volume_usd,
    ROUND(AVG(f.amount_usd), 2)                     AS avg_transaction_value,
    SUM(f.is_flagged_int)                           AS flagged_count,
    ROUND(SUM(f.is_flagged_int) * 100.0
        / NULLIF(COUNT(*), 0), 2)                   AS flagged_rate_pct,
    RANK() OVER (
        ORDER BY SUM(f.is_flagged_int) DESC
    )                                               AS risk_rank
FROM #flagged_base f
JOIN dim_merchant m ON f.merchant_key = m.merchant_key
GROUP BY
    m.merchant_key, m.merchant_name, m.merchant_category,
    m.risk_rating,  m.is_shell_merchant, m.country
ORDER BY flagged_count DESC;


-- 4.2  Shell Merchant Deep Dive
-- RESULT: Web Banking and Mobile Banking carry almost all shell merchant volume.
--         High-risk-to-high-risk flows dominate; non-high-risk country rows are layering transactions.

SELECT
    m.merchant_name,
    m.merchant_category,
    m.country                                       AS merchant_country,
    l.country                                       AS transaction_origin_country,
    l.is_high_risk_country,
    f.channel,
    COUNT(*)                                        AS transaction_count,
    COUNT(DISTINCT f.customer_key)                  AS unique_customers,
    ROUND(SUM(f.amount_usd), 2)                     AS total_volume_usd,
    ROUND(AVG(f.amount_usd), 2)                     AS avg_transaction_value
FROM #flagged_base f
JOIN dim_merchant m ON f.merchant_key = m.merchant_key
JOIN dim_location l ON f.location_key = l.location_key
WHERE m.is_shell_merchant = 1
GROUP BY
    m.merchant_name, m.merchant_category, m.country,
    l.country, l.is_high_risk_country, f.channel
ORDER BY total_volume_usd DESC;


-- 4.3  Channel Risk Comparison
-- RESULT: Mobile Banking highest flagged count (14.4%). Web Banking highest flagged volume (80.6%).
--         ATM at 2.5% — used as cash-out exit, not primary attack surface.

SELECT
    f.channel,
    COUNT(*)                                        AS total_transactions,
    SUM(f.is_flagged_int)                           AS flagged_count,
    ROUND(SUM(f.is_flagged_int) * 100.0
        / NULLIF(COUNT(*), 0), 2)                   AS flagged_rate_pct,
    ROUND(SUM(f.amount_usd), 2)                     AS total_volume_usd,
    ROUND(SUM(CASE WHEN f.is_flagged_int = 1
                   THEN f.amount_usd ELSE 0 END), 2) AS flagged_volume_usd,
    ROUND(SUM(CASE WHEN f.is_flagged_int = 1
                   THEN f.amount_usd ELSE 0 END) * 100.0
        / NULLIF(SUM(f.amount_usd), 0), 2)          AS pct_volume_flagged
FROM #flagged_base f
GROUP BY f.channel
ORDER BY flagged_rate_pct DESC;


-- 4.4  High-Risk Country Exposure
-- RESULT: All 10 high-risk countries show 100% flagged rates — zero legitimate transactions.
--         These 10 countries represent ~42% of total portfolio volume.

SELECT
    l.country,
    l.is_high_risk_country,
    COUNT(*)                                        AS transaction_count,
    COUNT(DISTINCT f.customer_key)                  AS unique_customers,
    ROUND(SUM(f.amount_usd), 2)                     AS total_volume_usd,
    SUM(f.is_flagged_int)                           AS flagged_count,
    ROUND(SUM(f.is_flagged_int) * 100.0
        / NULLIF(COUNT(*), 0), 2)                   AS flagged_rate_pct,
    ROUND(SUM(f.amount_usd) * 100.0
        / SUM(SUM(f.amount_usd)) OVER (), 2)        AS pct_of_total_volume
FROM #flagged_base f
JOIN dim_location l ON f.location_key = l.location_key
GROUP BY l.country, l.is_high_risk_country
ORDER BY flagged_count DESC;


/********************************************************************************************
 DELIVERABLE 5 — FRAUD RISK SCORING MODEL (COMPOSITE WATCHLIST)

 SCORING:
     Signal 1 — Off-hours rate          25 pts max
     Signal 2 — Spend spike             25 pts max
     Signal 3 — Shell merchant rate     20 pts max
     Signal 4 — High-risk country rate  20 pts max
     Signal 5 — KYC status             10 pts max
     TOTAL                             100 pts

 RISK TIERS:  >= 70 CRITICAL | 50-69 HIGH | 30-49 MEDIUM | < 30 LOW

 THRESHOLDS:
     Off-hours   : >= 40 → 25 | >= 25 → 18 | >= 10 → 10 | else 3
     Spend spike : >= 10 → 25 | >= 5  → 18 | >= 2  → 10 | else 2 | null → 0
     Shell       : >= 50 → 20 | >= 25 → 13 | >= 10 → 7  | else 1
     Geo risk    : >= 50 → 20 | >= 20 → 13 | >= 5  → 6  | else 0
     KYC         : Expired → 10 | Pending → 5 | else 0
*********************************************************************************************/

-- 5.1  Full Watchlist
-- RESULT: 32 CRITICAL | 62 HIGH | 165 MEDIUM | 7,688 LOW
--         Harry Wike and Aisha Hughes score 100/100 — all 5 signals at maximum weight.
-- Results materialised into #watchlist for reuse in 5.2

DROP TABLE IF EXISTS #watchlist;

WITH
off_hours_signal AS (
    SELECT
        customer_key,
        COUNT(*)                                    AS total_txns,
        SUM(is_off_hours_int)                       AS off_hours_txns,
        ROUND(SUM(is_off_hours_int) * 100.0
            / NULLIF(COUNT(*), 0), 2)               AS off_hours_rate_pct
    FROM #flagged_base
    GROUP BY customer_key
),
spend_spike_signal AS (
    SELECT
        customer_key,
        ROUND(AVG(CASE WHEN transaction_date < '2024-05-01'
                       THEN amount_usd END), 2)     AS early_avg_spend,
        ROUND(AVG(CASE WHEN transaction_date >= '2024-05-01'
                       THEN amount_usd END), 2)     AS late_avg_spend,
        ROUND(
            AVG(CASE WHEN transaction_date >= '2024-05-01'
                     THEN amount_usd END)
            / NULLIF(AVG(CASE WHEN transaction_date < '2024-05-01'
                              THEN amount_usd END), 0)
        , 2)                                        AS spend_multiplier
    FROM #flagged_base
    GROUP BY customer_key
    HAVING COUNT(CASE WHEN transaction_date <  '2024-05-01' THEN 1 END) >= 3
       AND COUNT(CASE WHEN transaction_date >= '2024-05-01' THEN 1 END) >= 3
),
shell_signal AS (
    SELECT
        f.customer_key,
        ROUND(SUM(CAST(m.is_shell_merchant AS INT)) * 100.0
            / NULLIF(COUNT(*), 0), 2)               AS shell_rate_pct
    FROM #flagged_base f
    JOIN dim_merchant m ON f.merchant_key = m.merchant_key
    GROUP BY f.customer_key
),
geo_signal AS (
    SELECT
        f.customer_key,
        ROUND(SUM(CAST(l.is_high_risk_country AS INT)) * 100.0
            / NULLIF(COUNT(*), 0), 2)               AS high_risk_country_rate_pct
    FROM #flagged_base f
    JOIN dim_location l ON f.location_key = l.location_key
    GROUP BY f.customer_key
),
scored AS (
    SELECT
        o.customer_key,
        c.full_name,
        c.country,
        c.kyc_status,
        c.customer_segment,
        o.off_hours_rate_pct,
        ss.spend_multiplier,
        sh.shell_rate_pct,
        g.high_risk_country_rate_pct,
        CASE
            WHEN o.off_hours_rate_pct >= 40 THEN 25
            WHEN o.off_hours_rate_pct >= 25 THEN 18
            WHEN o.off_hours_rate_pct >= 10 THEN 10
            ELSE 3
        END                                         AS score_off_hours,
        CASE
            WHEN ss.spend_multiplier IS NULL THEN 0
            WHEN ss.spend_multiplier >= 10   THEN 25
            WHEN ss.spend_multiplier >=  5   THEN 18
            WHEN ss.spend_multiplier >=  2   THEN 10
            ELSE 2
        END                                         AS score_spend_spike,
        CASE
            WHEN sh.shell_rate_pct >= 50 THEN 20
            WHEN sh.shell_rate_pct >= 25 THEN 13
            WHEN sh.shell_rate_pct >= 10 THEN  7
            ELSE 1
        END                                         AS score_shell_merchant,
        CASE
            WHEN g.high_risk_country_rate_pct >= 50 THEN 20
            WHEN g.high_risk_country_rate_pct >= 20 THEN 13
            WHEN g.high_risk_country_rate_pct >=  5 THEN  6
            ELSE 0
        END                                         AS score_geo_risk,
        CASE
            WHEN c.kyc_status = 'Expired' THEN 10
            WHEN c.kyc_status = 'Pending' THEN  5
            ELSE 0
        END                                         AS score_kyc
    FROM off_hours_signal o
    JOIN  dim_customer         c  ON o.customer_key = c.customer_key
    LEFT JOIN spend_spike_signal ss ON o.customer_key = ss.customer_key
    LEFT JOIN shell_signal       sh ON o.customer_key = sh.customer_key
    LEFT JOIN geo_signal          g ON o.customer_key = g.customer_key
),
watchlist AS (
    SELECT
        *,
        score_off_hours + score_spend_spike
        + score_shell_merchant + score_geo_risk
        + score_kyc                                 AS total_risk_score,
        CASE
            WHEN score_off_hours + score_spend_spike
               + score_shell_merchant + score_geo_risk
               + score_kyc >= 70                   THEN 'CRITICAL'
            WHEN score_off_hours + score_spend_spike
               + score_shell_merchant + score_geo_risk
               + score_kyc >= 50                   THEN 'HIGH'
            WHEN score_off_hours + score_spend_spike
               + score_shell_merchant + score_geo_risk
               + score_kyc >= 30                   THEN 'MEDIUM'
            ELSE                                        'LOW'
        END                                         AS risk_tier,
        ROUND(PERCENT_RANK() OVER (
            ORDER BY score_off_hours + score_spend_spike
                   + score_shell_merchant + score_geo_risk + score_kyc
        ) * 100, 1)                                 AS risk_percentile
    FROM scored
)
SELECT
    customer_key, full_name, country, kyc_status, customer_segment,
    off_hours_rate_pct, spend_multiplier, shell_rate_pct,
    high_risk_country_rate_pct, score_off_hours, score_spend_spike,
    score_shell_merchant, score_geo_risk, score_kyc,
    total_risk_score, risk_tier, risk_percentile
INTO #watchlist
FROM watchlist
ORDER BY total_risk_score DESC;

SELECT * FROM #watchlist;


-- 5.2  Risk Tier Summary
-- Reads directly from #watchlist — all 5 signals, consistent with 5.1

SELECT
    risk_tier,
    COUNT(*)                                        AS customer_count,
    ROUND(COUNT(*) * 100.0
        / SUM(COUNT(*)) OVER (), 2)                 AS pct_of_customers,
    ROUND(AVG(CAST(total_risk_score AS FLOAT)), 1)  AS avg_score,
    MIN(total_risk_score)                           AS min_score,
    MAX(total_risk_score)                           AS max_score
FROM #watchlist
GROUP BY risk_tier
ORDER BY CASE risk_tier
    WHEN 'CRITICAL' THEN 1 WHEN 'HIGH'   THEN 2
    WHEN 'MEDIUM'   THEN 3 ELSE 4 END;


/********************************************************************************************
 DELIVERABLE 6 — EXECUTIVE RISK REPORT & RECOMMENDATIONS
*********************************************************************************************/

-- 6.1  Total Fraud Exposure
-- RESULT: 19,741 flagged transactions | $267.3M exposure | 10.11% fraud rate | 75.3% of portfolio
-- Results materialised into #exec_summary for reuse in 6.5

DROP TABLE IF EXISTS #exec_summary;
SELECT
    COUNT(*)                                        AS total_transactions,
    SUM(f.is_flagged_int)                           AS flagged_transactions,
    ROUND(SUM(f.is_flagged_int) * 100.0
        / NULLIF(COUNT(*), 0), 2)                   AS fraud_rate_pct,
    ROUND(SUM(f.amount_usd), 2)                     AS total_portfolio_volume_usd,
    ROUND(SUM(CASE WHEN f.is_flagged_int = 1
                   THEN f.amount_usd ELSE 0 END), 2) AS estimated_fraud_exposure_usd,
    ROUND(SUM(CASE WHEN f.is_flagged_int = 1
                   THEN f.amount_usd ELSE 0 END) * 100.0
        / NULLIF(SUM(f.amount_usd), 0), 2)          AS exposure_as_pct_of_portfolio
INTO #exec_summary
FROM #flagged_base f;

SELECT * FROM #exec_summary;


-- 6.2  Fraud Exposure by Signal Type
-- RESULT: Account Takeover $99.5M (37.2%) | Shell Merchant $91M (34%) | top 2 = 71% of exposure

SELECT
    fraud_type,
    COUNT(*)                                        AS flagged_transactions,
    COUNT(DISTINCT customer_key)                    AS customers_affected,
    ROUND(SUM(amount_usd), 2)                       AS estimated_exposure_usd,
    ROUND(AVG(amount_usd), 2)                       AS avg_flagged_txn_value,
    ROUND(SUM(amount_usd) * 100.0
        / SUM(SUM(amount_usd)) OVER (), 2)          AS pct_of_total_exposure
FROM #flagged_base
WHERE is_flagged_int = 1
GROUP BY fraud_type
ORDER BY estimated_exposure_usd DESC;


-- 6.3  Fraud Trend by Month
-- RESULT: Jan–Apr ~$10M/month. May spikes 240% to $36M. Peaks at $50M in July.
--         Fraud established and sustained — not a one-off event.
-- MoM LAG materialised once in inner CTE

WITH monthly_fraud AS (
    SELECT
        d.year_number,
        d.month_number,
        d.month_name,
        d.quarter_name,
        COUNT(*)                                    AS flagged_transactions,
        COUNT(DISTINCT f.customer_key)              AS unique_affected_customers,
        ROUND(SUM(f.amount_usd), 2)                 AS monthly_exposure_usd
    FROM #flagged_base f
    JOIN #date_spine d ON f.date_key = d.date_key
    WHERE f.is_flagged_int = 1
    GROUP BY
        d.year_number, d.month_number,
        d.month_name,  d.quarter_name
),
with_lag AS (
    SELECT *,
        LAG(monthly_exposure_usd) OVER (
            ORDER BY year_number, month_number
        )                                           AS prev_month_exposure
    FROM monthly_fraud
)
SELECT
    month_name,
    quarter_name,
    flagged_transactions,
    unique_affected_customers,
    monthly_exposure_usd,
    ROUND(monthly_exposure_usd - prev_month_exposure, 2)
                                                    AS mom_exposure_change_usd,
    ROUND((monthly_exposure_usd - prev_month_exposure) * 100.0
        / NULLIF(prev_month_exposure, 0), 2)        AS mom_change_pct
FROM with_lag
ORDER BY year_number, month_number;


-- 6.4  Top 25 Highest-Risk Accounts — Freeze List
-- RESULT: All 25 accounts hit FREEZE IMMEDIATELY. Flagged rates 85–100%.
--         Notable: one Closed account still transacting — points to a system control failure.

WITH account_exposure AS (
    SELECT
        f.customer_key,
        f.account_key,
        c.full_name,
        c.country,
        c.kyc_status,
        a.account_id,
        a.account_type,
        a.account_status,
        a.current_balance,
        COUNT(*)                                    AS total_transactions,
        SUM(f.is_flagged_int)                       AS flagged_transactions,
        ROUND(SUM(f.is_flagged_int) * 100.0
            / NULLIF(COUNT(*), 0), 2)               AS flagged_rate_pct,
        ROUND(SUM(CASE WHEN f.is_flagged_int = 1
                       THEN f.amount_usd ELSE 0 END), 2)
                                                    AS flagged_exposure_usd,
        MAX(f.transaction_date)                     AS last_transaction_date
    FROM #flagged_base f
    JOIN dim_customer c ON f.customer_key = c.customer_key
    JOIN dim_account  a ON f.account_key  = a.account_key
    GROUP BY
        f.customer_key, f.account_key,
        c.full_name, c.country, c.kyc_status,
        a.account_id, a.account_type,
        a.account_status, a.current_balance
    HAVING SUM(f.is_flagged_int) >= 10
)
SELECT TOP 25
    account_id,
    full_name,
    country,
    kyc_status,
    account_type,
    account_status,
    ROUND(current_balance, 2)                       AS current_balance_usd,
    total_transactions,
    flagged_transactions,
    flagged_rate_pct,
    flagged_exposure_usd,
    last_transaction_date,
    CASE
        WHEN flagged_rate_pct >= 70
         AND flagged_exposure_usd >= 50000          THEN 'FREEZE IMMEDIATELY'
        WHEN flagged_rate_pct >= 50
          OR flagged_exposure_usd >= 25000          THEN 'ESCALATE TO COMPLIANCE'
        WHEN flagged_rate_pct >= 30                 THEN 'ENHANCED MONITORING'
        ELSE                                             'FLAG FOR REVIEW'
    END                                             AS recommended_action
FROM account_exposure
ORDER BY flagged_exposure_usd DESC;


-- 6.5  Strategic Recommendations Summary
-- RESULT: 268 accounts recommended for freeze across the full portfolio.
-- Sourced from #exec_summary — fact_transactions not re-scanned

SELECT 'TOTAL TRANSACTIONS REVIEWED'       AS metric,
       CAST(total_transactions AS NVARCHAR) AS value
FROM #exec_summary

UNION ALL

SELECT 'TOTAL FRAUD EXPOSURE (USD)',
       '$' + FORMAT(estimated_fraud_exposure_usd, 'N0')
FROM #exec_summary

UNION ALL

SELECT 'FRAUD RATE (%)',
       CAST(fraud_rate_pct AS NVARCHAR) + '%'
FROM #exec_summary

UNION ALL

SELECT 'HIGHEST RISK CHANNEL',
       channel
FROM (
    SELECT TOP 1 channel
    FROM #flagged_base
    WHERE is_flagged_int = 1
    GROUP BY channel
    ORDER BY COUNT(*) DESC
) t

UNION ALL

SELECT 'HIGHEST RISK FRAUD TYPE',
       fraud_type
FROM (
    SELECT TOP 1 fraud_type
    FROM #flagged_base
    WHERE is_flagged_int = 1
    GROUP BY fraud_type
    ORDER BY SUM(amount_usd) DESC
) t

UNION ALL

SELECT 'ACCOUNTS RECOMMENDED FOR FREEZE',
       CAST(COUNT(*) AS NVARCHAR)
FROM (
    SELECT account_key
    FROM #flagged_base
    GROUP BY account_key
    HAVING SUM(is_flagged_int) * 100.0 / COUNT(*) >= 70
       AND SUM(CASE WHEN is_flagged_int = 1
                    THEN amount_usd ELSE 0 END) >= 50000
) t;


-- CLEANUP
DROP TABLE IF EXISTS #flagged_base;
DROP TABLE IF EXISTS #portfolio_stats;
DROP TABLE IF EXISTS #date_spine;
DROP TABLE IF EXISTS #watchlist;
DROP TABLE IF EXISTS #exec_summary;

/********************************************************************************************
 END — OPERATION CLEARWATER
 NorthAxis Bank | Risk Intelligence Division | NAB-RI-2024-09
*********************************************************************************************/

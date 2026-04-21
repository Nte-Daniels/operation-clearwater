# Operation Clearwater: Bank Fraud Detection and Risk Intelligence

> **SQL Server | Data Warehousing | Fraud Analytics | Risk Scoring**

## The Scenario

It's Q3 2024. NorthAxis Bank's compliance hotline has just recorded a **340% spike** in fraud-related complaints. Internal audit has flagged an estimated **$2.3M in suspicious outflows** concentrated in a 6-week window. The CFO has escalated to the Board Risk Committee, who are convening in **72 hours**.

I was brought in as a Risk Intelligence Analyst with read access to the bank's Gold Layer data warehouse. The mandate was simple: investigate 195,000+ transactions, surface the anomalies, profile the high-risk accounts, and deliver a data-backed report before that board meeting.

This is the full investigation.

## What I Found

The $2.3M estimate was wrong. Not because the fraud was smaller, but because it was **much larger**.

| Metric | Value |
|---|---|
| Transactions Reviewed | 195,276 |
| Flagged Transactions | 19,741 |
| Fraud Rate | 10.11% |
| Total Portfolio Volume | $355,057,585 |
| Estimated Fraud Exposure | **$267,303,005** |
| Exposure as % of Portfolio | **75.28%** |
| Accounts Recommended for Freeze | **268** |
| Shell Merchants (100% flagged rate) | 8 |
| High-Risk Countries (100% flagged rate) | 10 |

Three quarters of every dollar processed went through a suspicious transaction. The fraud wasn't building slowly. It switched on like a light on **April 30, 2024** and never turned off.

## The Investigation

### Deliverable 1: Establishing the Baseline

Before hunting anomalies, I needed to understand what normal looked like. Mobile Banking led transaction volume at 39.9% of all transactions and 41.1% of total value. Wire Transfers dominated by raw dollar amount at $244M, nearly 69% of total portfolio volume despite being one of several transaction types.

The monthly trend told the first real story. January through April averaged around $20M per month, flat, consistent, unremarkable. Then May arrived with a **129% month-over-month jump to $46M**. June, July, and August kept climbing. The baseline was gone.

KYC status added another layer. 80.7% of customers were fully verified, but **7.98% had Expired KYC**, and those customers averaged $2,046 per transaction versus $1,799 for verified customers. A regulatory blind spot with above-average spend is exactly where fraud hides.

### Deliverable 2: Hunting the Anomalies

With the baseline set, I ran four anomaly checks.

**Off-hours activity.** Legitimate customers rarely transact at 2AM. The data confirmed this, but the fraud did not care. Off-hours transactions (1AM to 4AM) averaged **$4,897 per transaction** versus $1,295 during business hours. 3.8x the average. The fraud window was operating at night.

**Velocity checks.** I flagged customers making 5 or more transactions within a 60-minute window. The top offender had 887 rapid transaction pairs in the dataset. The same customers surfaced whether I used a self-join approach or a LAG window function. The ranking was consistent. The method just changed the count.

**Z-score outliers.** Using portfolio-level mean and standard deviation pre-computed in a temp table, I flagged every transaction more than 3 standard deviations above the mean. The top 100 results were exclusively Wire Transfers, all routed through Shell Merchants, all landing in high-risk countries. Z-scores clustered at **8.1 to 8.2**, more than 8 standard deviations above the portfolio mean. That is not noise.

**Daily spike detection.** The 7-day rolling average confirmed what the monthly trend had suggested. April 30 was the only day that crossed the SPIKE DETECTED threshold at 2.04x the rolling average. The day before the fraud window opened.

### Deliverable 3: Customer Risk Profiling

Transaction-level anomalies tell you something is wrong. Customer-level profiling tells you who.

**Account takeover signals.** I split each customer's history into an early period (January to April) and a late period (May to September) and compared their behaviour across both windows. The results were striking. The top customers showed spend multipliers of **94x to 105x**, meaning they were spending 100 times more per transaction post-May than they had in the first four months of the year. Combined with hour shifts of up to 6.7 hours and transactions spanning 20 distinct countries, this was not a change in spending habits. These were compromised accounts.

**Geographic anomalies.** I cross-referenced each customer's registered home country against their transaction origin countries, filtering for high-risk jurisdictions only. The same names kept appearing. Rotimi Mensah transacting across Venezuela, Belarus, Russia, Somalia, Iran, Myanmar, Cuba, North Korea, and Sudan simultaneously. Not one country. Nine. All starting in May 2024. All within weeks of each other.

The fraud network didn't build gradually. It activated.

### Deliverable 4: Merchant and Channel Risk Scoring

Knowing who was fraudulent wasn't enough. I needed to know where the money was flowing.

**Shell merchants.** Eight merchants showed a **100% flagged transaction rate**, meaning every single transaction through them was fraudulent. PrimeFin Corp registered in Iran. ClearPath Remit in Sudan. SwiftFunds Inc in Syria. GlobalTrade Ltd in North Korea. These were not legitimate businesses being exploited. They were the mechanism.

The drop-off after rank 8 was immediate. Legitimate merchants sat below 4% flagged rates. There was no grey area. The shell merchant population was completely isolated from the legitimate one.

**Channel risk.** Mobile Banking carried the highest number of flagged transactions at 14.4% of its total. Web Banking carried the highest flagged volume, with **80.64% of its total dollar value** being suspicious. The fraud was entirely digital. ATM sat at the bottom at 2.51% flagged rate, the cash-out exit used after the money had already moved.

**High-risk country exposure.** All 10 high-risk countries in the dataset (Venezuela, Belarus, North Korea, Myanmar, Russia, Cuba, Iran, Syria, Somalia, Sudan) showed a **100% flagged transaction rate**. Not a single legitimate transaction touched any of them. These 10 countries represented 42% of total portfolio volume.

### Deliverable 5: Composite Risk Scoring Model

With all signals established, I built a rule-based composite scoring model to rank every customer in the portfolio by fraud risk. Five signals, each weighted by severity.

| Signal | Max Points |
|---|---|
| Off-hours transaction rate | 25 |
| Spend spike vs early-period baseline | 25 |
| Shell merchant transaction rate | 20 |
| High-risk country transaction rate | 20 |
| KYC compliance status | 10 |
| **Total** | **100** |

Risk tiers: **CRITICAL >= 70 | HIGH 50 to 69 | MEDIUM 30 to 49 | LOW < 30**

Out of 7,947 customers:

| Tier | Customers | % of Portfolio |
|---|---|---|
| CRITICAL | 32 | 0.40% |
| HIGH | 62 | 0.78% |
| MEDIUM | 165 | 2.08% |
| LOW | 7,688 | 96.74% |

Harry Wike and Aisha Hughes both scored **100/100**, every signal firing at maximum weight. 94 accounts sitting in CRITICAL or HIGH combined is a manageable freeze list for a compliance team to action within 24 hours.

### Deliverable 6: Executive Risk Report

The board summary came down to six numbers:

| Metric | Value |
|---|---|
| Total Transactions Reviewed | 195,276 |
| Total Fraud Exposure | $267,303,005 |
| Fraud Rate | 10.11% |
| Highest Risk Channel | Mobile Banking |
| Highest Risk Fraud Type | Account Takeover |
| Accounts Recommended for Freeze | 268 |

**Account Takeover** was the most expensive attack vector at $99.5M, representing 37.23% of total exposure despite affecting only 50 customers. Average transaction value of $26,288. These were targeted, high-value hits.

The freeze list told its own story. All 25 top-risk accounts hit the FREEZE IMMEDIATELY threshold. Flagged rates ranged from 85% to 100%. One account was marked as **Closed** in the system but was still generating active transactions, pointing to a system control failure sitting underneath the fraud.

## Technical Architecture

### Data Warehouse Schema

```
fact_transactions     195,276 rows | core transaction ledger
dim_customer          customer profiles, KYC status, segments
dim_account           account types, balances, status
dim_merchant          merchant categories, shell flags, risk ratings
dim_location          country, region, high-risk flag
dim_date              full date spine with week/month/quarter fields
```

### Foundation Layer (Run First)

Three temp tables are materialised once at the start and referenced throughout:

```sql
#flagged_base      -- fact_transactions with is_flagged and is_off_hours pre-cast as INT
#portfolio_stats   -- portfolio mean, std dev, total flagged, computed once
#date_spine        -- date dimension subset joined repeatedly across deliverables
```

Two additional temp tables are created during execution:

```sql
#watchlist         -- full scored customer watchlist (D5), shared by 5.1 and 5.2
#exec_summary      -- fraud exposure totals (D6), shared by 6.1 and 6.5
```

This pattern eliminates repeated full-table scans and keeps the scoring logic consistent across queries that depend on the same results.

## How to Run

**Prerequisites**
- Microsoft SQL Server 2019+
- The NorthAxis Bank Gold Layer schema loaded
- SSMS or any SQL Server-compatible client

**Execution**

```sql
-- Step 1: Run the FOUNDATION block to materialise temp tables
-- Step 2: Run each deliverable independently, or execute the full script as a batch
-- Step 3: The CLEANUP block at the end drops all temp tables
```

Each deliverable is self-contained and labelled. You can run any single section after running the Foundation block first.

## Key SQL Techniques

**Z-Score Outlier Detection**
Portfolio mean and standard deviation are pre-computed once in `#portfolio_stats` and joined via CROSS JOIN. No repeated aggregation across the fact table.

**Velocity Check: Two Approaches**
Query 2.2 ships with both a self-join version (teaching, shows the logic explicitly, O(n²)) and a LAG window function version (production, O(n), scales to any volume). The same top offenders surface in both. The tradeoff is documented in the script.

**Composite Risk Scoring**
Five independent signal CTEs feed a single `scored` CTE. Scoring thresholds are defined once, so updating in one place automatically flows through all downstream tiers.

**MoM Change via Nested CTE**
LAG values are materialised once in an inner CTE and referenced in the outer SELECT, avoiding the double-evaluation pattern that appears in many MoM calculations.

**Temp Table Strategy**
Results that are referenced more than once are written to temp tables rather than recomputed. This keeps the script fast and ensures consistency across queries that share inputs.

## Tools and Environment

| Tool | Purpose |
|---|---|
| Microsoft SQL Server 2019 | Query execution environment |
| SSMS | Development and testing |
| Gold Layer Data Warehouse | Star schema, fact + 5 dimensions |

*NorthAxis Bank | Risk Intelligence Division | NAB-RI-2024-09*

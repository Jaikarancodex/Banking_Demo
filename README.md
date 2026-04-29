
<div style="background: linear-gradient(135deg, #0D1117 0%, #161B22 100%); padding: 40px; border-radius: 12px; border: 1px solid #30363D; margin-bottom: 20px;">

<h1 style="text-align: center; color: #E6EDF3; margin-bottom: 5px;"> Banking — Suspicious Activity Detection</h1>
<p style="text-align: center; color: #8B949E; font-size: 16px; margin-top: 0;">End-to-End Risk Intelligence & Explainability on Databricks Lakehouse</p>

</div>

---

## Business Use Case

Banks process **millions of transactions daily** across multiple channels — ATMs, online banking, mobile apps, POS terminals, and wire transfers. Hidden within this volume are **suspicious patterns** that signal money laundering, account takeover, identity theft, and payment manipulation.

Traditional rule-based systems catch obvious cases but miss sophisticated schemes. Modern banking requires a platform that can:

> **Ingest** transactions in real-time → **Detect** suspicious behavior using ML + rules → **Score** risk dynamically → **Explain** alerts to analysts → **Prioritize** cases for investigation

This demo simulates a **production banking environment** where:

- Transactions stream into Databricks via Kafka / IoT-style feeds
- A **suspicious pattern appears** — e.g., multiple small deposits (\$900, \$850, \$950) followed by one large withdrawal (\$3,500) — classic **structuring behavior**
- The **ML model flags the anomaly** — velocity features spike, amount z-score exceeds 2.8σ
- The **risk score increases dynamically** from Low to Critical
- The **system generates an alert** routed to the analyst queue
- **GenAI explains** the alert: *"This account shows structuring behavior — 5 deposits under reporting threshold followed by rapid consolidation withdrawal. Velocity 5x above normal. Cross-channel activity in 4 hours."*
- The **Dashboard** shows high-risk accounts, alert queues, and per-transaction explanations

---

## The Problem We Are Solving

<table style="width:100%; border-collapse: collapse;">
<tr style="border-bottom: 2px solid #30363D;">
<td style="padding: 15px; width: 5%;"><h3>⚠️</h3></td>
<td style="padding: 15px; width: 30%;"><strong>Detection Gap</strong></td>
<td style="padding: 15px;">Banks cannot detect suspicious transaction patterns in real-time. Legacy batch systems process transactions hours or days later — by then, funds have already moved. Sophisticated schemes like structuring, layering, and rapid cross-channel movement evade static rules entirely.</td>
</tr>
<tr style="border-bottom: 2px solid #30363D;">
<td style="padding: 15px;"><h3>🚨</h3></td>
<td style="padding: 15px;"><strong>Alert Fatigue</strong></td>
<td style="padding: 15px;">Rule-only systems generate <strong>90%+ false positive rates</strong>. Analysts waste time investigating legitimate transactions while real suspicious activity gets buried. This leads to missed cases, regulatory fines, and burnout.</td>
</tr>
<tr style="border-bottom: 2px solid #30363D;">
<td style="padding: 15px;"><h3>🔍</h3></td>
<td style="padding: 15px;"><strong>Explainability Deficit</strong></td>
<td style="padding: 15px;">Black-box ML models flag transactions but <strong>cannot explain why</strong>. Regulators (GDPR Article 22, SR 11-7, BSA/AML) require banks to justify every decision. Without explainability, models cannot be deployed in production.</td>
</tr>
<tr>
<td style="padding: 15px;"><h3>📊</h3></td>
<td style="padding: 15px;"><strong>Investigation Bottleneck</strong></td>
<td style="padding: 15px;">Analysts lack a unified view of customer risk, transaction history, alert context, and case status. They toggle between 5-10 systems. No prioritization means critical cases wait in the same queue as low-risk noise.</td>
</tr>
</table>

---

## Key Performance Indicators (KPIs)

These are the metrics this platform tracks to measure detection effectiveness, operational efficiency, and compliance readiness:

### Detection & Risk KPIs

| KPI | Definition | Target | Source Table |
|---|---|---|---|
| **Suspicious Activity Rate** | % of total transactions flagged as suspicious | < 5% (lower = better precision) | `gold_daily_fraud_summary` |
| **High-Risk Account Count** | Customers with `is_high_risk = true` | Monitor trend (should decrease over time) | `gold_customer_360` |
| **Risk Tier Distribution** | Breakdown of Critical / High / Medium / Low tiers | Majority should be Low | `gold_fraud_predictions` |
| **Daily Suspicious Volume** | Count of flagged transactions per day by channel | Track for spikes and seasonality | `gold_daily_fraud_summary` |

### Operational Efficiency KPIs

| KPI | Definition | Target | Source Table |
|---|---|---|---|
| **False Positive Rate** | Alerts confirmed as legitimate / total alerts | < 50% (industry avg: 90%+) | `gold_alert_rule_performance` |
| **Alert-to-Case Ratio** | System alerts generated vs confirmed suspicious cases | < 10:1 | `gold_alert_rule_performance` |
| **Mean Time to Detect (MTTD)** | Hours from suspicious activity to system detection | < 1 hour (real-time goal) | `silver_fraud_labels` |
| **Mean Time to Resolve (MTTR)** | Hours from alert creation to analyst resolution | < 24 hours | `gold_alert_rule_performance` |
| **Rule Performance Score** | Per-rule true positive rate and false positive rate | Retire rules with FP > 70% | `gold_alert_rule_performance` |

### Financial Impact KPIs

| KPI | Definition | Target | Source Table |
|---|---|---|---|
| **Total Loss Amount** | Sum of confirmed losses from suspicious activity | Minimize (\$0 ideal) | `silver_fraud_labels` |
| **Loss Recovery Rate** | `recovered_amount / loss_amount` | > 60% | `silver_fraud_labels` |
| **Loss by Activity Type** | Breakdown of losses by category (structuring, account takeover, etc.) | Identify top contributors | `silver_fraud_labels` |

### Compliance & Governance KPIs

| KPI | Definition | Target | Source Table |
|---|---|---|---|
| **Compliance Flag Rate** | % of audit actions flagged for compliance review | Track and review all flags | `silver_audit_logs` |
| **Explainability Coverage** | % of flagged transactions with SHAP explanations | 100% (regulatory requirement) | `gold_shap_explanations` |
| **Audit Trail Completeness** | All investigator actions logged with timestamps | 100% coverage | `silver_audit_logs` |

---

*The sections below provide the full technical deep-dive: architecture, data model, table catalog, pipeline notebooks, how to run, ML model details, explainability, and dashboard configuration.*

<div style="text-align: center; padding: 30px 0;">

# BANKING SUSPICIOUS ACTIVITY DETECTION & EXPLAINABILITY PLATFORM
### Production Demo on Databricks Lakehouse

**Catalog:** `banking_demo` &nbsp;|&nbsp; **Schemas:** `bronze` · `silver` · `gold` &nbsp;|&nbsp; **Runtime:** Apr 2025 – Mar 2026

---

*Ingest transactions in real-time · Detect suspicious behavior using ML + Rules · Score risk dynamically · Explain alerts · Prioritize cases for investigation*

</div>

---

## Table of Contents

1. [Problem Statement](#problem-statement)
2. [What This Demo Shows](#what-this-demo-shows)
3. [Solution Architecture](#solution-architecture)
4. [Data Model](#data-model)
5. [Table Catalog](#table-catalog)
6. [Pipeline Notebooks](#pipeline-notebooks)
7. [How to Run](#how-to-run)
8. [Demo Flow — Live Walkthrough](#demo-flow--live-walkthrough)
9. [ML Model & Risk Scoring](#ml-model--risk-scoring)
10. [Explainability (XAI)](#explainability-xai)
11. [Dashboard & GenAI Assistant](#dashboard--genai-assistant)
12. [Key Business Metrics](#key-business-metrics)
13. [Technical Notes](#technical-notes)

---

## 1. Problem Statement

Banks today struggle with three critical challenges:

| Challenge | Pain Point | Business Impact |
|---|---|---|
| **Detect** suspicious transaction patterns in real-time | Legacy batch systems miss fast-moving anomalies | Revenue loss, regulatory penalties, reputational damage |
| **Reduce** false alerts flooding analyst queues | Rule-only systems generate 90%+ false positives | Analyst fatigue, poor customer experience, wasted resources |
| **Explain** why a transaction was flagged | Black-box models fail regulatory scrutiny (GDPR, SR 11-7, BSA/AML) | Compliance risk, audit failures, inability to defend decisions |

---

## 2. What This Demo Shows

This platform demonstrates an **end-to-end suspicious activity detection and explainability solution** built entirely on the Databricks Lakehouse:

| Capability | Implementation |
|---|---|
| **Ingest transactions in real-time** | Streaming ingestion via Kafka / simulated IoT-style feeds into Delta Lake |
| **Detect suspicious behavior using ML + rules** | XGBoost anomaly detection model combined with configurable rule engine |
| **Score risk dynamically** | 61-feature pipeline computing velocity, anomaly z-scores, and composite risk in near-real-time |
| **Explain alerts with Dashboard** | SHAP-based per-transaction explanations surfaced in a 4-page Risk Intelligence Control Tower |
| **Prioritize cases for investigation** | Risk tiers (Critical/High/Medium/Low) auto-route to analyst queues |
| **GenAI assistant** | Genie Space for natural-language investigation queries ("Show me all structuring patterns this week") |

---

## 3. Solution Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                         DATABRICKS LAKEHOUSE PLATFORM                                   │
│                         Unity Catalog: banking_demo                                     │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                         │
│  ┌──────────────┐    ┌──────────────────┐    ┌──────────────────┐    ┌───────────────┐  │
│  │  INGESTION   │    │  BRONZE (Raw)    │    │  SILVER (Clean)  │    │  GOLD (Curated│  │
│  │              │    │  Schema: bronze   │    │  Schema: silver  │    │  Schema: gold) │  │
│  │  Streaming   │───▶│                  │───▶│                  │───▶│               │  │
│  │  Kafka /     │    │  7 raw tables    │    │  7 silver tables │    │  7 gold tables│  │
│  │  Simulated   │    │  As-ingested     │    │  Type-cast       │    │  Aggregated   │  │
│  │  IoT-style   │    │  Schema-on-read  │    │  Deduplicated    │    │  Feature-eng  │  │
│  │  feeds       │    │                  │    │  Validated       │    │  ML-ready     │  │
│  └──────────────┘    └──────────────────┘    └──────────────────┘    └───────┬───────┘  │
│                                                                             │           │
│                       ┌─────────────────────────────────────────────────────┤           │
│                       │                                                     │           │
│                       ▼                                                     ▼           │
│  ┌────────────────────────────────────┐    ┌────────────────────────────────────────┐   │
│  │  ML & ANOMALY DETECTION            │    │  ANALYTICS & SERVING                   │   │
│  │                                    │    │                                        │   │
│  │  ┌──────────────┐  ┌────────────┐  │    │  ┌─────────────────┐  ┌─────────────┐  │   │
│  │  │  XGBoost     │  │   SHAP     │  │    │  │  Dashboard:     │  │ Genie Space │  │   │
│  │  │  Anomaly     │──│  Explainer │  │    │  │  Risk Intel     │  │ GenAI       │  │   │
│  │  │  Detector    │  │            │  │    │  │  Control Tower  │  │ Natural     │  │   │
│  │  │              │  │  Per-txn   │  │    │  │  4 pages        │  │ Language    │  │   │
│  │  │  Features:61 │  │  drivers   │  │    │  │                 │  │ Queries     │  │   │
│  │  │  MLflow+UC   │  │            │  │    │  │                 │  │             │  │   │
│  │  └──────────────┘  └────────────┘  │    │  └─────────────────┘  └─────────────┘  │   │
│  │                                    │    │                                        │   │
│  │  Output: risk_predictions          │    │  Job: Banking_Demo_Pipeline            │   │
│  │          shap_explanations         │    │  6 sequential tasks, single-node       │   │
│  └────────────────────────────────────┘    └────────────────────────────────────────┘   │
│                                                                                         │
│  ┌─────────────────────────────────────────────────────────────────────────────────────┐ │
│  │  GOVERNANCE: Unity Catalog (lineage, access control, tagging, audit)               │ │
│  └─────────────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                         │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

### Architecture Components

| Layer | Technology | Purpose |
|---|---|---|
| **Ingestion** | Streaming (Kafka / simulated IoT-style) | Real-time transaction feed into Delta Lake |
| **Storage** | Delta Lake (Bronze / Silver / Gold) | ACID-compliant medallion architecture |
| **Processing** | Structured Streaming + Spark | Type casting, dedup, feature engineering |
| **ML Layer** | XGBoost + SHAP | Anomaly detection model + network/graph-based risk scoring |
| **Governance** | Unity Catalog | Lineage, access control, tagging, audit trail |
| **Serving** | Dashboard + Genie Space | Investigation app, alert management, GenAI assistant |

### Data Flow Summary

```
00_Data Generation          01_bronze_to_silver         02_Silver to Gold
┌───────────────┐           ┌───────────────┐           ┌───────────────┐
│ Simulate      │           │ Type casting  │           │ Star schema   │
│ streaming     │──────────▶│ Deduplication │──────────▶│ Aggregations  │
│ transactions  │           │ Null handling │           │ Customer 360  │
│ ~20K records  │           │ Validation    │           │ Daily summary │
└───────────────┘           └───────────────┘           └───────┬───────┘
                                                                │
                    ┌───────────────────────────────────────────┘
                    │
                    ▼
03_Feature Engineering      04_MLModel                  05_Dashboard & Genie
┌───────────────┐           ┌───────────────┐           ┌───────────────┐
│ 61 features   │           │ XGBoost train │           │ SQL views for │
│ Velocity      │──────────▶│ MLflow log    │──────────▶│ dashboard     │
│ Anomaly score │           │ SHAP values   │           │ Genie Space   │
│ Composite risk│           │ Risk tiers    │           │ GenAI config  │
└───────────────┘           └───────────────┘           └───────────────┘
```

---

## 4. Data Model

### Entity-Relationship Diagram

```
                                    ┌───────────────────────┐
                                    │      customers        │
                                    │  (PK) customer_id     │
                                    │  full_name, email     │
                                    │  country, segment     │
                                    │  kyc_status           │
                                    │  risk_score           │
                                    │  is_pep, is_sanctioned│
                                    └──────────┬────────────┘
                                               │
                                          1 ── ┤ ── N
                                               │
                              ┌────────────────┴────────────────┐
                              │           accounts              │
                              │  (PK) account_id               │
                              │  (FK) customer_id               │
                              │  account_type, balance          │
                              │  credit_limit, currency         │
                              │  status, branch_code            │
                              │  is_joint, open_date            │
                              └────────────────┬────────────────┘
                                               │
                                          1 ── ┤ ── N
                                               │
┌───────────────────────┐     ┌────────────────┴────────────────┐     ┌───────────────────────┐
│      merchants        │     │         transactions            │     │  suspicious_case_labels│
│  (PK) merchant_id     │     │  (PK) transaction_id            │     │  (PK) case_id          │
│  merchant_name        │ N──▶│  (FK) account_id                │◀──1 │  (FK) transaction_id   │
│  mcc_code             │  1  │  (FK) merchant_id               │     │  case_type             │
│  mcc_description      │     │  amount, currency, channel      │     │  case_status           │
│  country, risk_level  │     │  txn_datetime, txn_date         │     │  loss_amount           │
│  avg_txn_amount       │     │  transaction_type, status       │     │  recovered_amount      │
│  is_blacklisted       │     │  device_id, ip_address          │     │  investigator_id       │
│  registered_date      │     │  is_international               │     │  detected_datetime     │
└───────────────────────┘     │  counterparty_id                │     │  confirmed_datetime    │
                              │  response_code, txn_hour        │     │  reported_by, notes    │
                              └────────────────┬────────────────┘     └────────────────────────┘
                                               │
                                          1 ── ┤ ── N
                                               │
                              ┌────────────────┴────────────────┐
                              │            alerts               │
                              │  (PK) alert_id                 │
                              │  (FK) transaction_id            │
                              │  rule_id, alert_type            │
                              │  severity, score                │
                              │  alert_status, explanation      │
                              │  false_positive, analyst_id     │
                              │  created_datetime               │
                              │  resolved_datetime              │
                              └─────────────────────────────────┘

                              ┌─────────────────────────────────┐
                              │         audit_logs              │
                              │  (PK) log_id                   │
                              │  entity_id, entity_type         │
                              │  action, actor_id, actor_type   │
                              │  status, compliance_flag        │
                              │  change_summary, ip_address     │
                              │  session_id, log_datetime       │
                              └─────────────────────────────────┘
```

### Key Relationships

| FK Relationship | Join Condition |
|---|---|
| `accounts` → `customers` | `accounts.customer_id = customers.customer_id` |
| `transactions` → `accounts` | `transactions.account_id = accounts.account_id` |
| `transactions` → `merchants` | `transactions.merchant_id = merchants.merchant_id` |
| `case_labels` → `transactions` | `case_labels.transaction_id = transactions.transaction_id` |
| `alerts` → `transactions` | `alerts.transaction_id = transactions.transaction_id` |

---

## 5. Table Catalog

### Bronze Layer — `banking_demo.bronze`
> Raw ingested data from streaming feeds. No transformations applied. Schema-on-read.

| # | Table | Key Columns | Description |
|---|---|---|---|
| 1 | `customers` | `customer_id`, `full_name`, `country`, `segment`, `risk_score`, `kyc_status`, `is_pep`, `is_sanctioned` | Customer profiles with KYC and risk attributes |
| 2 | `accounts` | `account_id`, `customer_id`, `account_type`, `balance`, `credit_limit`, `status`, `branch_code` | Bank accounts linked to customers |
| 3 | `merchants` | `merchant_id`, `merchant_name`, `mcc_code`, `mcc_description`, `risk_level`, `is_blacklisted` | Merchant profiles with MCC classification |
| 4 | `transactions` | `transaction_id`, `account_id`, `merchant_id`, `amount`, `channel`, `txn_datetime`, `is_international` | Core transaction records (fact table) |
| 5 | `fraud_labels` | `fraud_id`, `transaction_id`, `fraud_type`, `fraud_status`, `loss_amount`, `recovered_amount` | Confirmed suspicious activity case labels |
| 6 | `alerts` | `alert_id`, `transaction_id`, `rule_id`, `alert_type`, `severity`, `score`, `false_positive` | System-generated alerts with rule explanations |
| 7 | `audit_logs` | `log_id`, `entity_id`, `action`, `actor_id`, `compliance_flag`, `change_summary` | Full compliance audit trail |

### Silver Layer — `banking_demo.silver`
> Cleaned, validated, type-cast. Deduplication and null handling applied. Metadata column `_silver_load_ts` added.

| # | Table | Source | Transformations Applied |
|---|---|---|---|
| 1 | `silver_customers` | `bronze.customers` | Date parsing (`date_joined` → DATE), null handling, dedup on `customer_id` |
| 2 | `silver_accounts` | `bronze.accounts` | Date parsing (`open_date` → DATE), balance validation, dedup on `account_id` |
| 3 | `silver_merchants` | `bronze.merchants` | Date parsing, risk_level standardization, dedup on `merchant_id` |
| 4 | `silver_transactions` | `bronze.transactions` | `txn_datetime` → TIMESTAMP, `txn_date` → DATE, `amount` → DOUBLE, dedup on `transaction_id` |
| 5 | `silver_fraud_labels` | `bronze.fraud_labels` | Datetime parsing, `loss_amount`/`recovered_amount` → DOUBLE, dedup |
| 6 | `silver_alerts` | `bronze.alerts` | Datetime parsing, `score` → DOUBLE, `false_positive` → BOOLEAN, dedup on `alert_id` |
| 7 | `silver_audit_logs` | `bronze.audit_logs` | Datetime parsing, `compliance_flag` → BOOLEAN, dedup on `log_id` |

### Gold Layer — `banking_demo.gold`
> Aggregated, feature-engineered, ML-ready. Star schema design.

| # | Table | Columns | Description |
|---|---|---|---|
| 1 | `gold_customer_360` | 22 cols | Customer 360 view: lifetime metrics (`lifetime_txns`, `lifetime_spend`, `high_value_txns`, `intl_txns`), risk bands, account summaries |
| 2 | `gold_transactions_enriched` | 46 cols | Enriched fact table joining transactions + accounts + customers + merchants + case_labels + alerts. Includes `is_suspicious`, `alert_count`, `loss_amount` |
| 3 | `gold_daily_fraud_summary` | 11 cols | Daily KPIs by `channel` and `account_type`: `total_txns`, suspicious count, suspicion rate, `total_loss`, `total_alerts` |
| 4 | `gold_alert_rule_performance` | 10 cols | Alert rule scorecard: `true_positives`, `false_positives`, `fp_rate`, `avg_resolution_hours`, confirmed cases per rule |
| 5 | `gold_ml_features` | 61 cols | Feature-engineered table with velocity, anomaly scores, composite risk. Partitioned by `txn_date` |
| 6 | `gold_fraud_predictions` | 4 cols | Model output: `transaction_id`, suspicion probability, prediction label, `risk_tier` (Critical/High/Medium/Low) |
| 7 | `gold_shap_explanations` | 23 cols | Per-transaction SHAP values for each feature + `top_drivers` (human-readable explanation) |

---

## 6. Pipeline Notebooks

All notebooks are located in `/Users/jaikaran.n@diggibyte.com/Banking_Demo/`

| Order | Notebook | Purpose | Inputs | Outputs |
|---|---|---|---|---|
| 0 | `00_Data Generation` | Simulate streaming transaction ingestion | None | 7 `bronze.*` tables |
| 1 | `01_bronze_to_silver` | Data quality, type casting, dedup | 7 `bronze.*` tables | 7 `silver.*` tables |
| 2 | `02_Silver to Gold` | Star schema joins, aggregations | 7 `silver.*` tables | 4 `gold.*` tables |
| 3 | `03_Feature Engineering` | Behavioral features, velocity, anomaly scores | `gold.gold_transactions_enriched` | `gold.gold_ml_features` (61 columns) |
| 4 | `04_MLModel` | XGBoost training, MLflow logging, SHAP | `gold.gold_ml_features` | Predictions, SHAP explanations, UC model |
| 5 | `05_Dashboard & Genie` | SQL view definitions for dashboard/Genie | `gold.*`, `silver.*` tables | Dashboard-ready SQL views |
| — | `06_Create Genie Space` | REST API to auto-create GenAI assistant | Config from notebook 05 | Genie Space asset |

### Feature Engineering Details (Notebook 03)

61 features organized into 5 groups:

| Feature Group | Examples | Count |
|---|---|---|
| **Transaction Base** | `amount`, `txn_hour`, `is_international`, `is_high_value`, `is_odd_hour` | 12 |
| **Velocity (Window)** | `txn_count_1h`, `txn_count_24h`, `txn_count_7d`, `unique_merchants_24h` | 15 |
| **Amount Aggregates** | `amount_sum_1h`, `amount_sum_24h`, `amount_avg_7d`, `amount_max_7d` | 12 |
| **Anomaly Scores** | `amount_zscore`, `geo_risk_score`, `channel_risk`, `composite_risk_score` | 10 |
| **Contextual** | `merchant_risk_encoded`, `account_balance`, `customer_risk_score`, `is_pep` | 12 |

---

## 7. How to Run

### Prerequisites

- **Databricks Workspace** with Unity Catalog enabled
- **Catalog** `banking_demo` must exist (or you need CREATE CATALOG permissions)
- **Compute**: Any cluster with DBR 14.3 LTS+ (Python 3.10+)
- **Libraries**: `mlflow`, `xgboost`, `shap` (auto-installed via `%pip` in notebook 04)

### Option A: Run via Lakeflow Job (Recommended)

A pre-configured job **Banking_Demo_Pipeline** (Job ID: `958717178058660`) executes all 6 notebooks in sequence.

```
Step 1:  Navigate to Jobs → Banking_Demo_Pipeline
Step 2:  Click "Run Now"
Step 3:  Monitor 6 sequential tasks:

         data_generation ──▶ bronze_to_silver ──▶ silver_to_gold
                                                       │
         feature_engineering ──▶ ml_model ──▶ dashboard_genie

Estimated Duration: ~15-25 minutes (single-node)
```

**Job Compute Configuration** (cost-optimized):

| Setting | Value |
|---|---|
| Instance Type | Standard_DS3_v2 (Azure) |
| Workers | 0 (single-node) |
| Runtime | 16.4.x-scala2.12 LTS |
| Photon | Disabled |
| Spot Instances | Enabled (with on-demand fallback) |

### Option B: Run Notebooks Individually

```bash
# Execute in order — each notebook depends on the previous

# Step 1: Simulate streaming transaction ingestion into bronze
Run → 00_Data Generation

# Step 2: Clean and validate into silver
Run → 01_bronze_to_silver

# Step 3: Build gold aggregations and star schema
Run → 02_Silver to Gold

# Step 4: Engineer 61 ML features (velocity, anomaly, composite risk)
Run → 03_Feature Engineering

# Step 5: Train anomaly detection model, generate SHAP explanations
Run → 04_MLModel

# Step 6: Set up dashboard SQL views and GenAI assistant config
Run → 05_Dashboard & Genie

# Optional: Create Genie Space via REST API
Run → 06_Create Genie Space
```

### Option C: Run via Databricks CLI

```bash
databricks jobs run-now 958717178058660
```

---

## 8. Demo Flow — Live Walkthrough

This is the recommended narrative for presenting the demo to stakeholders:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│  STEP 1: STREAM TRANSACTIONS INTO DATABRICKS                           │
│  ─────────────────────────────────────────                              │
│  Transactions arrive in real-time via simulated Kafka feeds.            │
│  They land in Delta Lake bronze tables as raw, immutable records.       │
│                                                                         │
│                              ▼                                          │
│                                                                         │
│  STEP 2: A SUSPICIOUS PATTERN APPEARS                                  │
│  ────────────────────────────────────                                   │
│  The system detects structuring behavior:                               │
│  ┌───────────────────────────────────────────────────────────┐          │
│  │  Multiple small deposits ($900, $850, $950, $800)         │          │
│  │  followed by one large withdrawal ($3,500)                │          │
│  │  across 3 different channels within 4 hours               │          │
│  └───────────────────────────────────────────────────────────┘          │
│                                                                         │
│                              ▼                                          │
│                                                                         │
│  STEP 3: ML MODEL FLAGS ANOMALY                                        │
│  ──────────────────────────────                                         │
│  XGBoost anomaly detector scores the transaction:                       │
│  • Velocity features spike (txn_count_1h = 5, normal = 1-2)           │
│  • Amount z-score = 2.8σ above customer average                        │
│  • Composite risk score = 0.87 (Critical tier)                         │
│                                                                         │
│                              ▼                                          │
│                                                                         │
│  STEP 4: RISK SCORE INCREASES DYNAMICALLY                              │
│  ────────────────────────────────────────                               │
│  The customer's risk score escalates from "Low" to "Critical"          │
│  based on the pattern. Account-level and customer-level risk           │
│  bands are recalculated in the gold_customer_360 table.                │
│                                                                         │
│                              ▼                                          │
│                                                                         │
│  STEP 5: SYSTEM GENERATES ALERT                                        │
│  ──────────────────────────────                                         │
│  An alert is created with:                                              │
│  • Severity: HIGH                                                       │
│  • Rule triggered: velocity_check + amount_anomaly                     │
│  • Auto-routed to analyst queue based on risk tier                     │
│                                                                         │
│                              ▼                                          │
│                                                                         │
│  STEP 6: GenAI EXPLAINS THE ALERT                                      │
│  ────────────────────────────────                                       │
│  SHAP-based explanation generated:                                      │
│  ┌───────────────────────────────────────────────────────────┐          │
│  │  "This account shows structuring behavior typical of      │          │
│  │   money laundering. 5 deposits under reporting threshold  │          │
│  │   followed by rapid consolidation withdrawal. Velocity    │          │
│  │   5x above normal. Cross-channel activity in 4 hours."   │          │
│  └───────────────────────────────────────────────────────────┘          │
│                                                                         │
│                              ▼                                          │
│                                                                         │
│  STEP 7: DASHBOARD SHOWS INVESTIGATION VIEW                            │
│  ───────────────────────────────────────────                            │
│  Risk Intelligence Control Tower displays:                              │
│  • High-risk accounts ranked by composite score                        │
│  • Alert queue with auto-prioritization                                │
│  • Per-transaction SHAP explanations                                    │
│  • Compliance audit trail                                               │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 9. ML Model & Risk Scoring

### Model Details

| Property | Value |
|---|---|
| **Algorithm** | XGBoost (`xgb.XGBClassifier`) — Anomaly Detection |
| **Features** | 61 engineered features from `gold_ml_features` |
| **Target** | `is_suspicious` (binary classification) |
| **Training Split** | 80/20 stratified |
| **Registry** | `banking_demo.gold.banking_fraud_detector` (Unity Catalog) |
| **Tracking** | MLflow experiment with metrics, params, artifacts |

### Risk Tier Classification

| Risk Tier | Probability Range | Automated Action |
|---|---|---|
| **Critical** | \>= 0.8 | Auto-block transaction + immediate investigation |
| **High** | 0.6 – 0.8 | Priority queue for analyst review |
| **Medium** | 0.4 – 0.6 | Standard review within 24h |
| **Low** | < 0.4 | Log and monitor |

### Model Output Tables

- **`gold_fraud_predictions`**: `transaction_id`, suspicion probability, prediction label, `risk_tier`
- **`gold_shap_explanations`**: Per-feature SHAP values + `top_fraud_drivers` (natural language explanation)

---

## 10. Explainability (XAI)

Every flagged transaction is explained using **SHAP values** computed via XGBoost's native `pred_contribs` method. This satisfies regulatory requirements for model explainability (SR 11-7, GDPR Article 22, BSA/AML).

### What Gets Explained

| Signal | Example Explanation |
|---|---|
| **Structuring Pattern** | "Multiple deposits under reporting threshold followed by consolidation withdrawal" |
| **Amount Anomaly** | "Transaction amount \$4,200 is 3.2 standard deviations above customer average" |
| **Velocity Spike** | "12 transactions in last hour (customer norm: 2-3)" |
| **Geographic Risk** | "First transaction from high-risk jurisdiction" |
| **Time-of-Day Anomaly** | "Transaction at 3:17 AM (customer norm: 9 AM – 6 PM)" |
| **Merchant Risk** | "Blacklisted merchant category (MCC 7995 - Gambling)" |
| **Channel Anomaly** | "First ATM withdrawal in 6 months, preceded by online deposits" |

### Output Format

The `gold_shap_explanations` table contains:
- **19 SHAP value columns** (one per model feature): numeric contribution to risk score
- **`top_fraud_drivers`**: Human-readable comma-separated explanation string
- **`predicted_fraud_prob`**: Model confidence (0.0 – 1.0)
- **`predicted_label`**: Binary classification (0 = legitimate, 1 = suspicious)

---

## 11. Dashboard & GenAI Assistant

### Banking Risk Intelligence Control Tower

**Dashboard ID:** `01f143ca9a921acb99d66f223b384a23`

A dark-themed, 4-page executive dashboard for investigation and monitoring:

| Page | Content |
|---|---|
| **Executive Overview** | KPI counters (total txns, suspicious detected, suspicion rate, total loss, active alerts, high-risk accounts), trend charts, risk distribution |
| **Alert Management & Rules** | Alert rule scorecard table, false positive rates by rule, resolution time metrics |
| **Investigation & Explainability** | High-risk account table, SHAP explanation viewer, risk by segment heatmap |
| **Compliance & Audit** | Compliance flags by action type, audit trail metrics, regulatory readiness |

### GenAI Assistant (Genie Space)

A natural-language query interface that allows analysts and business users to investigate without writing SQL:

| Example Question | What It Returns |
|---|---|
| *"Show me all structuring patterns this week"* | Transactions with velocity spikes and amount patterns below reporting thresholds |
| *"Which customers have the highest risk scores?"* | Top-N customers from gold_customer_360 ranked by composite risk |
| *"What is the false positive rate for rule RULE_003?"* | Precision metrics from the alert rule performance table |
| *"Show high-value international transactions above \$5,000"* | Filtered enriched transactions with cross-border flags |

---

## 12. Key Business Metrics

| Metric | Definition | Source Table |
|---|---|---|
| **Suspicion Rate** | % of transactions flagged as suspicious | `gold_daily_fraud_summary.fraud_rate` |
| **False Positive Rate** | Alerts that are legitimate / total alerts | `gold_alert_rule_performance.fp_rate` |
| **Alert-to-Case Ratio** | System alerts vs confirmed suspicious cases | `gold_alert_rule_performance` |
| **Mean Time to Detect** | Hours from suspicious activity to system detection | `silver_fraud_labels` (detected − txn_datetime) |
| **Loss Recovery Rate** | `recovered_amount / loss_amount` | `silver_fraud_labels` |
| **High-Risk Account %** | Customers with `is_high_risk = true` | `gold_customer_360` |
| **Daily Transaction Volume** | Transactions per day by channel | `gold_daily_fraud_summary.total_txns` |
| **Avg Alert Resolution Time** | Hours to resolve an alert | `gold_alert_rule_performance.avg_resolution_hours` |

---

## 13. Technical Notes

### Medallion Architecture Standards

| Layer | Schema | Naming Convention | Partitioning |
|---|---|---|---|
| **Bronze** | `banking_demo.bronze` | `{entity}` (e.g., `customers`) | None |
| **Silver** | `banking_demo.silver` | `silver_{entity}` (e.g., `silver_customers`) | None |
| **Gold** | `banking_demo.gold` | `gold_{purpose}` (e.g., `gold_customer_360`) | `txn_date` (where applicable) |

### Known Technical Fixes Applied

| Issue | Resolution |
|---|---|
| `DATATYPE_MISMATCH.RANGE_FRAME_INVALID_TYPE` in window functions | Changed ORDER BY from TIMESTAMP to BIGINT (`txn_ts_unix`) for `rangeBetween` compatibility |
| `MlflowException` — missing model signature | Added `infer_signature()` + `input_example` to `log_model()` call |
| SHAP / XGBoost `base_score` incompatibility | Bypassed SHAP's `TreeExplainer`, used XGBoost's native `booster.predict(dmatrix, pred_contribs=True)` |
| Module not found errors | Added `%pip install mlflow xgboost shap typing_extensions --upgrade` cell with kernel restart |

### Project Structure

```
/Users/jaikaran.n@diggibyte.com/Banking_Demo/
├── 00_BusinessWalkthrough.README    ← You are here
├── 00_Data Generation               ← Simulate streaming ingestion → bronze
├── 01_bronze_to_silver               ← Bronze → Silver pipeline
├── 02_Silver to Gold                 ← Silver → Gold pipeline
├── 03_Feature Engineering            ← 61-feature ML table
├── 04_MLModel                        ← XGBoost + SHAP + MLflow
├── 05_Dashboard & Genie              ← Dashboard SQL + GenAI config
├── 06_Create Genie Space             ← GenAI assistant REST API
├── sdp_banking_pipeline              ← Declarative pipeline (optional)
└── Dashboard/                        ← Dashboard assets
```

---

<div style="text-align: center; padding: 10px 0; color: #888;">

**Banking Suspicious Activity Detection & Explainability Platform** · Built on Databricks Lakehouse · Unity Catalog Governed

</div>

---

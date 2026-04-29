# Databricks notebook source
# DBTITLE 1,Business Use Case & KPIs
# MAGIC %md
# MAGIC
# MAGIC <div style="background: linear-gradient(135deg, #0D1117 0%, #161B22 100%); padding: 40px; border-radius: 12px; border: 1px solid #30363D; margin-bottom: 20px;">
# MAGIC
# MAGIC <h1 style="text-align: center; color: #E6EDF3; margin-bottom: 5px;"> Banking — Suspicious Activity Detection</h1>
# MAGIC <p style="text-align: center; color: #8B949E; font-size: 16px; margin-top: 0;">End-to-End Risk Intelligence & Explainability on Databricks Lakehouse</p>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Business Use Case
# MAGIC
# MAGIC Banks process **millions of transactions daily** across multiple channels — ATMs, online banking, mobile apps, POS terminals, and wire transfers. Hidden within this volume are **suspicious patterns** that signal money laundering, account takeover, identity theft, and payment manipulation.
# MAGIC
# MAGIC Traditional rule-based systems catch obvious cases but miss sophisticated schemes. Modern banking requires a platform that can:
# MAGIC
# MAGIC > **Ingest** transactions in real-time → **Detect** suspicious behavior using ML + rules → **Score** risk dynamically → **Explain** alerts to analysts → **Prioritize** cases for investigation
# MAGIC
# MAGIC This demo simulates a **production banking environment** where:
# MAGIC
# MAGIC - Transactions stream into Databricks via Kafka / IoT-style feeds
# MAGIC - A **suspicious pattern appears** — e.g., multiple small deposits (\$900, \$850, \$950) followed by one large withdrawal (\$3,500) — classic **structuring behavior**
# MAGIC - The **ML model flags the anomaly** — velocity features spike, amount z-score exceeds 2.8σ
# MAGIC - The **risk score increases dynamically** from Low to Critical
# MAGIC - The **system generates an alert** routed to the analyst queue
# MAGIC - **GenAI explains** the alert: *"This account shows structuring behavior — 5 deposits under reporting threshold followed by rapid consolidation withdrawal. Velocity 5x above normal. Cross-channel activity in 4 hours."*
# MAGIC - The **Dashboard** shows high-risk accounts, alert queues, and per-transaction explanations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## The Problem We Are Solving
# MAGIC
# MAGIC <table style="width:100%; border-collapse: collapse;">
# MAGIC <tr style="border-bottom: 2px solid #30363D;">
# MAGIC <td style="padding: 15px; width: 5%;"><h3>⚠️</h3></td>
# MAGIC <td style="padding: 15px; width: 30%;"><strong>Detection Gap</strong></td>
# MAGIC <td style="padding: 15px;">Banks cannot detect suspicious transaction patterns in real-time. Legacy batch systems process transactions hours or days later — by then, funds have already moved. Sophisticated schemes like structuring, layering, and rapid cross-channel movement evade static rules entirely.</td>
# MAGIC </tr>
# MAGIC <tr style="border-bottom: 2px solid #30363D;">
# MAGIC <td style="padding: 15px;"><h3>🚨</h3></td>
# MAGIC <td style="padding: 15px;"><strong>Alert Fatigue</strong></td>
# MAGIC <td style="padding: 15px;">Rule-only systems generate <strong>90%+ false positive rates</strong>. Analysts waste time investigating legitimate transactions while real suspicious activity gets buried. This leads to missed cases, regulatory fines, and burnout.</td>
# MAGIC </tr>
# MAGIC <tr style="border-bottom: 2px solid #30363D;">
# MAGIC <td style="padding: 15px;"><h3>🔍</h3></td>
# MAGIC <td style="padding: 15px;"><strong>Explainability Deficit</strong></td>
# MAGIC <td style="padding: 15px;">Black-box ML models flag transactions but <strong>cannot explain why</strong>. Regulators (GDPR Article 22, SR 11-7, BSA/AML) require banks to justify every decision. Without explainability, models cannot be deployed in production.</td>
# MAGIC </tr>
# MAGIC <tr>
# MAGIC <td style="padding: 15px;"><h3>📊</h3></td>
# MAGIC <td style="padding: 15px;"><strong>Investigation Bottleneck</strong></td>
# MAGIC <td style="padding: 15px;">Analysts lack a unified view of customer risk, transaction history, alert context, and case status. They toggle between 5-10 systems. No prioritization means critical cases wait in the same queue as low-risk noise.</td>
# MAGIC </tr>
# MAGIC </table>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Key Performance Indicators (KPIs)
# MAGIC
# MAGIC These are the metrics this platform tracks to measure detection effectiveness, operational efficiency, and compliance readiness:
# MAGIC
# MAGIC ### Detection & Risk KPIs
# MAGIC
# MAGIC | KPI | Definition | Target | Source Table |
# MAGIC |---|---|---|---|
# MAGIC | **Suspicious Activity Rate** | % of total transactions flagged as suspicious | < 5% (lower = better precision) | `gold_daily_fraud_summary` |
# MAGIC | **High-Risk Account Count** | Customers with `is_high_risk = true` | Monitor trend (should decrease over time) | `gold_customer_360` |
# MAGIC | **Risk Tier Distribution** | Breakdown of Critical / High / Medium / Low tiers | Majority should be Low | `gold_fraud_predictions` |
# MAGIC | **Daily Suspicious Volume** | Count of flagged transactions per day by channel | Track for spikes and seasonality | `gold_daily_fraud_summary` |
# MAGIC
# MAGIC ### Operational Efficiency KPIs
# MAGIC
# MAGIC | KPI | Definition | Target | Source Table |
# MAGIC |---|---|---|---|
# MAGIC | **False Positive Rate** | Alerts confirmed as legitimate / total alerts | < 50% (industry avg: 90%+) | `gold_alert_rule_performance` |
# MAGIC | **Alert-to-Case Ratio** | System alerts generated vs confirmed suspicious cases | < 10:1 | `gold_alert_rule_performance` |
# MAGIC | **Mean Time to Detect (MTTD)** | Hours from suspicious activity to system detection | < 1 hour (real-time goal) | `silver_fraud_labels` |
# MAGIC | **Mean Time to Resolve (MTTR)** | Hours from alert creation to analyst resolution | < 24 hours | `gold_alert_rule_performance` |
# MAGIC | **Rule Performance Score** | Per-rule true positive rate and false positive rate | Retire rules with FP > 70% | `gold_alert_rule_performance` |
# MAGIC
# MAGIC ### Financial Impact KPIs
# MAGIC
# MAGIC | KPI | Definition | Target | Source Table |
# MAGIC |---|---|---|---|
# MAGIC | **Total Loss Amount** | Sum of confirmed losses from suspicious activity | Minimize (\$0 ideal) | `silver_fraud_labels` |
# MAGIC | **Loss Recovery Rate** | `recovered_amount / loss_amount` | > 60% | `silver_fraud_labels` |
# MAGIC | **Loss by Activity Type** | Breakdown of losses by category (structuring, account takeover, etc.) | Identify top contributors | `silver_fraud_labels` |
# MAGIC
# MAGIC ### Compliance & Governance KPIs
# MAGIC
# MAGIC | KPI | Definition | Target | Source Table |
# MAGIC |---|---|---|---|
# MAGIC | **Compliance Flag Rate** | % of audit actions flagged for compliance review | Track and review all flags | `silver_audit_logs` |
# MAGIC | **Explainability Coverage** | % of flagged transactions with SHAP explanations | 100% (regulatory requirement) | `gold_shap_explanations` |
# MAGIC | **Audit Trail Completeness** | All investigator actions logged with timestamps | 100% coverage | `silver_audit_logs` |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC *The sections below provide the full technical deep-dive: architecture, data model, table catalog, pipeline notebooks, how to run, ML model details, explainability, and dashboard configuration.*
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC <div style="text-align: center; padding: 30px 0;">
# MAGIC
# MAGIC # BANKING SUSPICIOUS ACTIVITY DETECTION & EXPLAINABILITY PLATFORM
# MAGIC ### Production Demo on Databricks Lakehouse
# MAGIC
# MAGIC **Catalog:** `banking_demo` &nbsp;|&nbsp; **Schemas:** `bronze` · `silver` · `gold` &nbsp;|&nbsp; **Runtime:** Apr 2025 – Mar 2026
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC *Ingest transactions in real-time · Detect suspicious behavior using ML + Rules · Score risk dynamically · Explain alerts · Prioritize cases for investigation*
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Table of Contents
# MAGIC
# MAGIC 1. [Problem Statement](#problem-statement)
# MAGIC 2. [What This Demo Shows](#what-this-demo-shows)
# MAGIC 3. [Solution Architecture](#solution-architecture)
# MAGIC 4. [Data Model](#data-model)
# MAGIC 5. [Table Catalog](#table-catalog)
# MAGIC 6. [Pipeline Notebooks](#pipeline-notebooks)
# MAGIC 7. [How to Run](#how-to-run)
# MAGIC 8. [Demo Flow — Live Walkthrough](#demo-flow--live-walkthrough)
# MAGIC 9. [ML Model & Risk Scoring](#ml-model--risk-scoring)
# MAGIC 10. [Explainability (XAI)](#explainability-xai)
# MAGIC 11. [Dashboard & GenAI Assistant](#dashboard--genai-assistant)
# MAGIC 12. [Key Business Metrics](#key-business-metrics)
# MAGIC 13. [Technical Notes](#technical-notes)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Problem Statement
# MAGIC
# MAGIC Banks today struggle with three critical challenges:
# MAGIC
# MAGIC | Challenge | Pain Point | Business Impact |
# MAGIC |---|---|---|
# MAGIC | **Detect** suspicious transaction patterns in real-time | Legacy batch systems miss fast-moving anomalies | Revenue loss, regulatory penalties, reputational damage |
# MAGIC | **Reduce** false alerts flooding analyst queues | Rule-only systems generate 90%+ false positives | Analyst fatigue, poor customer experience, wasted resources |
# MAGIC | **Explain** why a transaction was flagged | Black-box models fail regulatory scrutiny (GDPR, SR 11-7, BSA/AML) | Compliance risk, audit failures, inability to defend decisions |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 2. What This Demo Shows
# MAGIC
# MAGIC This platform demonstrates an **end-to-end suspicious activity detection and explainability solution** built entirely on the Databricks Lakehouse:
# MAGIC
# MAGIC | Capability | Implementation |
# MAGIC |---|---|
# MAGIC | **Ingest transactions in real-time** | Streaming ingestion via Kafka / simulated IoT-style feeds into Delta Lake |
# MAGIC | **Detect suspicious behavior using ML + rules** | XGBoost anomaly detection model combined with configurable rule engine |
# MAGIC | **Score risk dynamically** | 61-feature pipeline computing velocity, anomaly z-scores, and composite risk in near-real-time |
# MAGIC | **Explain alerts with Dashboard** | SHAP-based per-transaction explanations surfaced in a 4-page Risk Intelligence Control Tower |
# MAGIC | **Prioritize cases for investigation** | Risk tiers (Critical/High/Medium/Low) auto-route to analyst queues |
# MAGIC | **GenAI assistant** | Genie Space for natural-language investigation queries ("Show me all structuring patterns this week") |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 3. Solution Architecture
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────────────────────────────┐
# MAGIC │                         DATABRICKS LAKEHOUSE PLATFORM                                   │
# MAGIC │                         Unity Catalog: banking_demo                                     │
# MAGIC ├─────────────────────────────────────────────────────────────────────────────────────────┤
# MAGIC │                                                                                         │
# MAGIC │  ┌──────────────┐    ┌──────────────────┐    ┌──────────────────┐    ┌───────────────┐  │
# MAGIC │  │  INGESTION   │    │  BRONZE (Raw)    │    │  SILVER (Clean)  │    │  GOLD (Curated│  │
# MAGIC │  │              │    │  Schema: bronze   │    │  Schema: silver  │    │  Schema: gold) │  │
# MAGIC │  │  Streaming   │───▶│                  │───▶│                  │───▶│               │  │
# MAGIC │  │  Kafka /     │    │  7 raw tables    │    │  7 silver tables │    │  7 gold tables│  │
# MAGIC │  │  Simulated   │    │  As-ingested     │    │  Type-cast       │    │  Aggregated   │  │
# MAGIC │  │  IoT-style   │    │  Schema-on-read  │    │  Deduplicated    │    │  Feature-eng  │  │
# MAGIC │  │  feeds       │    │                  │    │  Validated       │    │  ML-ready     │  │
# MAGIC │  └──────────────┘    └──────────────────┘    └──────────────────┘    └───────┬───────┘  │
# MAGIC │                                                                             │           │
# MAGIC │                       ┌─────────────────────────────────────────────────────┤           │
# MAGIC │                       │                                                     │           │
# MAGIC │                       ▼                                                     ▼           │
# MAGIC │  ┌────────────────────────────────────┐    ┌────────────────────────────────────────┐   │
# MAGIC │  │  ML & ANOMALY DETECTION            │    │  ANALYTICS & SERVING                   │   │
# MAGIC │  │                                    │    │                                        │   │
# MAGIC │  │  ┌──────────────┐  ┌────────────┐  │    │  ┌─────────────────┐  ┌─────────────┐  │   │
# MAGIC │  │  │  XGBoost     │  │   SHAP     │  │    │  │  Dashboard:     │  │ Genie Space │  │   │
# MAGIC │  │  │  Anomaly     │──│  Explainer │  │    │  │  Risk Intel     │  │ GenAI       │  │   │
# MAGIC │  │  │  Detector    │  │            │  │    │  │  Control Tower  │  │ Natural     │  │   │
# MAGIC │  │  │              │  │  Per-txn   │  │    │  │  4 pages        │  │ Language    │  │   │
# MAGIC │  │  │  Features:61 │  │  drivers   │  │    │  │                 │  │ Queries     │  │   │
# MAGIC │  │  │  MLflow+UC   │  │            │  │    │  │                 │  │             │  │   │
# MAGIC │  │  └──────────────┘  └────────────┘  │    │  └─────────────────┘  └─────────────┘  │   │
# MAGIC │  │                                    │    │                                        │   │
# MAGIC │  │  Output: risk_predictions          │    │  Job: Banking_Demo_Pipeline            │   │
# MAGIC │  │          shap_explanations         │    │  6 sequential tasks, single-node       │   │
# MAGIC │  └────────────────────────────────────┘    └────────────────────────────────────────┘   │
# MAGIC │                                                                                         │
# MAGIC │  ┌─────────────────────────────────────────────────────────────────────────────────────┐ │
# MAGIC │  │  GOVERNANCE: Unity Catalog (lineage, access control, tagging, audit)               │ │
# MAGIC │  └─────────────────────────────────────────────────────────────────────────────────────┘ │
# MAGIC │                                                                                         │
# MAGIC └─────────────────────────────────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Architecture Components
# MAGIC
# MAGIC | Layer | Technology | Purpose |
# MAGIC |---|---|---|
# MAGIC | **Ingestion** | Streaming (Kafka / simulated IoT-style) | Real-time transaction feed into Delta Lake |
# MAGIC | **Storage** | Delta Lake (Bronze / Silver / Gold) | ACID-compliant medallion architecture |
# MAGIC | **Processing** | Structured Streaming + Spark | Type casting, dedup, feature engineering |
# MAGIC | **ML Layer** | XGBoost + SHAP | Anomaly detection model + network/graph-based risk scoring |
# MAGIC | **Governance** | Unity Catalog | Lineage, access control, tagging, audit trail |
# MAGIC | **Serving** | Dashboard + Genie Space | Investigation app, alert management, GenAI assistant |
# MAGIC
# MAGIC ### Data Flow Summary
# MAGIC
# MAGIC ```
# MAGIC 00_Data Generation          01_bronze_to_silver         02_Silver to Gold
# MAGIC ┌───────────────┐           ┌───────────────┐           ┌───────────────┐
# MAGIC │ Simulate      │           │ Type casting  │           │ Star schema   │
# MAGIC │ streaming     │──────────▶│ Deduplication │──────────▶│ Aggregations  │
# MAGIC │ transactions  │           │ Null handling │           │ Customer 360  │
# MAGIC │ ~20K records  │           │ Validation    │           │ Daily summary │
# MAGIC └───────────────┘           └───────────────┘           └───────┬───────┘
# MAGIC                                                                 │
# MAGIC                     ┌───────────────────────────────────────────┘
# MAGIC                     │
# MAGIC                     ▼
# MAGIC 03_Feature Engineering      04_MLModel                  05_Dashboard & Genie
# MAGIC ┌───────────────┐           ┌───────────────┐           ┌───────────────┐
# MAGIC │ 61 features   │           │ XGBoost train │           │ SQL views for │
# MAGIC │ Velocity      │──────────▶│ MLflow log    │──────────▶│ dashboard     │
# MAGIC │ Anomaly score │           │ SHAP values   │           │ Genie Space   │
# MAGIC │ Composite risk│           │ Risk tiers    │           │ GenAI config  │
# MAGIC └───────────────┘           └───────────────┘           └───────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 4. Data Model
# MAGIC
# MAGIC ### Entity-Relationship Diagram
# MAGIC
# MAGIC ```
# MAGIC                                     ┌───────────────────────┐
# MAGIC                                     │      customers        │
# MAGIC                                     │  (PK) customer_id     │
# MAGIC                                     │  full_name, email     │
# MAGIC                                     │  country, segment     │
# MAGIC                                     │  kyc_status           │
# MAGIC                                     │  risk_score           │
# MAGIC                                     │  is_pep, is_sanctioned│
# MAGIC                                     └──────────┬────────────┘
# MAGIC                                                │
# MAGIC                                           1 ── ┤ ── N
# MAGIC                                                │
# MAGIC                               ┌────────────────┴────────────────┐
# MAGIC                               │           accounts              │
# MAGIC                               │  (PK) account_id               │
# MAGIC                               │  (FK) customer_id               │
# MAGIC                               │  account_type, balance          │
# MAGIC                               │  credit_limit, currency         │
# MAGIC                               │  status, branch_code            │
# MAGIC                               │  is_joint, open_date            │
# MAGIC                               └────────────────┬────────────────┘
# MAGIC                                                │
# MAGIC                                           1 ── ┤ ── N
# MAGIC                                                │
# MAGIC ┌───────────────────────┐     ┌────────────────┴────────────────┐     ┌───────────────────────┐
# MAGIC │      merchants        │     │         transactions            │     │  suspicious_case_labels│
# MAGIC │  (PK) merchant_id     │     │  (PK) transaction_id            │     │  (PK) case_id          │
# MAGIC │  merchant_name        │ N──▶│  (FK) account_id                │◀──1 │  (FK) transaction_id   │
# MAGIC │  mcc_code             │  1  │  (FK) merchant_id               │     │  case_type             │
# MAGIC │  mcc_description      │     │  amount, currency, channel      │     │  case_status           │
# MAGIC │  country, risk_level  │     │  txn_datetime, txn_date         │     │  loss_amount           │
# MAGIC │  avg_txn_amount       │     │  transaction_type, status       │     │  recovered_amount      │
# MAGIC │  is_blacklisted       │     │  device_id, ip_address          │     │  investigator_id       │
# MAGIC │  registered_date      │     │  is_international               │     │  detected_datetime     │
# MAGIC └───────────────────────┘     │  counterparty_id                │     │  confirmed_datetime    │
# MAGIC                               │  response_code, txn_hour        │     │  reported_by, notes    │
# MAGIC                               └────────────────┬────────────────┘     └────────────────────────┘
# MAGIC                                                │
# MAGIC                                           1 ── ┤ ── N
# MAGIC                                                │
# MAGIC                               ┌────────────────┴────────────────┐
# MAGIC                               │            alerts               │
# MAGIC                               │  (PK) alert_id                 │
# MAGIC                               │  (FK) transaction_id            │
# MAGIC                               │  rule_id, alert_type            │
# MAGIC                               │  severity, score                │
# MAGIC                               │  alert_status, explanation      │
# MAGIC                               │  false_positive, analyst_id     │
# MAGIC                               │  created_datetime               │
# MAGIC                               │  resolved_datetime              │
# MAGIC                               └─────────────────────────────────┘
# MAGIC
# MAGIC                               ┌─────────────────────────────────┐
# MAGIC                               │         audit_logs              │
# MAGIC                               │  (PK) log_id                   │
# MAGIC                               │  entity_id, entity_type         │
# MAGIC                               │  action, actor_id, actor_type   │
# MAGIC                               │  status, compliance_flag        │
# MAGIC                               │  change_summary, ip_address     │
# MAGIC                               │  session_id, log_datetime       │
# MAGIC                               └─────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Key Relationships
# MAGIC
# MAGIC | FK Relationship | Join Condition |
# MAGIC |---|---|
# MAGIC | `accounts` → `customers` | `accounts.customer_id = customers.customer_id` |
# MAGIC | `transactions` → `accounts` | `transactions.account_id = accounts.account_id` |
# MAGIC | `transactions` → `merchants` | `transactions.merchant_id = merchants.merchant_id` |
# MAGIC | `case_labels` → `transactions` | `case_labels.transaction_id = transactions.transaction_id` |
# MAGIC | `alerts` → `transactions` | `alerts.transaction_id = transactions.transaction_id` |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 5. Table Catalog
# MAGIC
# MAGIC ### Bronze Layer — `banking_demo.bronze`
# MAGIC > Raw ingested data from streaming feeds. No transformations applied. Schema-on-read.
# MAGIC
# MAGIC | # | Table | Key Columns | Description |
# MAGIC |---|---|---|---|
# MAGIC | 1 | `customers` | `customer_id`, `full_name`, `country`, `segment`, `risk_score`, `kyc_status`, `is_pep`, `is_sanctioned` | Customer profiles with KYC and risk attributes |
# MAGIC | 2 | `accounts` | `account_id`, `customer_id`, `account_type`, `balance`, `credit_limit`, `status`, `branch_code` | Bank accounts linked to customers |
# MAGIC | 3 | `merchants` | `merchant_id`, `merchant_name`, `mcc_code`, `mcc_description`, `risk_level`, `is_blacklisted` | Merchant profiles with MCC classification |
# MAGIC | 4 | `transactions` | `transaction_id`, `account_id`, `merchant_id`, `amount`, `channel`, `txn_datetime`, `is_international` | Core transaction records (fact table) |
# MAGIC | 5 | `fraud_labels` | `fraud_id`, `transaction_id`, `fraud_type`, `fraud_status`, `loss_amount`, `recovered_amount` | Confirmed suspicious activity case labels |
# MAGIC | 6 | `alerts` | `alert_id`, `transaction_id`, `rule_id`, `alert_type`, `severity`, `score`, `false_positive` | System-generated alerts with rule explanations |
# MAGIC | 7 | `audit_logs` | `log_id`, `entity_id`, `action`, `actor_id`, `compliance_flag`, `change_summary` | Full compliance audit trail |
# MAGIC
# MAGIC ### Silver Layer — `banking_demo.silver`
# MAGIC > Cleaned, validated, type-cast. Deduplication and null handling applied. Metadata column `_silver_load_ts` added.
# MAGIC
# MAGIC | # | Table | Source | Transformations Applied |
# MAGIC |---|---|---|---|
# MAGIC | 1 | `silver_customers` | `bronze.customers` | Date parsing (`date_joined` → DATE), null handling, dedup on `customer_id` |
# MAGIC | 2 | `silver_accounts` | `bronze.accounts` | Date parsing (`open_date` → DATE), balance validation, dedup on `account_id` |
# MAGIC | 3 | `silver_merchants` | `bronze.merchants` | Date parsing, risk_level standardization, dedup on `merchant_id` |
# MAGIC | 4 | `silver_transactions` | `bronze.transactions` | `txn_datetime` → TIMESTAMP, `txn_date` → DATE, `amount` → DOUBLE, dedup on `transaction_id` |
# MAGIC | 5 | `silver_fraud_labels` | `bronze.fraud_labels` | Datetime parsing, `loss_amount`/`recovered_amount` → DOUBLE, dedup |
# MAGIC | 6 | `silver_alerts` | `bronze.alerts` | Datetime parsing, `score` → DOUBLE, `false_positive` → BOOLEAN, dedup on `alert_id` |
# MAGIC | 7 | `silver_audit_logs` | `bronze.audit_logs` | Datetime parsing, `compliance_flag` → BOOLEAN, dedup on `log_id` |
# MAGIC
# MAGIC ### Gold Layer — `banking_demo.gold`
# MAGIC > Aggregated, feature-engineered, ML-ready. Star schema design.
# MAGIC
# MAGIC | # | Table | Columns | Description |
# MAGIC |---|---|---|---|
# MAGIC | 1 | `gold_customer_360` | 22 cols | Customer 360 view: lifetime metrics (`lifetime_txns`, `lifetime_spend`, `high_value_txns`, `intl_txns`), risk bands, account summaries |
# MAGIC | 2 | `gold_transactions_enriched` | 46 cols | Enriched fact table joining transactions + accounts + customers + merchants + case_labels + alerts. Includes `is_suspicious`, `alert_count`, `loss_amount` |
# MAGIC | 3 | `gold_daily_fraud_summary` | 11 cols | Daily KPIs by `channel` and `account_type`: `total_txns`, suspicious count, suspicion rate, `total_loss`, `total_alerts` |
# MAGIC | 4 | `gold_alert_rule_performance` | 10 cols | Alert rule scorecard: `true_positives`, `false_positives`, `fp_rate`, `avg_resolution_hours`, confirmed cases per rule |
# MAGIC | 5 | `gold_ml_features` | 61 cols | Feature-engineered table with velocity, anomaly scores, composite risk. Partitioned by `txn_date` |
# MAGIC | 6 | `gold_fraud_predictions` | 4 cols | Model output: `transaction_id`, suspicion probability, prediction label, `risk_tier` (Critical/High/Medium/Low) |
# MAGIC | 7 | `gold_shap_explanations` | 23 cols | Per-transaction SHAP values for each feature + `top_drivers` (human-readable explanation) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 6. Pipeline Notebooks
# MAGIC
# MAGIC All notebooks are located in `/Users/jaikaran.n@diggibyte.com/Banking_Demo/`
# MAGIC
# MAGIC | Order | Notebook | Purpose | Inputs | Outputs |
# MAGIC |---|---|---|---|---|
# MAGIC | 0 | `00_Data Generation` | Simulate streaming transaction ingestion | None | 7 `bronze.*` tables |
# MAGIC | 1 | `01_bronze_to_silver` | Data quality, type casting, dedup | 7 `bronze.*` tables | 7 `silver.*` tables |
# MAGIC | 2 | `02_Silver to Gold` | Star schema joins, aggregations | 7 `silver.*` tables | 4 `gold.*` tables |
# MAGIC | 3 | `03_Feature Engineering` | Behavioral features, velocity, anomaly scores | `gold.gold_transactions_enriched` | `gold.gold_ml_features` (61 columns) |
# MAGIC | 4 | `04_MLModel` | XGBoost training, MLflow logging, SHAP | `gold.gold_ml_features` | Predictions, SHAP explanations, UC model |
# MAGIC | 5 | `05_Dashboard & Genie` | SQL view definitions for dashboard/Genie | `gold.*`, `silver.*` tables | Dashboard-ready SQL views |
# MAGIC | — | `06_Create Genie Space` | REST API to auto-create GenAI assistant | Config from notebook 05 | Genie Space asset |
# MAGIC
# MAGIC ### Feature Engineering Details (Notebook 03)
# MAGIC
# MAGIC 61 features organized into 5 groups:
# MAGIC
# MAGIC | Feature Group | Examples | Count |
# MAGIC |---|---|---|
# MAGIC | **Transaction Base** | `amount`, `txn_hour`, `is_international`, `is_high_value`, `is_odd_hour` | 12 |
# MAGIC | **Velocity (Window)** | `txn_count_1h`, `txn_count_24h`, `txn_count_7d`, `unique_merchants_24h` | 15 |
# MAGIC | **Amount Aggregates** | `amount_sum_1h`, `amount_sum_24h`, `amount_avg_7d`, `amount_max_7d` | 12 |
# MAGIC | **Anomaly Scores** | `amount_zscore`, `geo_risk_score`, `channel_risk`, `composite_risk_score` | 10 |
# MAGIC | **Contextual** | `merchant_risk_encoded`, `account_balance`, `customer_risk_score`, `is_pep` | 12 |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 7. How to Run
# MAGIC
# MAGIC ### Prerequisites
# MAGIC
# MAGIC - **Databricks Workspace** with Unity Catalog enabled
# MAGIC - **Catalog** `banking_demo` must exist (or you need CREATE CATALOG permissions)
# MAGIC - **Compute**: Any cluster with DBR 14.3 LTS+ (Python 3.10+)
# MAGIC - **Libraries**: `mlflow`, `xgboost`, `shap` (auto-installed via `%pip` in notebook 04)
# MAGIC
# MAGIC ### Option A: Run via Lakeflow Job (Recommended)
# MAGIC
# MAGIC A pre-configured job **Banking_Demo_Pipeline** (Job ID: `958717178058660`) executes all 6 notebooks in sequence.
# MAGIC
# MAGIC ```
# MAGIC Step 1:  Navigate to Jobs → Banking_Demo_Pipeline
# MAGIC Step 2:  Click "Run Now"
# MAGIC Step 3:  Monitor 6 sequential tasks:
# MAGIC
# MAGIC          data_generation ──▶ bronze_to_silver ──▶ silver_to_gold
# MAGIC                                                        │
# MAGIC          feature_engineering ──▶ ml_model ──▶ dashboard_genie
# MAGIC
# MAGIC Estimated Duration: ~15-25 minutes (single-node)
# MAGIC ```
# MAGIC
# MAGIC **Job Compute Configuration** (cost-optimized):
# MAGIC
# MAGIC | Setting | Value |
# MAGIC |---|---|
# MAGIC | Instance Type | Standard_DS3_v2 (Azure) |
# MAGIC | Workers | 0 (single-node) |
# MAGIC | Runtime | 16.4.x-scala2.12 LTS |
# MAGIC | Photon | Disabled |
# MAGIC | Spot Instances | Enabled (with on-demand fallback) |
# MAGIC
# MAGIC ### Option B: Run Notebooks Individually
# MAGIC
# MAGIC ```bash
# MAGIC # Execute in order — each notebook depends on the previous
# MAGIC
# MAGIC # Step 1: Simulate streaming transaction ingestion into bronze
# MAGIC Run → 00_Data Generation
# MAGIC
# MAGIC # Step 2: Clean and validate into silver
# MAGIC Run → 01_bronze_to_silver
# MAGIC
# MAGIC # Step 3: Build gold aggregations and star schema
# MAGIC Run → 02_Silver to Gold
# MAGIC
# MAGIC # Step 4: Engineer 61 ML features (velocity, anomaly, composite risk)
# MAGIC Run → 03_Feature Engineering
# MAGIC
# MAGIC # Step 5: Train anomaly detection model, generate SHAP explanations
# MAGIC Run → 04_MLModel
# MAGIC
# MAGIC # Step 6: Set up dashboard SQL views and GenAI assistant config
# MAGIC Run → 05_Dashboard & Genie
# MAGIC
# MAGIC # Optional: Create Genie Space via REST API
# MAGIC Run → 06_Create Genie Space
# MAGIC ```
# MAGIC
# MAGIC ### Option C: Run via Databricks CLI
# MAGIC
# MAGIC ```bash
# MAGIC databricks jobs run-now 958717178058660
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 8. Demo Flow — Live Walkthrough
# MAGIC
# MAGIC This is the recommended narrative for presenting the demo to stakeholders:
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────────────┐
# MAGIC │                                                                         │
# MAGIC │  STEP 1: STREAM TRANSACTIONS INTO DATABRICKS                           │
# MAGIC │  ─────────────────────────────────────────                              │
# MAGIC │  Transactions arrive in real-time via simulated Kafka feeds.            │
# MAGIC │  They land in Delta Lake bronze tables as raw, immutable records.       │
# MAGIC │                                                                         │
# MAGIC │                              ▼                                          │
# MAGIC │                                                                         │
# MAGIC │  STEP 2: A SUSPICIOUS PATTERN APPEARS                                  │
# MAGIC │  ────────────────────────────────────                                   │
# MAGIC │  The system detects structuring behavior:                               │
# MAGIC │  ┌───────────────────────────────────────────────────────────┐          │
# MAGIC │  │  Multiple small deposits ($900, $850, $950, $800)         │          │
# MAGIC │  │  followed by one large withdrawal ($3,500)                │          │
# MAGIC │  │  across 3 different channels within 4 hours               │          │
# MAGIC │  └───────────────────────────────────────────────────────────┘          │
# MAGIC │                                                                         │
# MAGIC │                              ▼                                          │
# MAGIC │                                                                         │
# MAGIC │  STEP 3: ML MODEL FLAGS ANOMALY                                        │
# MAGIC │  ──────────────────────────────                                         │
# MAGIC │  XGBoost anomaly detector scores the transaction:                       │
# MAGIC │  • Velocity features spike (txn_count_1h = 5, normal = 1-2)           │
# MAGIC │  • Amount z-score = 2.8σ above customer average                        │
# MAGIC │  • Composite risk score = 0.87 (Critical tier)                         │
# MAGIC │                                                                         │
# MAGIC │                              ▼                                          │
# MAGIC │                                                                         │
# MAGIC │  STEP 4: RISK SCORE INCREASES DYNAMICALLY                              │
# MAGIC │  ────────────────────────────────────────                               │
# MAGIC │  The customer's risk score escalates from "Low" to "Critical"          │
# MAGIC │  based on the pattern. Account-level and customer-level risk           │
# MAGIC │  bands are recalculated in the gold_customer_360 table.                │
# MAGIC │                                                                         │
# MAGIC │                              ▼                                          │
# MAGIC │                                                                         │
# MAGIC │  STEP 5: SYSTEM GENERATES ALERT                                        │
# MAGIC │  ──────────────────────────────                                         │
# MAGIC │  An alert is created with:                                              │
# MAGIC │  • Severity: HIGH                                                       │
# MAGIC │  • Rule triggered: velocity_check + amount_anomaly                     │
# MAGIC │  • Auto-routed to analyst queue based on risk tier                     │
# MAGIC │                                                                         │
# MAGIC │                              ▼                                          │
# MAGIC │                                                                         │
# MAGIC │  STEP 6: GenAI EXPLAINS THE ALERT                                      │
# MAGIC │  ────────────────────────────────                                       │
# MAGIC │  SHAP-based explanation generated:                                      │
# MAGIC │  ┌───────────────────────────────────────────────────────────┐          │
# MAGIC │  │  "This account shows structuring behavior typical of      │          │
# MAGIC │  │   money laundering. 5 deposits under reporting threshold  │          │
# MAGIC │  │   followed by rapid consolidation withdrawal. Velocity    │          │
# MAGIC │  │   5x above normal. Cross-channel activity in 4 hours."   │          │
# MAGIC │  └───────────────────────────────────────────────────────────┘          │
# MAGIC │                                                                         │
# MAGIC │                              ▼                                          │
# MAGIC │                                                                         │
# MAGIC │  STEP 7: DASHBOARD SHOWS INVESTIGATION VIEW                            │
# MAGIC │  ───────────────────────────────────────────                            │
# MAGIC │  Risk Intelligence Control Tower displays:                              │
# MAGIC │  • High-risk accounts ranked by composite score                        │
# MAGIC │  • Alert queue with auto-prioritization                                │
# MAGIC │  • Per-transaction SHAP explanations                                    │
# MAGIC │  • Compliance audit trail                                               │
# MAGIC │                                                                         │
# MAGIC └─────────────────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 9. ML Model & Risk Scoring
# MAGIC
# MAGIC ### Model Details
# MAGIC
# MAGIC | Property | Value |
# MAGIC |---|---|
# MAGIC | **Algorithm** | XGBoost (`xgb.XGBClassifier`) — Anomaly Detection |
# MAGIC | **Features** | 61 engineered features from `gold_ml_features` |
# MAGIC | **Target** | `is_suspicious` (binary classification) |
# MAGIC | **Training Split** | 80/20 stratified |
# MAGIC | **Registry** | `banking_demo.gold.banking_fraud_detector` (Unity Catalog) |
# MAGIC | **Tracking** | MLflow experiment with metrics, params, artifacts |
# MAGIC
# MAGIC ### Risk Tier Classification
# MAGIC
# MAGIC | Risk Tier | Probability Range | Automated Action |
# MAGIC |---|---|---|
# MAGIC | **Critical** | \>= 0.8 | Auto-block transaction + immediate investigation |
# MAGIC | **High** | 0.6 – 0.8 | Priority queue for analyst review |
# MAGIC | **Medium** | 0.4 – 0.6 | Standard review within 24h |
# MAGIC | **Low** | < 0.4 | Log and monitor |
# MAGIC
# MAGIC ### Model Output Tables
# MAGIC
# MAGIC - **`gold_fraud_predictions`**: `transaction_id`, suspicion probability, prediction label, `risk_tier`
# MAGIC - **`gold_shap_explanations`**: Per-feature SHAP values + `top_fraud_drivers` (natural language explanation)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 10. Explainability (XAI)
# MAGIC
# MAGIC Every flagged transaction is explained using **SHAP values** computed via XGBoost's native `pred_contribs` method. This satisfies regulatory requirements for model explainability (SR 11-7, GDPR Article 22, BSA/AML).
# MAGIC
# MAGIC ### What Gets Explained
# MAGIC
# MAGIC | Signal | Example Explanation |
# MAGIC |---|---|
# MAGIC | **Structuring Pattern** | "Multiple deposits under reporting threshold followed by consolidation withdrawal" |
# MAGIC | **Amount Anomaly** | "Transaction amount \$4,200 is 3.2 standard deviations above customer average" |
# MAGIC | **Velocity Spike** | "12 transactions in last hour (customer norm: 2-3)" |
# MAGIC | **Geographic Risk** | "First transaction from high-risk jurisdiction" |
# MAGIC | **Time-of-Day Anomaly** | "Transaction at 3:17 AM (customer norm: 9 AM – 6 PM)" |
# MAGIC | **Merchant Risk** | "Blacklisted merchant category (MCC 7995 - Gambling)" |
# MAGIC | **Channel Anomaly** | "First ATM withdrawal in 6 months, preceded by online deposits" |
# MAGIC
# MAGIC ### Output Format
# MAGIC
# MAGIC The `gold_shap_explanations` table contains:
# MAGIC - **19 SHAP value columns** (one per model feature): numeric contribution to risk score
# MAGIC - **`top_fraud_drivers`**: Human-readable comma-separated explanation string
# MAGIC - **`predicted_fraud_prob`**: Model confidence (0.0 – 1.0)
# MAGIC - **`predicted_label`**: Binary classification (0 = legitimate, 1 = suspicious)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 11. Dashboard & GenAI Assistant
# MAGIC
# MAGIC ### Banking Risk Intelligence Control Tower
# MAGIC
# MAGIC **Dashboard ID:** `01f143ca9a921acb99d66f223b384a23`
# MAGIC
# MAGIC A dark-themed, 4-page executive dashboard for investigation and monitoring:
# MAGIC
# MAGIC | Page | Content |
# MAGIC |---|---|
# MAGIC | **Executive Overview** | KPI counters (total txns, suspicious detected, suspicion rate, total loss, active alerts, high-risk accounts), trend charts, risk distribution |
# MAGIC | **Alert Management & Rules** | Alert rule scorecard table, false positive rates by rule, resolution time metrics |
# MAGIC | **Investigation & Explainability** | High-risk account table, SHAP explanation viewer, risk by segment heatmap |
# MAGIC | **Compliance & Audit** | Compliance flags by action type, audit trail metrics, regulatory readiness |
# MAGIC
# MAGIC ### GenAI Assistant (Genie Space)
# MAGIC
# MAGIC A natural-language query interface that allows analysts and business users to investigate without writing SQL:
# MAGIC
# MAGIC | Example Question | What It Returns |
# MAGIC |---|---|
# MAGIC | *"Show me all structuring patterns this week"* | Transactions with velocity spikes and amount patterns below reporting thresholds |
# MAGIC | *"Which customers have the highest risk scores?"* | Top-N customers from gold_customer_360 ranked by composite risk |
# MAGIC | *"What is the false positive rate for rule RULE_003?"* | Precision metrics from the alert rule performance table |
# MAGIC | *"Show high-value international transactions above \$5,000"* | Filtered enriched transactions with cross-border flags |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 12. Key Business Metrics
# MAGIC
# MAGIC | Metric | Definition | Source Table |
# MAGIC |---|---|---|
# MAGIC | **Suspicion Rate** | % of transactions flagged as suspicious | `gold_daily_fraud_summary.fraud_rate` |
# MAGIC | **False Positive Rate** | Alerts that are legitimate / total alerts | `gold_alert_rule_performance.fp_rate` |
# MAGIC | **Alert-to-Case Ratio** | System alerts vs confirmed suspicious cases | `gold_alert_rule_performance` |
# MAGIC | **Mean Time to Detect** | Hours from suspicious activity to system detection | `silver_fraud_labels` (detected − txn_datetime) |
# MAGIC | **Loss Recovery Rate** | `recovered_amount / loss_amount` | `silver_fraud_labels` |
# MAGIC | **High-Risk Account %** | Customers with `is_high_risk = true` | `gold_customer_360` |
# MAGIC | **Daily Transaction Volume** | Transactions per day by channel | `gold_daily_fraud_summary.total_txns` |
# MAGIC | **Avg Alert Resolution Time** | Hours to resolve an alert | `gold_alert_rule_performance.avg_resolution_hours` |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 13. Technical Notes
# MAGIC
# MAGIC ### Medallion Architecture Standards
# MAGIC
# MAGIC | Layer | Schema | Naming Convention | Partitioning |
# MAGIC |---|---|---|---|
# MAGIC | **Bronze** | `banking_demo.bronze` | `{entity}` (e.g., `customers`) | None |
# MAGIC | **Silver** | `banking_demo.silver` | `silver_{entity}` (e.g., `silver_customers`) | None |
# MAGIC | **Gold** | `banking_demo.gold` | `gold_{purpose}` (e.g., `gold_customer_360`) | `txn_date` (where applicable) |
# MAGIC
# MAGIC ### Known Technical Fixes Applied
# MAGIC
# MAGIC | Issue | Resolution |
# MAGIC |---|---|
# MAGIC | `DATATYPE_MISMATCH.RANGE_FRAME_INVALID_TYPE` in window functions | Changed ORDER BY from TIMESTAMP to BIGINT (`txn_ts_unix`) for `rangeBetween` compatibility |
# MAGIC | `MlflowException` — missing model signature | Added `infer_signature()` + `input_example` to `log_model()` call |
# MAGIC | SHAP / XGBoost `base_score` incompatibility | Bypassed SHAP's `TreeExplainer`, used XGBoost's native `booster.predict(dmatrix, pred_contribs=True)` |
# MAGIC | Module not found errors | Added `%pip install mlflow xgboost shap typing_extensions --upgrade` cell with kernel restart |
# MAGIC
# MAGIC ### Project Structure
# MAGIC
# MAGIC ```
# MAGIC /Users/jaikaran.n@diggibyte.com/Banking_Demo/
# MAGIC ├── 00_BusinessWalkthrough.README    ← You are here
# MAGIC ├── 00_Data Generation               ← Simulate streaming ingestion → bronze
# MAGIC ├── 01_bronze_to_silver               ← Bronze → Silver pipeline
# MAGIC ├── 02_Silver to Gold                 ← Silver → Gold pipeline
# MAGIC ├── 03_Feature Engineering            ← 61-feature ML table
# MAGIC ├── 04_MLModel                        ← XGBoost + SHAP + MLflow
# MAGIC ├── 05_Dashboard & Genie              ← Dashboard SQL + GenAI config
# MAGIC ├── 06_Create Genie Space             ← GenAI assistant REST API
# MAGIC ├── sdp_banking_pipeline              ← Declarative pipeline (optional)
# MAGIC └── Dashboard/                        ← Dashboard assets
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC <div style="text-align: center; padding: 10px 0; color: #888;">
# MAGIC
# MAGIC **Banking Suspicious Activity Detection & Explainability Platform** · Built on Databricks Lakehouse · Unity Catalog Governed
# MAGIC
# MAGIC </div>

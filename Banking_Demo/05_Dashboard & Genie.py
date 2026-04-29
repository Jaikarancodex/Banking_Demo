# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Dashboard & Genie
# MAGIC ## Banking Fraud Detection — Business Intelligence Queries

# COMMAND ----------

CATALOG = "banking_demo"
SILVER  = "silver"
GOLD    = "gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Genie Space — Natural Language Questions
# MAGIC
# MAGIC Business users can ask questions like:
# MAGIC - *"How many fraud cases were detected last month?"*
# MAGIC - *"Which rule generates the most false positives?"*
# MAGIC - *"Show top 10 highest-risk customers"*
# MAGIC - *"What is the daily fraud rate trend for Q1 2026?"*
# MAGIC - *"Which merchant categories have the most fraud?"*

# COMMAND ----------

# MAGIC %md ### KPI 1: Overall Fraud Summary
# MAGIC
# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*)                                         AS total_transactions,
# MAGIC     SUM(is_fraud)                                    AS total_fraud_txns,
# MAGIC     ROUND(AVG(is_fraud) * 100, 3)                    AS fraud_rate_pct,
# MAGIC     ROUND(SUM(loss_amount), 2)                       AS total_fraud_loss,
# MAGIC     ROUND(AVG(loss_amount), 2)                       AS avg_fraud_loss,
# MAGIC     COUNT(DISTINCT CASE WHEN is_fraud=1 THEN account_id END) AS affected_accounts
# MAGIC FROM banking_demo.gold.gold_transactions_enriched
# MAGIC WHERE txn_date BETWEEN '2025-04-01' AND '2026-03-31';

# COMMAND ----------

# MAGIC %md ### KPI 2: Monthly Fraud Trend
# MAGIC
# MAGIC %sql
# MAGIC SELECT
# MAGIC     DATE_FORMAT(txn_date, 'yyyy-MM')       AS month,
# MAGIC     COUNT(*)                               AS total_txns,
# MAGIC     SUM(is_fraud)                          AS fraud_count,
# MAGIC     ROUND(AVG(is_fraud) * 100, 3)         AS fraud_rate_pct,
# MAGIC     ROUND(SUM(loss_amount), 2)            AS fraud_loss,
# MAGIC     ROUND(AVG(fraud_probability), 4)      AS avg_fraud_prob
# MAGIC FROM banking_demo.gold.gold_transactions_enriched t
# MAGIC LEFT JOIN banking_demo.gold.gold_fraud_predictions p
# MAGIC     USING (transaction_id)
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1;

# COMMAND ----------

# MAGIC %md ### KPI 3: Alert Rule False-Positive Leaderboard
# MAGIC
# MAGIC %sql
# MAGIC SELECT
# MAGIC     rule_id,
# MAGIC     alert_type,
# MAGIC     severity,
# MAGIC     total_alerts,
# MAGIC     true_positives,
# MAGIC     false_positives,
# MAGIC     ROUND(fp_rate * 100, 1)                AS false_positive_rate_pct,
# MAGIC     avg_score,
# MAGIC     ROUND(avg_resolution_hours, 1)         AS avg_resolution_hrs
# MAGIC FROM banking_demo.gold.gold_alert_rule_performance
# MAGIC ORDER BY fp_rate DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md ### KPI 4: Top 10 High-Risk Customers
# MAGIC
# MAGIC %sql
# MAGIC SELECT
# MAGIC     c.customer_id,
# MAGIC     c.segment,
# MAGIC     c.country,
# MAGIC     c.risk_score,
# MAGIC     c.risk_band,
# MAGIC     c.kyc_status,
# MAGIC     c.is_pep,
# MAGIC     c.is_sanctioned,
# MAGIC     c360.lifetime_txns,
# MAGIC     c360.lifetime_spend,
# MAGIC     c360.total_declines,
# MAGIC     c360.high_value_txns
# MAGIC FROM banking_demo.silver.silver_customers c
# MAGIC JOIN banking_demo.gold.gold_customer_360 c360 USING (customer_id)
# MAGIC WHERE c.is_high_risk = true
# MAGIC ORDER BY c.risk_score DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md ### KPI 5: Fraud by Channel & Transaction Type
# MAGIC
# MAGIC %sql
# MAGIC SELECT
# MAGIC     channel,
# MAGIC     transaction_type,
# MAGIC     COUNT(*)                         AS total_txns,
# MAGIC     SUM(is_fraud)                    AS fraud_count,
# MAGIC     ROUND(AVG(is_fraud)*100, 2)      AS fraud_rate_pct,
# MAGIC     ROUND(SUM(loss_amount), 0)       AS total_loss
# MAGIC FROM banking_demo.gold.gold_transactions_enriched
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY fraud_rate_pct DESC;

# COMMAND ----------

# MAGIC %md ### KPI 6: SHAP Explanation — Top Fraud Drivers (most common)
# MAGIC
# MAGIC %sql
# MAGIC SELECT
# MAGIC     top_fraud_drivers,
# MAGIC     COUNT(*)                                AS flagged_transactions,
# MAGIC     ROUND(AVG(predicted_fraud_prob), 4)    AS avg_fraud_probability
# MAGIC FROM banking_demo.gold.gold_shap_explanations
# MAGIC WHERE predicted_label = 1
# MAGIC GROUP BY 1
# MAGIC ORDER BY flagged_transactions DESC
# MAGIC LIMIT 15;

# COMMAND ----------

# MAGIC %md ### KPI 7: Merchant Risk Heatmap
# MAGIC
# MAGIC %sql
# MAGIC SELECT
# MAGIC     m.mcc_description,
# MAGIC     m.risk_level,
# MAGIC     COUNT(t.transaction_id)         AS total_txns,
# MAGIC     SUM(t.is_fraud)                 AS fraud_count,
# MAGIC     ROUND(AVG(t.is_fraud)*100, 2)   AS fraud_rate_pct,
# MAGIC     ROUND(SUM(t.loss_amount), 2)    AS total_loss
# MAGIC FROM banking_demo.gold.gold_transactions_enriched t
# MAGIC JOIN banking_demo.silver.silver_merchants m USING (merchant_id)
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY fraud_rate_pct DESC;

# COMMAND ----------

# MAGIC %md ### KPI 8: Fraud Velocity — Hourly Distribution
# MAGIC
# MAGIC %sql
# MAGIC SELECT
# MAGIC     txn_hour,
# MAGIC     COUNT(*)                          AS total_txns,
# MAGIC     SUM(is_fraud)                     AS fraud_count,
# MAGIC     ROUND(AVG(is_fraud)*100, 3)       AS fraud_rate_pct
# MAGIC FROM banking_demo.gold.gold_transactions_enriched
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1;

# COMMAND ----------

# MAGIC %md ### KPI 9: Loss Recovery by Fraud Type
# MAGIC
# MAGIC %sql
# MAGIC SELECT
# MAGIC     fraud_type,
# MAGIC     COUNT(*)                              AS cases,
# MAGIC     ROUND(SUM(loss_amount), 2)           AS total_loss,
# MAGIC     ROUND(SUM(recovered_amount), 2)      AS total_recovered,
# MAGIC     ROUND(AVG(recovery_rate)*100, 1)     AS avg_recovery_pct,
# MAGIC     ROUND(AVG(hours_to_confirm), 1)      AS avg_hours_to_confirm
# MAGIC FROM banking_demo.silver.silver_fraud_labels
# MAGIC GROUP BY 1
# MAGIC ORDER BY total_loss DESC;

# COMMAND ----------

# MAGIC %md ### KPI 10: Compliance Audit — Flagged Actions
# MAGIC
# MAGIC %sql
# MAGIC SELECT
# MAGIC     action,
# MAGIC     actor_type,
# MAGIC     COUNT(*)                                       AS log_count,
# MAGIC     SUM(CAST(compliance_flag AS INT))              AS compliance_flags,
# MAGIC     SUM(CAST(is_failure AS INT))                   AS failures,
# MAGIC     ROUND(AVG(CAST(compliance_flag AS INT))*100,1) AS flag_rate_pct
# MAGIC FROM banking_demo.silver.silver_audit_logs
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY compliance_flags DESC;

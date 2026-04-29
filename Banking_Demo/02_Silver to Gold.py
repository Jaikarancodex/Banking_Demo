# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver to Gold
# MAGIC ## Banking Fraud Detection — Aggregation & Enrichment Layer

# COMMAND ----------

CATALOG = "banking_demo"
SILVER  = "silver"
GOLD    = "gold"

from pyspark.sql import functions as F
from pyspark.sql.window import Window

print("Silver → Gold: Aggregations, Joins & Business-ready tables")

# COMMAND ----------


txn      = spark.table(f"{CATALOG}.{SILVER}.silver_transactions")
accounts = spark.table(f"{CATALOG}.{SILVER}.silver_accounts")
customers= spark.table(f"{CATALOG}.{SILVER}.silver_customers")
fraud    = spark.table(f"{CATALOG}.{SILVER}.silver_fraud_labels")
alerts   = spark.table(f"{CATALOG}.{SILVER}.silver_alerts")

# Per-account stats
acct_stats = (
    txn.groupBy("account_id")
    .agg(
        F.count("transaction_id").alias("total_txns"),
        F.sum("amount").alias("total_amount"),
        F.avg("amount").alias("avg_txn_amount"),
        F.max("amount").alias("max_txn_amount"),
        F.sum(F.col("is_high_value").cast("int")).alias("high_value_txn_count"),
        F.sum(F.col("is_international").cast("int")).alias("intl_txn_count"),
        F.sum(F.col("is_declined").cast("int")).alias("decline_count"),
        F.sum(F.col("is_odd_hour").cast("int")).alias("odd_hour_txn_count"),
    )
)

# Join accounts → customers → txn stats
gold_cust360 = (
    customers
    .join(accounts.groupBy("customer_id").agg(
        F.count("account_id").alias("num_accounts"),
        F.sum("balance").alias("total_balance"),
        F.sum(F.col("is_overdrawn").cast("int")).alias("overdrawn_accounts"),
    ), on="customer_id", how="left")
    .join(
        accounts.select("customer_id","account_id")
        .join(acct_stats, on="account_id", how="left")
        .groupBy("customer_id")
        .agg(
            F.sum("total_txns").alias("lifetime_txns"),
            F.sum("total_amount").alias("lifetime_spend"),
            F.sum("high_value_txn_count").alias("high_value_txns"),
            F.sum("decline_count").alias("total_declines"),
            F.sum("intl_txn_count").alias("intl_txns"),
        ),
        on="customer_id", how="left"
    )
)

gold_cust360.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{CATALOG}.{GOLD}.gold_customer_360")
print(f"✅ gold_customer_360: {gold_cust360.count():,} rows")

# COMMAND ----------

# ── GOLD: TRANSACTION ENRICHED ───────────────────────────────────────────────────────────────────────

merchants = spark.table(f"{CATALOG}.{SILVER}.silver_merchants")

fraud_txn = (
    fraud.select(
        "transaction_id",
        "fraud_type",
        F.col("fraud_status").alias("fraud_label_status"),
        "loss_amount",
        "is_resolved"
    )
)

alert_txn = (
    alerts.groupBy("transaction_id").agg(
        F.count("alert_id").alias("alert_count"),
        F.max("score").alias("max_alert_score"),
        F.max(F.col("is_true_positive").cast("int")).alias("has_true_positive_alert"),
    )
)

accounts_df = accounts.select(
    "account_id",
    "customer_id",
    "account_type",
    F.col("currency").alias("account_currency"),   
    F.col("status").alias("account_status")
)

gold_txn = (
    txn
    .join(accounts_df, on="account_id", how="left")
    .join(
        customers.select(
            "customer_id","segment","country","risk_score","risk_band",
            "kyc_status","is_pep","is_sanctioned","is_high_risk"
        ),
        on="customer_id",
        how="left"
    )
    .join(
        merchants.select(
            "merchant_id","mcc_code","mcc_description","risk_level","is_high_risk_merchant"
        ),
        on="merchant_id",
        how="left"
    )
    .join(fraud_txn, on="transaction_id", how="left")
    .join(alert_txn, on="transaction_id", how="left")
    .withColumn("is_fraud", F.col("fraud_type").isNotNull().cast("int"))
    .withColumn("alert_count", F.coalesce(F.col("alert_count"), F.lit(0)))
)

# Write to Delta
gold_txn.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("txn_date") \
    .saveAsTable(f"{CATALOG}.{GOLD}.gold_transactions_enriched")

print(f"gold_transactions_enriched: {gold_txn.count():,} rows")

# COMMAND ----------

# ── GOLD: DAILY FRAUD SUMMARY ────────────────────────────────────────────────────────────────────
### Gold: Daily Fraud KPI Summary

gold_daily = (
    gold_txn
    .groupBy("txn_date", "account_type", "channel")
    .agg(
        F.count("transaction_id").alias("total_txns"),
        F.sum("amount").alias("total_volume"),
        F.sum("is_fraud").alias("fraud_count"),
        F.round(F.sum("is_fraud") / F.count("transaction_id"), 4).alias("fraud_rate"),
        F.sum("loss_amount").alias("total_loss"),
        F.avg("max_alert_score").alias("avg_alert_score"),
        F.sum("alert_count").alias("total_alerts"),
        F.sum(F.col("is_declined").cast("int")).alias("total_declines"),
    )
    .orderBy("txn_date")
)

gold_daily.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{CATALOG}.{GOLD}.gold_daily_fraud_summary")
print(f"✅ gold_daily_fraud_summary: {gold_daily.count():,} rows")

# COMMAND ----------

# ── GOLD: ALERT PERFORMANCE ──────────────────────────────────────────────────────────────────────

fraud_txn = fraud.select("transaction_id", "fraud_type")

gold_alert_perf = (
    alerts
    .join(fraud_txn, on="transaction_id", how="left")
    .withColumn("is_true_positive", F.col("is_true_positive").cast("int"))
    .withColumn("false_positive", F.col("false_positive").cast("int"))
    
    .withColumn("is_confirmed_fraud", F.col("fraud_type").isNotNull().cast("int"))

    .groupBy("rule_id", "alert_type", "severity")
    .agg(
        F.count("alert_id").alias("total_alerts"),
        F.sum("is_true_positive").alias("true_positives"),
        F.sum("false_positive").alias("false_positives"),
        F.sum("is_confirmed_fraud").alias("confirmed_frauds"),
        F.avg("score").alias("avg_score"),
        F.avg("resolution_hours").alias("avg_resolution_hours"),
        F.round(
            F.sum("false_positive") / F.count("alert_id"), 4
        ).alias("fp_rate"),
    )
    .orderBy(F.col("fp_rate").desc())
)

gold_alert_perf.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{CATALOG}.{GOLD}.gold_alert_rule_performance")

print(f"✅ gold_alert_rule_performance: {gold_alert_perf.count():,} rows")

# COMMAND ----------

# MAGIC %md ### ✅ Silver → Gold Complete
# MAGIC Gold layer ready for feature engineering and ML model.

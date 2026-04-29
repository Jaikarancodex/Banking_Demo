# =============================================================================
# Gold Layer: Business Aggregations and Enriched Views
# Source: Silver materialized views from this pipeline
# Target: banking_demo.gold schema
# =============================================================================

from pyspark import pipelines as dp
from pyspark.sql import functions as F


# -----------------------------------------------------------------------------
# 1. Gold Customer 360
# -----------------------------------------------------------------------------
@dp.materialized_view(
    name="banking_demo.gold.gold_customer_360",
    comment="360-degree customer view with account and transaction aggregates",
)
def gold_customer_360():
    customers = spark.read.table("banking_demo.silver.silver_customers")

    # Account aggregates per customer
    account_agg = (
        spark.read.table("banking_demo.silver.silver_accounts")
        .groupBy("customer_id")
        .agg(
            F.count("*").alias("num_accounts"),
            F.sum("balance").alias("total_balance"),
            F.sum(F.when(F.col("is_overdrawn"), 1).otherwise(0)).alias(
                "overdrawn_accounts"
            ),
        )
    )

    # Transaction aggregates per customer (via accounts)
    accounts_lookup = spark.read.table("banking_demo.silver.silver_accounts").select(
        "account_id", "customer_id"
    )
    txn_agg = (
        spark.read.table("banking_demo.silver.silver_transactions")
        .join(accounts_lookup, "account_id", "inner")
        .groupBy("customer_id")
        .agg(
            F.count("*").alias("lifetime_txns"),
            F.sum("amount").alias("lifetime_spend"),
            F.sum(F.when(F.col("is_high_value"), 1).otherwise(0)).alias(
                "high_value_txns"
            ),
            F.sum(F.when(F.col("is_declined"), 1).otherwise(0)).alias(
                "total_declines"
            ),
            F.sum(F.when(F.col("is_international"), 1).otherwise(0)).alias(
                "intl_txns"
            ),
        )
    )

    return (
        customers.join(account_agg, "customer_id", "left").join(
            txn_agg, "customer_id", "left"
        )
    )


# -----------------------------------------------------------------------------
# 2. Gold Transactions Enriched
# -----------------------------------------------------------------------------
@dp.materialized_view(
    name="banking_demo.gold.gold_transactions_enriched",
    comment="Fully enriched transaction records with customer, merchant, fraud, and alert data",
)
def gold_transactions_enriched():
    txns = spark.read.table("banking_demo.silver.silver_transactions")

    accounts = spark.read.table("banking_demo.silver.silver_accounts").select(
        F.col("account_id"),
        F.col("account_type"),
        F.col("customer_id"),
        F.col("currency").alias("account_currency"),
        F.col("status").alias("account_status"),
    )

    customers = spark.read.table("banking_demo.silver.silver_customers").select(
        "customer_id",
        "segment",
        "country",
        "risk_score",
        "risk_band",
        "kyc_status",
        "is_pep",
        "is_sanctioned",
        "is_high_risk",
    )

    merchants = spark.read.table("banking_demo.silver.silver_merchants").select(
        "merchant_id",
        "mcc_code",
        "mcc_description",
        "risk_level",
        "is_high_risk_merchant",
    )

    fraud = spark.read.table("banking_demo.silver.silver_fraud_labels").select(
        F.col("transaction_id"),
        F.col("fraud_type"),
        F.col("fraud_status").alias("fraud_label_status"),
        F.col("loss_amount"),
        F.col("is_resolved"),
    )

    alert_agg = (
        spark.read.table("banking_demo.silver.silver_alerts")
        .groupBy("transaction_id")
        .agg(
            F.count("*").alias("alert_count"),
            F.max("score").alias("max_alert_score"),
            F.max(F.col("is_true_positive").cast("int"))
            .cast("boolean")
            .alias("has_true_positive_alert"),
        )
    )

    return (
        txns.join(accounts, "account_id", "left")
        .join(customers, "customer_id", "left")
        .join(merchants, "merchant_id", "left")
        .join(fraud, "transaction_id", "left")
        .join(alert_agg, "transaction_id", "left")
        .withColumn("is_fraud", F.col("fraud_type").isNotNull())
    )


# -----------------------------------------------------------------------------
# 3. Gold Daily Fraud Summary
# -----------------------------------------------------------------------------
@dp.materialized_view(
    name="banking_demo.gold.gold_daily_fraud_summary",
    comment="Daily fraud metrics aggregated by date, account type, and channel",
)
def gold_daily_fraud_summary():
    return (
        spark.read.table("banking_demo.gold.gold_transactions_enriched")
        .groupBy("txn_date", "account_type", "channel")
        .agg(
            F.count("*").alias("total_txns"),
            F.sum("amount").alias("total_volume"),
            F.sum(F.when(F.col("is_fraud"), 1).otherwise(0)).alias("fraud_count"),
            F.sum(
                F.when(F.col("is_fraud"), F.col("loss_amount")).otherwise(0)
            ).alias("total_loss"),
            F.avg("max_alert_score").alias("avg_alert_score"),
            F.sum(F.coalesce(F.col("alert_count"), F.lit(0))).alias("total_alerts"),
            F.sum(F.when(F.col("is_declined"), 1).otherwise(0)).alias(
                "total_declines"
            ),
        )
        .withColumn(
            "fraud_rate",
            F.when(F.col("total_txns") > 0, F.col("fraud_count") / F.col("total_txns")).otherwise(0),
        )
    )


# -----------------------------------------------------------------------------
# 4. Gold Alert Rule Performance
# -----------------------------------------------------------------------------
@dp.materialized_view(
    name="banking_demo.gold.gold_alert_rule_performance",
    comment="Alert rule effectiveness analysis with precision metrics",
)
def gold_alert_rule_performance():
    alerts = spark.read.table("banking_demo.silver.silver_alerts")
    fraud = spark.read.table("banking_demo.silver.silver_fraud_labels").select(
        "transaction_id", "fraud_id"
    )

    return (
        alerts.join(fraud, "transaction_id", "left")
        .groupBy("rule_id", "alert_type", "severity")
        .agg(
            F.count("*").alias("total_alerts"),
            F.sum(F.when(F.col("is_true_positive"), 1).otherwise(0)).alias(
                "true_positives"
            ),
            F.sum(F.when(F.col("false_positive"), 1).otherwise(0)).alias(
                "false_positives"
            ),
            F.sum(F.when(F.col("fraud_id").isNotNull(), 1).otherwise(0)).alias(
                "confirmed_frauds"
            ),
            F.avg("score").alias("avg_score"),
            F.avg("resolution_hours").alias("avg_resolution_hours"),
        )
        .withColumn(
            "fp_rate",
            F.when(
                F.col("total_alerts") > 0,
                F.col("false_positives") / F.col("total_alerts"),
            ).otherwise(0),
        )
    )

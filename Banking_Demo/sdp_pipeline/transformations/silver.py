# =============================================================================
# Silver Layer: Cleaned, Validated, and Enriched Data
# Source: Bronze streaming tables from this pipeline
# Target: banking_demo.silver schema
# =============================================================================

from pyspark import pipelines as dp
from pyspark.sql import functions as F


# -----------------------------------------------------------------------------
# 1. Silver Customers
# -----------------------------------------------------------------------------
@dp.materialized_view(
    name="banking_demo.silver.silver_customers",
    comment="Cleaned customer profiles with risk classification",
)
@dp.expect("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect("valid_risk_score", "risk_score BETWEEN 0 AND 100")
def silver_customers():
    return (
        spark.read.table("banking_demo.bronze.bronze_customers")
        .withColumn("date_joined", F.col("date_joined").cast("DATE"))
        .withColumn(
            "risk_band",
            F.when(F.col("risk_score") <= 30, "Low")
            .when(F.col("risk_score") <= 60, "Medium")
            .when(F.col("risk_score") <= 80, "High")
            .otherwise("Critical"),
        )
        .withColumn(
            "is_high_risk",
            (F.col("risk_score") > 70) | F.col("is_pep") | F.col("is_sanctioned"),
        )
    )


# -----------------------------------------------------------------------------
# 2. Silver Accounts
# -----------------------------------------------------------------------------
@dp.materialized_view(
    name="banking_demo.silver.silver_accounts",
    comment="Cleaned account data with overdraft flag",
)
@dp.expect("valid_account_id", "account_id IS NOT NULL")
@dp.expect("valid_customer_id", "customer_id IS NOT NULL")
def silver_accounts():
    return (
        spark.read.table("banking_demo.bronze.bronze_accounts")
        .withColumn("open_date", F.col("open_date").cast("DATE"))
        .withColumn("is_overdrawn", F.col("balance") < 0)
    )


# -----------------------------------------------------------------------------
# 3. Silver Merchants
# -----------------------------------------------------------------------------
@dp.materialized_view(
    name="banking_demo.silver.silver_merchants",
    comment="Cleaned merchant profiles",
)
@dp.expect("valid_merchant_id", "merchant_id IS NOT NULL")
def silver_merchants():
    return spark.read.table("banking_demo.bronze.bronze_merchants")


# -----------------------------------------------------------------------------
# 4. Silver Transactions
# -----------------------------------------------------------------------------
@dp.materialized_view(
    name="banking_demo.silver.silver_transactions",
    comment="Cleaned transactions with derived flags",
)
@dp.expect("valid_transaction_id", "transaction_id IS NOT NULL")
@dp.expect("valid_amount", "amount > 0")
@dp.expect("valid_account_id", "account_id IS NOT NULL")
def silver_transactions():
    return (
        spark.read.table("banking_demo.bronze.bronze_transactions")
        .withColumn("txn_datetime", F.col("txn_datetime").cast("TIMESTAMP"))
        .withColumn("txn_date", F.col("txn_date").cast("DATE"))
        .withColumn("is_high_value", F.col("amount") > 10000)
        .withColumn("is_declined", F.col("status") == "Declined")
        .withColumn(
            "is_odd_hour", (F.col("txn_hour") < 6) | (F.col("txn_hour") > 22)
        )
    )


# -----------------------------------------------------------------------------
# 5. Silver Fraud Labels
# -----------------------------------------------------------------------------
@dp.materialized_view(
    name="banking_demo.silver.silver_fraud_labels",
    comment="Cleaned fraud labels with resolution metrics",
)
@dp.expect("valid_fraud_id", "fraud_id IS NOT NULL")
@dp.expect("valid_transaction_id", "transaction_id IS NOT NULL")
def silver_fraud_labels():
    return (
        spark.read.table("banking_demo.bronze.bronze_fraud_labels")
        .withColumn("detected_datetime", F.col("detected_datetime").cast("TIMESTAMP"))
        .withColumn(
            "confirmed_datetime", F.col("confirmed_datetime").cast("TIMESTAMP")
        )
        .withColumn(
            "hours_to_confirm",
            (
                F.unix_timestamp("confirmed_datetime")
                - F.unix_timestamp("detected_datetime")
            )
            / 3600,
        )
        .withColumn(
            "recovery_rate",
            F.when(
                F.col("loss_amount") > 0,
                F.col("recovered_amount") / F.col("loss_amount"),
            ).otherwise(F.lit(None)),
        )
        .withColumn(
            "is_resolved",
            F.col("fraud_status").isin("Resolved", "Confirmed"),
        )
    )


# -----------------------------------------------------------------------------
# 6. Silver Alerts
# -----------------------------------------------------------------------------
@dp.materialized_view(
    name="banking_demo.silver.silver_alerts",
    comment="Cleaned alerts with resolution metrics",
)
@dp.expect("valid_alert_id", "alert_id IS NOT NULL")
def silver_alerts():
    return (
        spark.read.table("banking_demo.bronze.bronze_alerts")
        .withColumn("created_datetime", F.col("created_datetime").cast("TIMESTAMP"))
        .withColumn("resolved_datetime", F.col("resolved_datetime").cast("TIMESTAMP"))
        .withColumn(
            "resolution_hours",
            (
                F.unix_timestamp("resolved_datetime")
                - F.unix_timestamp("created_datetime")
            )
            / 3600,
        )
        .withColumn(
            "is_true_positive", F.col("alert_status").like("%True Positive%")
        )
    )


# -----------------------------------------------------------------------------
# 7. Silver Audit Logs
# -----------------------------------------------------------------------------
@dp.materialized_view(
    name="banking_demo.silver.silver_audit_logs",
    comment="Cleaned audit logs with failure flag",
)
@dp.expect("valid_log_id", "log_id IS NOT NULL")
def silver_audit_logs():
    return (
        spark.read.table("banking_demo.bronze.bronze_audit_logs")
        .withColumn("log_datetime", F.col("log_datetime").cast("TIMESTAMP"))
        .withColumn("is_failure", F.col("status") == "Failure")
    )

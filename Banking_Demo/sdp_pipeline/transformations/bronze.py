# =============================================================================
# Bronze Layer: Raw Data Ingestion as Streaming Tables
# Source: banking_demo.bronze.* (existing Delta tables)
# Target: banking_demo.bronze schema
# =============================================================================

from pyspark import pipelines as dp


@dp.table(
    name="banking_demo.bronze.bronze_customers",
    comment="Raw customer profiles ingested as streaming table",
)
def bronze_customers():
    return spark.readStream.table("banking_demo.bronze.customers")


@dp.table(
    name="banking_demo.bronze.bronze_accounts",
    comment="Raw account data ingested as streaming table",
)
def bronze_accounts():
    return spark.readStream.table("banking_demo.bronze.accounts")


@dp.table(
    name="banking_demo.bronze.bronze_merchants",
    comment="Raw merchant profiles ingested as streaming table",
)
def bronze_merchants():
    return spark.readStream.table("banking_demo.bronze.merchants")


@dp.table(
    name="banking_demo.bronze.bronze_transactions",
    comment="Raw transaction records ingested as streaming table",
)
def bronze_transactions():
    return spark.readStream.table("banking_demo.bronze.transactions")


@dp.table(
    name="banking_demo.bronze.bronze_fraud_labels",
    comment="Raw confirmed and investigated fraud cases",
)
def bronze_fraud_labels():
    return spark.readStream.table("banking_demo.bronze.fraud_labels")


@dp.table(
    name="banking_demo.bronze.bronze_alerts",
    comment="Raw system-generated alerts with rule explanations",
)
def bronze_alerts():
    return spark.readStream.table("banking_demo.bronze.alerts")


@dp.table(
    name="banking_demo.bronze.bronze_audit_logs",
    comment="Raw audit trail for compliance",
)
def bronze_audit_logs():
    return spark.readStream.table("banking_demo.bronze.audit_logs")

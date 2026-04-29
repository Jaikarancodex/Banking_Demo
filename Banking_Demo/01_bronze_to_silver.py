# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze to Silver
# MAGIC ## Banking Fraud Detection — Data Quality & Cleansing Layer

# COMMAND ----------

CATALOG = "banking_demo"
BRONZE  = "bronze"
SILVER  = "silver"

from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, DoubleType, BooleanType
from pyspark.sql.window import Window

print("Bronze → Silver: Cleaning & Validating all 7 tables")

# COMMAND ----------

customers_raw = spark.table(f"{CATALOG}.{BRONZE}.customers")

silver_customers = (
    customers_raw
    .dropDuplicates(["customer_id"])
    .filter(F.col("customer_id").isNotNull())
    .withColumn("risk_band",
        F.when(F.col("risk_score") < 25, "Low")
         .when(F.col("risk_score") < 50, "Medium")
         .when(F.col("risk_score") < 75, "High")
         .otherwise("Critical"))
    .withColumn("date_joined", F.to_date("date_joined"))
    .withColumn("is_high_risk",
        (F.col("is_pep") | F.col("is_sanctioned") | (F.col("risk_score") > 75)).cast(BooleanType()))
    .withColumn("_silver_load_ts", F.current_timestamp())
)

silver_customers.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{CATALOG}.{SILVER}.silver_customers")
print(f"✅ silver_customers: {silver_customers.count():,} rows")

# COMMAND ----------


accounts_raw = spark.table(f"{CATALOG}.{BRONZE}.accounts")

silver_accounts = (
    accounts_raw
    .dropDuplicates(["account_id"])
    .filter(F.col("account_id").isNotNull() & F.col("customer_id").isNotNull())
    .withColumn("open_date", F.to_date("open_date"))
    .withColumn("balance", F.col("balance").cast(DoubleType()))
    .withColumn("is_overdrawn", (F.col("balance") < 0).cast(BooleanType()))
    .withColumn("account_age_days",
        F.datediff(F.current_date(), F.col("open_date")))
    .withColumn("_silver_load_ts", F.current_timestamp())
)

silver_accounts.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{CATALOG}.{SILVER}.silver_accounts")
print(f"✅ silver_accounts: {silver_accounts.count():,} rows")

# COMMAND ----------


merchants_raw = spark.table(f"{CATALOG}.{BRONZE}.merchants")

silver_merchants = (
    merchants_raw
    .dropDuplicates(["merchant_id"])
    .filter(~F.col("is_blacklisted"))   # flag blacklisted separately
    .withColumn("registered_date", F.to_date("registered_date"))
    .withColumn("is_high_risk_merchant",
        ((F.col("risk_level").isin(["High","Very High"])) | F.col("is_blacklisted"))
        .cast(BooleanType()))
    .withColumn("_silver_load_ts", F.current_timestamp())
)

silver_merchants.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{CATALOG}.{SILVER}.silver_merchants")
print(f"✅ silver_merchants: {silver_merchants.count():,} rows")

# COMMAND ----------


txn_raw = spark.table(f"{CATALOG}.{BRONZE}.transactions")

silver_txn = (
    txn_raw
    .dropDuplicates(["transaction_id"])
    .filter(F.col("transaction_id").isNotNull() & F.col("account_id").isNotNull())
    .filter(F.col("amount") > 0)                        # remove zero/negative amounts
    .filter(F.col("status").isin(["Completed","Pending","Declined","Reversed"]))
    .withColumn("txn_datetime",  F.to_timestamp("txn_datetime"))
    .withColumn("txn_date",      F.to_date("txn_date"))
    .withColumn("amount",        F.col("amount").cast(DoubleType()))
    .withColumn("is_high_value", (F.col("amount") > 10000).cast(BooleanType()))
    .withColumn("is_odd_hour",   (F.col("txn_hour").between(0, 5)).cast(BooleanType()))
    .withColumn("is_declined",   (F.col("status") == "Declined").cast(BooleanType()))
    .withColumn("week_of_year",  F.weekofyear("txn_date"))
    .withColumn("month",         F.month("txn_date"))
    .withColumn("_silver_load_ts", F.current_timestamp())
)

silver_txn.write.format("delta").mode("overwrite").partitionBy("txn_date") \
    .saveAsTable(f"{CATALOG}.{SILVER}.silver_transactions")
print(f"✅ silver_transactions: {silver_txn.count():,} rows")

# COMMAND ----------


fraud_raw = spark.table(f"{CATALOG}.{BRONZE}.fraud_labels")

silver_fraud = (
    fraud_raw
    .dropDuplicates(["fraud_id"])
    .filter(F.col("transaction_id").isNotNull())
    .withColumn("detected_datetime",  F.to_timestamp("detected_datetime"))
    .withColumn("confirmed_datetime", F.to_timestamp("confirmed_datetime"))
    .withColumn("loss_amount",        F.col("loss_amount").cast(DoubleType()))
    .withColumn("recovered_amount",   F.col("recovered_amount").cast(DoubleType()))
    .withColumn("recovery_rate",
        F.round(F.col("recovered_amount") / F.col("loss_amount"), 4))
    .withColumn("hours_to_confirm",
        (F.unix_timestamp("confirmed_datetime") - F.unix_timestamp("detected_datetime")) / 3600)
    .withColumn("is_resolved",
        F.col("fraud_status").isin(["Confirmed","Resolved"]).cast(BooleanType()))
    .withColumn("_silver_load_ts", F.current_timestamp())
)

silver_fraud.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{CATALOG}.{SILVER}.silver_fraud_labels")
print(f"✅ silver_fraud_labels: {silver_fraud.count():,} rows")

# COMMAND ----------


alerts_raw = spark.table(f"{CATALOG}.{BRONZE}.alerts")

silver_alerts = (
    alerts_raw
    .dropDuplicates(["alert_id"])
    .filter(F.col("transaction_id").isNotNull())
    .withColumn("created_datetime",  F.to_timestamp("created_datetime"))
    .withColumn("resolved_datetime", F.to_timestamp("resolved_datetime"))
    .withColumn("score",             F.col("score").cast(DoubleType()))
    .withColumn("resolution_hours",
        F.when(F.col("resolved_datetime").isNotNull(),
            (F.unix_timestamp("resolved_datetime") - F.unix_timestamp("created_datetime")) / 3600
        ).otherwise(None))
    .withColumn("is_true_positive",
        F.col("alert_status").contains("True Positive").cast(BooleanType()))
    .withColumn("_silver_load_ts", F.current_timestamp())
)

silver_alerts.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{CATALOG}.{SILVER}.silver_alerts")
print(f"✅ silver_alerts: {silver_alerts.count():,} rows")

# COMMAND ----------


audit_raw = spark.table(f"{CATALOG}.{BRONZE}.audit_logs")

silver_audit = (
    audit_raw
    .dropDuplicates(["log_id"])
    .filter(F.col("log_id").isNotNull())
    .withColumn("log_datetime", F.to_timestamp("log_datetime"))
    .withColumn("log_date",     F.to_date("log_datetime"))
    .withColumn("is_failure",   (F.col("status") == "Failure").cast(BooleanType()))
    .withColumn("_silver_load_ts", F.current_timestamp())
)

silver_audit.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{CATALOG}.{SILVER}.silver_audit_logs")
print(f"✅ silver_audit_logs: {silver_audit.count():,} rows")

# COMMAND ----------

# MAGIC %md ### ✅ Bronze → Silver Complete
# MAGIC All 7 tables cleaned, typed, and enriched. Ready for Gold aggregation.

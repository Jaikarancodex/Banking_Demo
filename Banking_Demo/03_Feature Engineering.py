# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Feature Engineering
# MAGIC ## Banking Fraud Detection — Behavioral & Velocity Features

# COMMAND ----------

CATALOG = "banking_demo"
SCHEMA  = "gold"
spark.sql(f"USE {CATALOG}.{SCHEMA}")

from pyspark.sql import functions as F
from pyspark.sql.window import Window

txn = spark.table(f"{CATALOG}.{SCHEMA}.gold_transactions_enriched")
print("Building behavioral & velocity features for ML model...")

# COMMAND ----------

# ── VELOCITY FEATURES ────────────────────────────────────────────────────────
### Velocity: Transactions per account in rolling windows

txn_ts = txn.withColumn("txn_ts_unix", F.unix_timestamp("txn_datetime"))

# Window specs (ordered by unix timestamp for rangeBetween compatibility)
w_acct_1h  = Window.partitionBy("account_id").orderBy("txn_ts_unix") \
                   .rangeBetween(-3600, 0)
w_acct_24h = Window.partitionBy("account_id").orderBy("txn_ts_unix") \
                   .rangeBetween(-86400, 0)
w_acct_7d  = Window.partitionBy("account_id").orderBy("txn_ts_unix") \
                   .rangeBetween(-604800, 0)

velocity_features = (
    txn_ts
    .withColumn("txn_count_1h",    F.count("transaction_id").over(w_acct_1h))
    .withColumn("txn_count_24h",   F.count("transaction_id").over(w_acct_24h))
    .withColumn("txn_count_7d",    F.count("transaction_id").over(w_acct_7d))
    .withColumn("amount_sum_1h",   F.sum("amount").over(w_acct_1h))
    .withColumn("amount_sum_24h",  F.sum("amount").over(w_acct_24h))
    .withColumn("amount_avg_7d",   F.avg("amount").over(w_acct_7d))
    .withColumn("amount_stddev_7d",F.stddev("amount").over(w_acct_7d))
)

# COMMAND ----------

# ── ANOMALY SCORE ────────────────────────────────────────────────────────────
### Amount Anomaly Score (Z-score vs 7-day history)

feature_df = (
    velocity_features
    .withColumn("amount_zscore",
        F.when(F.col("amount_stddev_7d") > 0,
            (F.col("amount") - F.col("amount_avg_7d")) / F.col("amount_stddev_7d")
        ).otherwise(0.0))
    # Geo anomaly proxy (international + night)
    .withColumn("geo_risk_score",
        (F.col("is_international").cast("int") * 2 +
         F.col("is_odd_hour").cast("int") * 1.5 +
         F.col("is_high_risk_merchant").cast("int") * 2).cast("double"))
    # Device / channel risk
    .withColumn("channel_risk",
        F.when(F.col("channel") == "API", 3)
         .when(F.col("channel") == "Web", 2)
         .when(F.col("channel") == "Mobile", 1)
         .otherwise(0))
    # Composite fraud risk score (rule-based pre-ML)
    .withColumn("composite_risk_score",
        F.round(
            F.least(F.lit(100.0),
                F.col("risk_score") * 0.3 +
                F.abs(F.col("amount_zscore")) * 10 +
                F.col("geo_risk_score") * 5 +
                F.col("channel_risk") * 3 +
                F.col("txn_count_1h") * 2
            ), 2))
    # High-velocity flag
    .withColumn("is_high_velocity",
        ((F.col("txn_count_1h") > 5) | (F.col("txn_count_24h") > 20)).cast("int"))
    # Round-amount flag (common in fraud)
    .withColumn("is_round_amount",
        ((F.col("amount") % 100 == 0) & (F.col("amount") > 500)).cast("int"))
    # Final label
    .withColumn("label", F.col("is_fraud"))
)

feature_df.write.format("delta").mode("overwrite").partitionBy("txn_date") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.gold_ml_features")
print(f"✅ gold_ml_features: {feature_df.count():,} rows")

# COMMAND ----------

# MAGIC %md ### Feature Summary
# MAGIC
# MAGIC feature_df.select(
# MAGIC     "transaction_id","amount","amount_zscore","composite_risk_score",
# MAGIC     "txn_count_1h","txn_count_24h","is_high_velocity",
# MAGIC     "geo_risk_score","is_round_amount","label"
# MAGIC ).describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Features Built
# MAGIC | Feature | Description |
# MAGIC |---------|-------------|
# MAGIC | `txn_count_1h` | # transactions in last 1 hour (velocity) |
# MAGIC | `amount_zscore` | How unusual is this amount vs 7-day history |
# MAGIC | `composite_risk_score` | Rule-based pre-ML risk score (0–100) |
# MAGIC | `geo_risk_score` | International + night + high-risk merchant |
# MAGIC | `is_high_velocity` | >5 txns/hr or >20 txns/day |
# MAGIC | `is_round_amount` | Round amounts >500 (fraud signal) |
# MAGIC | `label` | 1 = fraud, 0 = legitimate |

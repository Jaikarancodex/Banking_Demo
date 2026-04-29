# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Data Generation
# MAGIC ## Banking Fraud Detection & Explainability Platform
# MAGIC
# MAGIC **Problem Statement:**
# MAGIC Banks struggle to:
# MAGIC - Detect suspicious transaction patterns in real-time
# MAGIC - Reduce false alerts
# MAGIC - Explain why a transaction was flagged
# MAGIC
# MAGIC Generate 1 year of synthetic banking data (Apr 1, 2025 – Mar 31, 2026) — **~20,000 total records**:
# MAGIC - **Customers** – 2,000 bank customers (retail + corporate)
# MAGIC - **Accounts** – 3,000 accounts (savings, checking, credit, loan)
# MAGIC - **Transactions** – ~8,000 transaction records
# MAGIC - **Merchants** – 500 merchant profiles
# MAGIC - **Fraud Labels** – ~2,500 labeled fraud cases
# MAGIC - **Alerts** – ~2,000 system-generated alerts
# MAGIC - **Audit Logs** – ~2,000 audit/compliance log entries

# COMMAND ----------

# SETUP: Create Catalog & Schema
# ==================================================
CATALOG = "banking_demo"
SCHEMA  = "bronze"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")
spark.sql(f"USE {CATALOG}.{SCHEMA}")
print(f"Using: {CATALOG}.{SCHEMA}")

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import random
import uuid
from datetime import datetime, timedelta
import numpy as np

spark = SparkSession.builder.getOrCreate()
random.seed(42)
np.random.seed(42)

START_DATE = datetime(2025, 4, 1)
END_DATE   = datetime(2026, 3, 31)

def rand_date(start=START_DATE, end=END_DATE):
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta),
                             hours=random.randint(0, 23),
                             minutes=random.randint(0, 59),
                             seconds=random.randint(0, 59))

print("Libraries loaded. Date range:", START_DATE.date(), "→", END_DATE.date())

# COMMAND ----------

# ── TABLE 1: CUSTOMERS ──────────────────────────────────────────────────────────────────────
COUNTRIES  = ["USA","UK","Germany","India","Singapore","UAE","Canada","Australia"]
SEGMENTS   = ["Retail", "Premium", "Corporate", "SME"]
KYC_STATUS = ["Verified", "Pending", "Rejected", "Under Review"]

customers = []
for i in range(1, 2001):
    joined = rand_date(START_DATE - timedelta(days=365*5), END_DATE - timedelta(days=30))
    customers.append({
        "customer_id":    f"CUST{i:05d}",
        "full_name":      f"Customer_{i}",
        "email":          f"customer_{i}@example.com",
        "phone":          f"+1-{random.randint(200,999)}-{random.randint(1000000,9999999)}",
        "country":        random.choice(COUNTRIES),
        "segment":        random.choice(SEGMENTS),
        "kyc_status":     random.choices(KYC_STATUS, weights=[70,15,5,10])[0],
        "risk_score":     round(random.uniform(0, 100), 2),
        "date_joined":    joined.strftime("%Y-%m-%d"),
        "is_pep":         random.choices([True, False], weights=[3, 97])[0],  # Politically Exposed Person
        "is_sanctioned":  random.choices([True, False], weights=[1, 99])[0],
    })

customers_df = spark.createDataFrame(customers)
customers_df.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.customers")
print(f"✅ customers: {customers_df.count():,} rows")

# COMMAND ----------

# ── TABLE 2: ACCOUNTS ───────────────────────────────────────────────────────────────────────
ACCT_TYPES   = ["Checking","Savings","Credit","Loan","Forex","Investment"]
ACCT_STATUS  = ["Active","Dormant","Suspended","Closed"]
CURRENCIES   = ["USD","EUR","GBP","INR","SGD","AED","CAD","AUD"]

cust_ids = [f"CUST{i:05d}" for i in range(1, 2001)]
accounts = []
for i in range(1, 3001):
    open_dt = rand_date(START_DATE - timedelta(days=365*3), END_DATE - timedelta(days=30))
    acct_type = random.choice(ACCT_TYPES)
    accounts.append({
        "account_id":    f"ACCT{i:07d}",
        "customer_id":   random.choice(cust_ids),
        "account_type":  acct_type,
        "currency":      random.choice(CURRENCIES),
        "balance":       round(random.uniform(-5000, 500000), 2),
        "credit_limit":  round(random.uniform(1000, 100000), 2) if acct_type == "Credit" else None,
        "status":        random.choices(ACCT_STATUS, weights=[80, 10, 7, 3])[0],
        "open_date":     open_dt.strftime("%Y-%m-%d"),
        "branch_code":   f"BR{random.randint(100, 999)}",
        "is_joint":      random.choices([True, False], weights=[15, 85])[0],
    })

accounts_df = spark.createDataFrame(accounts)
accounts_df.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.accounts")
print(f"✅ accounts: {accounts_df.count():,} rows")

# COMMAND ----------

# ── TABLE 3: MERCHANTS ──────────────────────────────────────────────────────────────────────
MCC_CODES    = ["5411","5912","5812","7011","4111","5999","6011","7372","5944","4812"]
MCC_LABELS   = ["Grocery","Pharmacy","Restaurant","Hotel","Transit","Misc Retail",
                "ATM","Software","Jewelry","Telecom"]
RISK_LEVELS  = ["Low","Medium","High","Very High"]

merchants = []
for i in range(1, 501):
    mcc_idx = random.randint(0, 9)
    merchants.append({
        "merchant_id":     f"MERCH{i:05d}",
        "merchant_name":   f"Merchant_{i}",
        "mcc_code":        MCC_CODES[mcc_idx],
        "mcc_description": MCC_LABELS[mcc_idx],
        "country":         random.choice(COUNTRIES),
        "risk_level":      random.choices(RISK_LEVELS, weights=[50, 30, 15, 5])[0],
        "is_blacklisted":  random.choices([True, False], weights=[2, 98])[0],
        "avg_txn_amount":  round(random.uniform(10, 5000), 2),
        "registered_date": rand_date(START_DATE - timedelta(days=365*5),
                                     START_DATE).strftime("%Y-%m-%d"),
    })

merchants_df = spark.createDataFrame(merchants)
merchants_df.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.merchants")
print(f"✅ merchants: {merchants_df.count():,} rows")

# COMMAND ----------

# ── TABLE 4: TRANSACTIONS ───────────────────────────────────────────────────────────────────
TXN_TYPES    = ["Purchase","ATM Withdrawal","Wire Transfer","ACH","Bill Payment",
                "Refund","P2P Transfer","FX Conversion","Loan Payment","Cash Deposit"]
CHANNELS     = ["Mobile","Web","Branch","ATM","POS","API"]
acct_ids     = [f"ACCT{i:07d}" for i in range(1, 3001)]
merch_ids    = [f"MERCH{i:05d}" for i in range(1, 501)]

transactions = []
for i in range(1, 8001):
    txn_dt  = rand_date()
    is_intl = random.random() < 0.15
    amount  = round(np.random.lognormal(mean=4.5, sigma=1.8), 2)  # log-normal amounts
    transactions.append({
        "transaction_id":    f"TXN{i:08d}",
        "account_id":        random.choice(acct_ids),
        "merchant_id":       random.choice(merch_ids) if random.random() > 0.2 else None,
        "transaction_type":  random.choice(TXN_TYPES),
        "channel":           random.choice(CHANNELS),
        "amount":            amount,
        "currency":          random.choice(CURRENCIES),
        "txn_datetime":      txn_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "txn_date":          txn_dt.strftime("%Y-%m-%d"),
        "txn_hour":          txn_dt.hour,
        "is_international":  is_intl,
        "counterparty_id":   random.choice(acct_ids) if random.random() > 0.5 else None,
        "ip_address":        f"{random.randint(1,255)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}",
        "device_id":         f"DEV{random.randint(10000,99999)}",
        "status":            random.choices(["Completed","Pending","Declined","Reversed"],
                                            weights=[88, 5, 5, 2])[0],
        "response_code":     random.choices(["00","05","51","54","57"],
                                            weights=[88, 4, 4, 2, 2])[0],
    })

txn_df = spark.createDataFrame(transactions)
txn_df.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.transactions")
print(f"✅ transactions: {txn_df.count():,} rows")

# COMMAND ----------

# ── TABLE 5: FRAUD LABELS ───────────────────────────────────────────────────────────────────
FRAUD_TYPES    = ["Card Cloning","Account Takeover","Identity Theft","Money Laundering",
                  "Phishing","Synthetic Identity","Bust-Out","Card Not Present","Insider Fraud"]
FRAUD_STATUS   = ["Confirmed","Under Investigation","False Positive","Resolved"]
REPORTED_BY    = ["System","Customer","Branch","AML Team","Regulator","Partner Bank"]

txn_ids = [f"TXN{i:08d}" for i in random.sample(range(1, 8001), 2500)]
fraud_labels = []
for i, tid in enumerate(txn_ids, 1):
    detected_dt = rand_date()
    fraud_labels.append({
        "fraud_id":         f"FRD{i:06d}",
        "transaction_id":   tid,
        "fraud_type":       random.choice(FRAUD_TYPES),
        "fraud_status":     random.choices(FRAUD_STATUS, weights=[40, 30, 20, 10])[0],
        "reported_by":      random.choice(REPORTED_BY),
        "detected_datetime":detected_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "confirmed_datetime":(detected_dt + timedelta(hours=random.randint(1, 72))).strftime("%Y-%m-%d %H:%M:%S"),
        "loss_amount":      round(random.uniform(50, 50000), 2),
        "recovered_amount": round(random.uniform(0, 10000), 2),
        "investigator_id":  f"INV{random.randint(100,199)}",
        "notes":            f"Fraud pattern detected via rule R{random.randint(100,999)}",
    })

fraud_df = spark.createDataFrame(fraud_labels)
fraud_df.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.fraud_labels")
print(f"✅ fraud_labels: {fraud_df.count():,} rows")

# COMMAND ----------

# ── TABLE 6: ALERTS ───────────────────────────────────────────────────────────────────────
ALERT_TYPES   = ["Velocity Check","Amount Threshold","Geo Anomaly","Device Anomaly",
                 "Blacklist Match","Dormant Account Activity","Round Amount","Night Transaction",
                 "Multiple Declines","Cross-Border High Value"]
ALERT_STATUS  = ["Open","Reviewed","Escalated","Closed - True Positive","Closed - False Positive"]
RULE_IDS      = [f"R{i:03d}" for i in range(100, 160)]
SEVERITY      = ["Low","Medium","High","Critical"]

txn_sample = random.choices([f"TXN{i:08d}" for i in range(1, 8001)], k=2000)
alerts = []
for i, tid in enumerate(txn_sample, 1):
    created_dt = rand_date()
    alerts.append({
        "alert_id":        f"ALT{i:07d}",
        "transaction_id":  tid,
        "rule_id":         random.choice(RULE_IDS),
        "alert_type":      random.choice(ALERT_TYPES),
        "severity":        random.choices(SEVERITY, weights=[30,40,20,10])[0],
        "alert_status":    random.choices(ALERT_STATUS, weights=[20,25,10,25,20])[0],
        "score":           round(random.uniform(0.5, 1.0), 4),
        "created_datetime":created_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "resolved_datetime":(created_dt + timedelta(hours=random.randint(1,96))).strftime("%Y-%m-%d %H:%M:%S") \
                            if random.random() > 0.2 else None,
        "analyst_id":      f"ANL{random.randint(200,299)}",
        "false_positive":  random.choices([True, False], weights=[35, 65])[0],
        "explanation":     f"Triggered by {random.choice(ALERT_TYPES)} rule; "
                           f"score {round(random.uniform(0.5,1.0),2)} exceeded threshold",
    })

alerts_df = spark.createDataFrame(alerts)
alerts_df.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.alerts")
print(f"✅ alerts: {alerts_df.count():,} rows")

# COMMAND ----------

# ── TABLE 7: AUDIT LOGS ─────────────────────────────────────────────────────────────────────
LOG_ACTIONS  = ["Login","Logout","Password Change","Profile Update","Large Transfer Approved",
                "Account Suspended","KYC Update","Fraud Case Opened","Alert Escalated",
                "Beneficiary Added","Limit Override","Manual Review Completed"]
ACTORS       = ["Customer","Analyst","System","Branch Officer","Compliance Officer","API"]

audit_logs = []
for i in range(1, 2001):
    log_dt = rand_date()
    audit_logs.append({
        "log_id":          f"LOG{i:08d}",
        "entity_type":     random.choice(["Customer","Account","Transaction","Alert"]),
        "entity_id":       random.choice(cust_ids + acct_ids),
        "action":          random.choice(LOG_ACTIONS),
        "actor_type":      random.choice(ACTORS),
        "actor_id":        f"USR{random.randint(1000,9999)}",
        "log_datetime":    log_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "ip_address":      f"{random.randint(1,255)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}",
        "session_id":      str(uuid.uuid4()),
        "status":          random.choices(["Success","Failure","Warning"], weights=[85,10,5])[0],
        "change_summary":  f"Field updated by {random.choice(ACTORS)} on {log_dt.strftime('%Y-%m-%d')}",
        "compliance_flag": random.choices([True, False], weights=[8, 92])[0],
    })

audit_df = spark.createDataFrame(audit_logs)
audit_df.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.audit_logs")
print(f"✅ audit_logs: {audit_df.count():,} rows")

# COMMAND ----------

# Summary
print("\n" + "="*60)
print("   DATA GENERATION COMPLETE")
print("="*60)
summary = [
    ("customers",    2000),
    ("accounts",     3000),
    ("merchants",    500),
    ("transactions", 8000),
    ("fraud_labels", 2500),
    ("alerts",       2000),
    ("audit_logs",   2000),
]
print(f"  {'Table':<35} {'~Rows':>10}")
print(f"  {'-'*35} {'-'*10}")
total = 0
for tbl, cnt in summary:
    print(f"  {CATALOG}.{SCHEMA}.{tbl:<20} ~{cnt:>10,} rows")
    total += cnt
print(f"  {'-'*35} {'-'*10}")
print(f"  {'TOTAL':<35} ~{total:>10,} rows")
print("="*60)

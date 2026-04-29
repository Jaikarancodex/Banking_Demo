# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Create Genie Space
# MAGIC ## Banking Fraud Detection — Auto-create Genie Space via REST API

# COMMAND ----------

import requests, json

CATALOG = "banking_demo"
SILVER  = "silver"
GOLD    = "gold"

# Get workspace host & token from Databricks secrets (or dbutils)
WORKSPACE_HOST = dbutils.notebook.entry_point.getDbutils().notebook().getContext() \
                    .browserHostName().get()
TOKEN          = dbutils.notebook.entry_point.getDbutils().notebook().getContext() \
                    .apiToken().get()

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type":  "application/json"
}

print(f"Workspace: {WORKSPACE_HOST}")

# COMMAND ----------

# MAGIC %md ### Define Genie Space Configuration
# MAGIC
# MAGIC GENIE_SPACE_CONFIG = {
# MAGIC     "display_name": "🏦 Banking Fraud Detection Control Tower",
# MAGIC     "description":  (
# MAGIC         "AI-powered fraud intelligence platform. Ask questions about transaction fraud, "
# MAGIC         "suspicious patterns, false positive rates, customer risk profiles, and SHAP explainability. "
# MAGIC         "Data: Apr 2025 – Mar 2026."
# MAGIC     ),
# MAGIC     "warehouse_id": "<YOUR_WAREHOUSE_ID>",   # Replace with your SQL Warehouse ID
# MAGIC     "tables": [
# MAGIC         {"catalog": CATALOG, "schema": GOLD,   "table": "gold_transactions_enriched"},
# MAGIC         {"catalog": CATALOG, "schema": GOLD,   "table": "gold_customer_360"},
# MAGIC         {"catalog": CATALOG, "schema": GOLD,   "table": "gold_daily_fraud_summary"},
# MAGIC         {"catalog": CATALOG, "schema": GOLD,   "table": "gold_alert_rule_performance"},
# MAGIC         {"catalog": CATALOG, "schema": GOLD,   "table": "gold_fraud_predictions"},
# MAGIC         {"catalog": CATALOG, "schema": GOLD,   "table": "gold_shap_explanations"},
# MAGIC         {"catalog": CATALOG, "schema": SILVER, "table": "silver_merchants"},
# MAGIC         {"catalog": CATALOG, "schema": SILVER, "table": "silver_fraud_labels"},
# MAGIC         {"catalog": CATALOG, "schema": SILVER, "table": "silver_audit_logs"},
# MAGIC     ],
# MAGIC     "sample_questions": [
# MAGIC         "How many fraud transactions were detected in Q1 2026?",
# MAGIC         "Which rule generates the most false positive alerts?",
# MAGIC         "Show the top 10 highest-risk customers",
# MAGIC         "What is the daily fraud rate trend over the year?",
# MAGIC         "Which merchant categories have the highest fraud rates?",
# MAGIC         "What are the most common reasons a transaction is flagged as fraud?",
# MAGIC         "Which channel has the highest fraud rate?",
# MAGIC         "Show fraud loss and recovery by fraud type",
# MAGIC         "What percentage of alerts are true positives?",
# MAGIC         "List transactions with composite_risk_score above 80",
# MAGIC     ]
# MAGIC }
# MAGIC
# MAGIC print(json.dumps(GENIE_SPACE_CONFIG, indent=2))

# COMMAND ----------

# MAGIC %md ### Create Genie Space
# MAGIC
# MAGIC response = requests.post(
# MAGIC     url=f"https://{WORKSPACE_HOST}/api/2.0/genie/spaces",
# MAGIC     headers=HEADERS,
# MAGIC     json=GENIE_SPACE_CONFIG
# MAGIC )
# MAGIC
# MAGIC if response.status_code in [200, 201]:
# MAGIC     result      = response.json()
# MAGIC     space_id    = result.get("id", result.get("space_id", "N/A"))
# MAGIC     space_url   = f"https://{WORKSPACE_HOST}/genie/spaces/{space_id}"
# MAGIC     print(f"✅ Genie Space created successfully!")
# MAGIC     print(f"   Space ID : {space_id}")
# MAGIC     print(f"   URL      : {space_url}")
# MAGIC     displayHTML(f'<a href="{space_url}" target="_blank">👉 Open Genie Space</a>')
# MAGIC else:
# MAGIC     print(f"❌ Failed: {response.status_code}")
# MAGIC     print(response.text)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Genie Space Summary
# MAGIC
# MAGIC | Setting | Value |
# MAGIC |---------|-------|
# MAGIC | **Name** | 🏦 Banking Fraud Detection Control Tower |
# MAGIC | **Tables** | 9 Gold/Silver tables |
# MAGIC | **Sample Questions** | 10 pre-loaded |
# MAGIC | **Date Range** | Apr 1, 2025 – Mar 31, 2026 |
# MAGIC | **Catalog** | banking_demo (schemas: bronze, silver, gold) |
# MAGIC
# MAGIC ### Sample Genie Questions to Try
# MAGIC 1. *"Show me the fraud rate by month"*
# MAGIC 2. *"Which customers have risk_score > 80 and have had fraud?"*
# MAGIC 3. *"What is the false positive rate by alert rule?"*
# MAGIC 4. *"Explain why transaction TXN00000042 was flagged"*
# MAGIC 5. *"What is total fraud loss by country?"*

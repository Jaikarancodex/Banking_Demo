# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - ML Model
# MAGIC ## Banking Fraud Detection — XGBoost + SHAP Explainability

# COMMAND ----------

# DBTITLE 1,Install Dependencies
# MAGIC %pip install mlflow xgboost shap typing_extensions --upgrade --quiet

# COMMAND ----------

CATALOG = "banking_demo"
SCHEMA  = "gold"
spark.sql(f"USE {CATALOG}.{SCHEMA}")

import mlflow
import mlflow.xgboost
import xgboost as xgb
import shap
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.metrics import (classification_report, roc_auc_score,
                             precision_score, recall_score, f1_score,
                             confusion_matrix)
from pyspark.sql import functions as F

print("Loading ML features...")
features_df = spark.table(f"{CATALOG}.{SCHEMA}.gold_ml_features")

# COMMAND ----------

# ── FEATURE SELECTION ────────────────────────────────────────────────────────
### Feature columns for XGBoost

FEATURE_COLS = [
    "amount", "txn_hour", "is_international", "is_high_value",
    "is_odd_hour", "is_declined", "is_round_amount",
    "txn_count_1h", "txn_count_24h", "txn_count_7d",
    "amount_sum_1h", "amount_sum_24h", "amount_avg_7d",
    "amount_zscore", "geo_risk_score", "channel_risk",
    "composite_risk_score", "is_high_velocity",
    "risk_score",           # customer risk
]
TARGET = "label"

pdf = (
    features_df
    .select(["transaction_id"] + FEATURE_COLS + [TARGET])
    .fillna(0)
    .toPandas()
)

X = pdf[FEATURE_COLS]
y = pdf[TARGET]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y)

print(f"Train: {len(X_train):,} | Test: {len(X_test):,}")
print(f"Fraud rate in train: {y_train.mean():.2%}")

# COMMAND ----------

# ── TRAIN XGBOOST ────────────────────────────────────────────────────────────────────────
### Train XGBoost Fraud Classifier
from mlflow.models import infer_signature

mlflow.set_experiment("/Shared/banking_fraud_detection")

with mlflow.start_run(run_name="xgboost_fraud_v1") as run:
    params = {
        "n_estimators":    300,
        "max_depth":       6,
        "learning_rate":   0.05,
        "subsample":       0.8,
        "colsample_bytree":0.8,
        "scale_pos_weight": (y_train == 0).sum() / (y_train == 1).sum(),  # handle imbalance
        "eval_metric":     "aucpr",
        "use_label_encoder": False,
        "random_state":    42,
    }
    mlflow.log_params(params)

    model = xgb.XGBClassifier(**params)
    model.fit(X_train, y_train,
              eval_set=[(X_test, y_test)],
              verbose=50)

    y_pred       = model.predict(X_test)
    y_pred_proba = model.predict_proba(X_test)[:,1]

    metrics = {
        "roc_auc":   roc_auc_score(y_test, y_pred_proba),
        "precision": precision_score(y_test, y_pred),
        "recall":    recall_score(y_test, y_pred),
        "f1":        f1_score(y_test, y_pred),
    }
    mlflow.log_metrics(metrics)

    signature = infer_signature(X_train, model.predict(X_train))
    mlflow.xgboost.log_model(model, "fraud_xgb_model",
                              signature=signature,
                              input_example=X_train.head(5),
                              registered_model_name="banking_fraud_detector")

    print("\n📊 Model Performance:")
    for k, v in metrics.items():
        print(f"   {k:<12}: {v:.4f}")
    print("\n", classification_report(y_test, y_pred, target_names=["Legit","Fraud"]))

RUN_ID = run.info.run_id

# COMMAND ----------

# ── SHAP EXPLAINABILITY ──────────────────────────────────────────────────────────────────────
## SHAP — Why was this transaction flagged?

# Use XGBoost's native SHAP computation (bypasses SHAP library loader bug)
sample_X = X_test.sample(n=min(5000, len(X_test)), random_state=42)
dmatrix  = xgb.DMatrix(sample_X, feature_names=FEATURE_COLS)

# pred_contribs=True returns SHAP values + bias (last column)
raw_contribs = model.get_booster().predict(dmatrix, pred_contribs=True)
shap_values  = raw_contribs[:, :-1]  # drop bias column

# Build SHAP explanation table
shap_df = pd.DataFrame(shap_values, columns=FEATURE_COLS)
shap_df["transaction_id"] = pdf.loc[sample_X.index, "transaction_id"].values
shap_df["predicted_fraud_prob"] = model.predict_proba(sample_X)[:,1]
shap_df["predicted_label"]      = model.predict(sample_X)

# Top 3 reasons per transaction
def top_reasons(row, n=3):
    feats = {col: abs(row[col]) for col in FEATURE_COLS}
    top   = sorted(feats.items(), key=lambda x: x[1], reverse=True)[:n]
    return ", ".join([f"{k}({row[k]:+.3f})" for k, _ in top])

shap_df["top_fraud_drivers"] = shap_df.apply(top_reasons, axis=1)

shap_spark = spark.createDataFrame(shap_df)
shap_spark.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.gold_shap_explanations")
print(f"✅ gold_shap_explanations: {shap_spark.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔍 Sample Explanation
# MAGIC > **Transaction TXN00000042** flagged as fraud (prob=0.94)
# MAGIC > **Top reasons:**
# MAGIC > 1. `amount_zscore` = +4.7 → Amount is 4.7 standard deviations above customer's 7-day average
# MAGIC > 2. `txn_count_1h` = +3.2 → 8 transactions in the last hour (high velocity)
# MAGIC > 3. `geo_risk_score` = +2.1 → International transaction at 2 AM

# COMMAND ----------

# ── SCORE ALL TRANSACTIONS ───────────────────────────────────────────────────
### Score all transactions and write predictions

import mlflow.pyfunc
loaded_model = mlflow.xgboost.load_model(f"runs:/{RUN_ID}/fraud_xgb_model")

all_pdf = features_df.select(["transaction_id"] + FEATURE_COLS).fillna(0).toPandas()
all_pdf["fraud_probability"] = loaded_model.predict_proba(all_pdf[FEATURE_COLS])[:,1]
all_pdf["fraud_prediction"]  = loaded_model.predict(all_pdf[FEATURE_COLS])
all_pdf["risk_tier"] = pd.cut(all_pdf["fraud_probability"],
                               bins=[0,.3,.6,.85,1.0],
                               labels=["Low","Medium","High","Critical"])

predictions_spark = spark.createDataFrame(
    all_pdf[["transaction_id","fraud_probability","fraud_prediction","risk_tier"]])
predictions_spark.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.gold_fraud_predictions")
print(f"✅ gold_fraud_predictions: {predictions_spark.count():,} rows")

# COMMAND ----------

# MAGIC %md ### ✅ Model Training Complete
# MAGIC - XGBoost model registered in MLflow Model Registry
# MAGIC - SHAP explanations written to `gold_shap_explanations`
# MAGIC - All transaction predictions in `gold_fraud_predictions`

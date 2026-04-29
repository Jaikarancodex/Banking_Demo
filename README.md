
<div style="background: linear-gradient(135deg, #0D1117 0%, #161B22 100%); padding: 40px; border-radius: 12px; border: 1px solid #30363D; margin-bottom: 20px;">

<h1 style="text-align: center; color: #E6EDF3; margin-bottom: 5px;"> Banking — Suspicious Activity Detection</h1>
<p style="text-align: center; color: #8B949E; font-size: 16px; margin-top: 0;">End-to-End Risk Intelligence & Explainability on Databricks Lakehouse</p>

</div>

---

## Business Use Case

Banks process **millions of transactions daily** across multiple channels — ATMs, online banking, mobile apps, POS terminals, and wire transfers. Hidden within this volume are **suspicious patterns** that signal money laundering, account takeover, identity theft, and payment manipulation.

Traditional rule-based systems catch obvious cases but miss sophisticated schemes. Modern banking requires a platform that can:

> **Ingest** transactions in real-time → **Detect** suspicious behavior using ML + rules → **Score** risk dynamically → **Explain** alerts to analysts → **Prioritize** cases for investigation

This demo simulates a **production banking environment** where:

- Transactions stream into Databricks via Kafka / IoT-style feeds
- A **suspicious pattern appears** — e.g., multiple small deposits (\$900, \$850, \$950) followed by one large withdrawal (\$3,500) — classic **structuring behavior**
- The **ML model flags the anomaly** — velocity features spike, amount z-score exceeds 2.8σ
- The **risk score increases dynamically** from Low to Critical
- The **system generates an alert** routed to the analyst queue
- **GenAI explains** the alert: *"This account shows structuring behavior — 5 deposits under reporting threshold followed by rapid consolidation withdrawal. Velocity 5x above normal. Cross-channel activity in 4 hours."*
- The **Dashboard** shows high-risk accounts, alert queues, and per-transaction explanations

---

## The Problem We Are Solving

<table style="width:100%; border-collapse: collapse;">
<tr style="border-bottom: 2px solid #30363D;">
<td style="padding: 15px; width: 5%;"><h3>⚠️</h3></td>
<td style="padding: 15px; width: 30%;"><strong>Detection Gap</strong></td>
<td style="padding: 15px;">Banks cannot detect suspicious transaction patterns in real-time. Legacy batch systems process transactions hours or days later — by then, funds have already moved. Sophisticated schemes like structuring, layering, and rapid cross-channel movement evade static rules entirely.</td>
</tr>
<tr style="border-bottom: 2px solid #30363D;">
<td style="padding: 15px;"><h3>🚨</h3></td>
<td style="padding: 15px;"><strong>Alert Fatigue</strong></td>
<td style="padding: 15px;">Rule-only systems generate <strong>90%+ false positive rates</strong>. Analysts waste time investigating legitimate transactions while real suspicious activity gets buried. This leads to missed cases, regulatory fines, and burnout.</td>
</tr>
<tr style="border-bottom: 2px solid #30363D;">
<td style="padding: 15px;"><h3>🔍</h3></td>
<td style="padding: 15px;"><strong>Explainability Deficit</strong></td>
<td style="padding: 15px;">Black-box ML models flag transactions but <strong>cannot explain why</strong>. Regulators (GDPR Article 22, SR 11-7, BSA/AML) require banks to justify every decision. Without explainability, models cannot be deployed in production.</td>
</tr>
<tr>
<td style="padding: 15px;"><h3>📊</h3></td>
<td style="padding: 15px;"><strong>Investigation Bottleneck</strong></td>
<td style="padding: 15px;">Analysts lack a unified view of customer risk, transaction history, alert context, and case status. They toggle between 5-10 systems. No prioritization means critical cases wait in the same queue as low-risk noise.</td>
</tr>
</table>

---

## Key Performance Indicators (KPIs)

These are the metrics this platform tracks to measure detection effectiveness, operational efficiency, and compliance readiness:

### Detection & Risk KPIs

| KPI | Definition | Target | Source Table |
|---|---|---|---|
| **Suspicious Activity Rate** | % of total transactions flagged as suspicious | < 5% (lower = better precision) | `gold_daily_fraud_summary` |
| **High-Risk Account Count** | Customers with `is_high_risk = true` | Monitor trend (should decrease over time) | `gold_customer_360` |
| **Risk Tier Distribution** | Breakdown of Critical / High / Medium / Low tiers | Majority should be Low | `gold_fraud_predictions` |
| **Daily Suspicious Volume** | Count of flagged transactions per day by channel | Track for spikes and seasonality | `gold_daily_fraud_summary` |

### Operational Efficiency KPIs

| KPI | Definition | Target | Source Table |
|---|---|---|---|
| **False Positive Rate** | Alerts confirmed as legitimate / total alerts | < 50% (industry avg: 90%+) | `gold_alert_rule_performance` |
| **Alert-to-Case Ratio** | System alerts generated vs confirmed suspicious cases | < 10:1 | `gold_alert_rule_performance` |
| **Mean Time to Detect (MTTD)** | Hours from suspicious activity to system detection | < 1 hour (real-time goal) | `silver_fraud_labels` |
| **Mean Time to Resolve (MTTR)** | Hours from alert creation to analyst resolution | < 24 hours | `gold_alert_rule_performance` |
| **Rule Performance Score** | Per-rule true positive rate and false positive rate | Retire rules with FP > 70% | `gold_alert_rule_performance` |

### Financial Impact KPIs

| KPI | Definition | Target | Source Table |
|---|---|---|---|
| **Total Loss Amount** | Sum of confirmed losses from suspicious activity | Minimize (\$0 ideal) | `silver_fraud_labels` |
| **Loss Recovery Rate** | `recovered_amount / loss_amount` | > 60% | `silver_fraud_labels` |
| **Loss by Activity Type** | Breakdown of losses by category (structuring, account takeover, etc.) | Identify top contributors | `silver_fraud_labels` |

### Compliance & Governance KPIs

| KPI | Definition | Target | Source Table |
|---|---|---|---|
| **Compliance Flag Rate** | % of audit actions flagged for compliance review | Track and review all flags | `silver_audit_logs` |
| **Explainability Coverage** | % of flagged transactions with SHAP explanations | 100% (regulatory requirement) | `gold_shap_explanations` |
| **Audit Trail Completeness** | All investigator actions logged with timestamps | 100% coverage | `silver_audit_logs` |

---

*The sections below provide the full technical deep-dive: architecture, data model, table catalog, pipeline notebooks, how to run, ML model details, explainability, and dashboard configuration.*

---

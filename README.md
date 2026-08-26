# 🌾 GramIQ Daily Mandi ETL Pipeline (`gramiq-mandi-daily-etl`)

Production-grade automated daily APMC Mandi price extraction, PostgreSQL ingestion, and **Microsoft Teams Adaptive Card** notification pipeline for **GramIQ Krishi MandiBhav**.

Extracts live commodity modal rates, arrivals, varieties, and grades directly from official **AGMARKNET 2.0 APIs (`api.agmarknet.gov.in`)**, validates and standardizes data, streams records into **PostgreSQL** with idempotent SHA-256 deduplication, and posts rich Adaptive Cards to Microsoft Teams.

---

## 🏗️ Architecture & Features

* **AGMARKNET 2.0 Gateway**: State-partitioned extraction for high-volume staples (Wheat, Paddy, Maize, Chana, Mustard, Soyabean, Cotton, Onion, Potato, Tomato, Turmeric, Banana).
* **Fail-Closed Validation**: Validates $P_{\min} \le P_{\text{modal}} \le P_{\max}$ and standardizes units to `INR/Quintal` and `Tonnes`.
* **Idempotency & Zero Duplicates**: Calculates a unique SHA-256 `observation_hash` for each market/commodity/trade_date record and executes `ON CONFLICT (observation_hash) DO UPDATE`.
* **PostgreSQL Planner Warmup**: Automatically triggers `ANALYZE mandi_observations;` post-batch ingestion to keep B-tree index scans fast.
* **Microsoft Teams Adaptive Cards (v1.4)**: Real-time telemetry cards dispatched directly to your Teams channel/group chat on every pipeline run.
* **Serverless Scheduled Automation**: Automated via GitHub Actions daily at **7:00 PM IST (13:30 UTC)** and **12:00 AM IST (18:30 UTC)**.

---

## ⚙️ GitHub Actions Setup Runbook

### 1. Repository Secrets Configuration
Navigate to your GitHub repository:
👉 **[https://github.com/harsh-gramiq/gramiq-mandi-daily-etl/settings/secrets/actions](https://github.com/harsh-gramiq/gramiq-mandi-daily-etl/settings/secrets/actions)**

Click **New repository secret** and add the following:

| Secret Name | Description | Example / Format |
| :--- | :--- | :--- |
| `PRODUCTION_DB_HOST` | Production PostgreSQL IP / Host | `34.100.185.77` |
| `PRODUCTION_DB_PORT` | PostgreSQL Port | `5432` |
| `PRODUCTION_DB_NAME` | Target database name | `app_production` |
| `PRODUCTION_DB_USERNAME` | Database username | `postgres` |
| `PRODUCTION_DB_PASSWORD` | Database password | *[Your Secure DB Password]* |
| `TEAMS_WEBHOOK_URL` | Microsoft Teams Incoming Webhook URL | `https://your-tenant.webhook.office.com/...` |

---

## 💬 How to Get Microsoft Teams Webhook URL

### Option 1: Microsoft Teams Channel Incoming Webhook
1. Open **Microsoft Teams** and go to your target Channel (or Group Chat).
2. Click the `•••` (More options) next to the channel name $\rightarrow$ **Connectors** (or **Workflows**).
3. Search for **Incoming Webhook** $\rightarrow$ Click **Add** / **Configure**.
4. Name it `GramIQ Mandi ETL Bot` and upload a wheat/leaf icon.
5. Copy the generated Webhook URL and save it as `TEAMS_WEBHOOK_URL` in GitHub Secrets.

### Option 2: Power Automate Flow ("Post card when a webhook request is received")
1. In Teams or Power Automate, create an automated cloud flow triggered by **"When a Teams webhook request is received"**.
2. Add action: **"Post card in a chat or channel"** using the dynamic body.
3. Copy the HTTP POST URL into GitHub Secrets as `TEAMS_WEBHOOK_URL`.

---

## 🚀 Manual Pipeline Execution (GitHub Actions)

You can trigger a live ingestion anytime from GitHub without code changes:

1. Open **[https://github.com/harsh-gramiq/gramiq-mandi-daily-etl/actions](https://github.com/harsh-gramiq/gramiq-mandi-daily-etl/actions)**.
2. Select **🌾 GramIQ MandiBhav — Daily National ETL & Teams Sync**.
3. Click **Run workflow** $\rightarrow$ Optionally enter a specific `trade_date` (e.g. `2026-08-25`) or toggle `dry_run`.
4. Click **Run workflow**.

---

## 💻 Local Testing Commands

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Test Teams Adaptive Card generation & webhook
python main.py --test-card

# 3. Dry run extraction for specific date (prints Adaptive Card JSON)
python main.py --date 2026-08-25 --dry-run --print-card

# 4. Ingest today's live market data into PostgreSQL
python main.py
```

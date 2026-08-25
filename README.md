# 🌾 GramIQ Daily Mandi ETL Pipeline (`gramiq-mandi-daily-etl`)

Production-grade automated daily APMC Mandi price extraction and ingestion pipeline for **GramIQ Krishi MandiBhav**.

Extracts live commodity modal rates, arrivals, varieties, and grades directly from official **DMI AGMARKNET 2.0 APIs** and streams them into **PostgreSQL** with idempotent SHA-256 deduplication.

---

## 🏗️ Architecture & Features

* **Zero Gateway Timeouts**: Uses state-partitioned queries for high-volume staples (Wheat, Cotton, Paddy, Soyabean, Mustard, Chana, Maize, Onion, Potato, Tomato).
* **Fail-Closed Validation**: Validates that all prices satisfy $P_{\min} \le P_{\text{modal}} \le P_{\max}$ and standardizes units to `INR/Quintal` and `Tonnes`.
* **Idempotency & Zero Duplicates**: Calculates a unique SHA-256 `observation_hash` for each market/commodity/trade_date record and executes `ON CONFLICT (observation_hash) DO UPDATE`.
* **Serverless Daily Cron**: Automatically scheduled via GitHub Actions to run every day at **7:00 PM IST (13:30 UTC)**.
* **Structured JSON Logging**: Cloud-ready logging schema with ISO 8601 timestamps.

---

## 🚀 Quick Start (Local Development)

### 1. Clone & Setup Virtual Environment
```bash
git clone https://github.com/<your-org-or-user>/gramiq-mandi-daily-etl.git
cd gramiq-mandi-daily-etl

python -m venv .venv
# On Linux/macOS:
source .venv/bin/activate
# On Windows PowerShell:
.\.venv\Scripts\Activate.ps1

pip install -r requirements.txt
```

### 2. Configure Environment Variables
Copy `.env.example` to `.env` and fill in your PostgreSQL credentials:
```bash
cp .env.example .env
```

### 3. Run Ingestion Engine
```bash
# Ingest today's live data
python main.py

# Ingest a specific trade date
python main.py --date 2026-08-25

# Dry run (extract and validate without database write)
python main.py --dry-run
```

---

## ⚙️ GitHub Actions Scheduled Automation Setup

To enable automated daily ingestion on GitHub Cloud:

1. Push this repository to GitHub under your account (e.g. `harsh@gramiq.ai`).
2. Go to **Settings** $\rightarrow$ **Secrets and variables** $\rightarrow$ **Actions** $\rightarrow$ **New repository secret**.
3. Add the following secrets:
   * `DATABASE_URL`: Full PostgreSQL connection string (`postgresql://postgres:<password>@<host>:5432/<dbname>`), **OR**:
   * `POSTGRES_HOST`: Database host IP or domain.
   * `POSTGRES_PORT`: `5432`
   * `POSTGRES_DB`: Database name (`app_production` or `gramiq_mandi`)
   * `POSTGRES_USER`: Database user (`postgres`)
   * `POSTGRES_PASSWORD`: Database password
4. The workflow in `.github/workflows/daily_mandi_ingest.yml` will automatically trigger every day at **7:00 PM IST** (13:30 UTC), and you can also manually trigger it anytime via the **Actions** tab $\rightarrow$ **Run workflow**.

---

## 📊 PostgreSQL Database Schema

The pipeline automatically creates the table and analytical views on first run:

```sql
CREATE TABLE IF NOT EXISTS mandi_observations (
    observation_hash VARCHAR(64) PRIMARY KEY,
    source VARCHAR(64) NOT NULL,
    trade_date DATE NOT NULL,
    state VARCHAR(128) NOT NULL,
    district VARCHAR(128) NOT NULL,
    market VARCHAR(128) NOT NULL,
    commodity VARCHAR(128) NOT NULL,
    variety VARCHAR(128),
    grade VARCHAR(64),
    raw_min_price NUMERIC(12,2) NOT NULL,
    raw_modal_price NUMERIC(12,2) NOT NULL,
    raw_max_price NUMERIC(12,2) NOT NULL,
    raw_price_unit VARCHAR(32) NOT NULL,
    normalized_min_price_qtl NUMERIC(12,2) NOT NULL,
    normalized_modal_price_qtl NUMERIC(12,2) NOT NULL,
    normalized_max_price_qtl NUMERIC(12,2) NOT NULL,
    raw_arrival_quantity NUMERIC(12,2),
    raw_arrival_unit VARCHAR(32),
    quality_status VARCHAR(32) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

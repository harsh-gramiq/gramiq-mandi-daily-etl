# GramIQ MandiBhav Daily ETL

Production Python pipeline for collecting APMC mandi prices and arrivals from the official AGMARKNET 2.0 gateway, validating observations, loading PostgreSQL, and publishing Microsoft Teams Adaptive Card reports.

The pipeline is deliberately fail-closed: invalid prices are rejected, state attribution comes from the requested AGMARKNET partition, and database writes use an observation hash for idempotency.

## What it does

- Queries configured commodity/state partitions concurrently through a pooled HTTP client.
- Collects a rolling lookback window so late mandi submissions can be reconciled.
- Normalizes prices to INR per quintal and arrivals to tonnes.
- Validates `min_price <= modal_price <= max_price`.
- Upserts observations into PostgreSQL and refreshes the summary table.
- Calculates real volume, state, date, and inter-mandi spread analytics.
- Generates a Gemini brief when configured, with a quantitative fallback when unavailable.
- Sends either a preliminary or final Teams report.

## Repository layout

```text
.
├── app/
│   ├── analytics.py       # Pure market analytics
│   └── config.py          # Commodity and AGMARKNET state partitions
├── main.py               # CLI and pipeline compatibility entrypoint
├── test_main.py          # Deterministic regression tests
├── .github/workflows/
│   ├── ci.yml             # Tests and syntax checks
│   └── daily_mandi_ingest.yml
└── LOGS_AUDIT_REPORT.md   # Historical ingestion audit and findings
```

The remaining database, extractor, Gemini, and Teams functions are currently kept in `main.py` for backward compatibility. They are the next safe extraction boundaries as the project grows.

## Requirements

- Python 3.11+
- PostgreSQL with the GramIQ mandi schema
- AGMARKNET 2.0 network access
- Optional Google Gemini API key
- Optional Microsoft Teams webhook

Install dependencies in a virtual environment:

```bash
python -m venv .venv
# Linux/macOS: source .venv/bin/activate
# Windows: .venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

## Configuration

Copy `.env.example` to `.env` for local development. Never commit `.env` or credentials.

Required for a live database run:

| Variable | Purpose |
|---|---|
| `PRODUCTION_DB_HOST` | PostgreSQL hostname |
| `PRODUCTION_DB_PORT` | PostgreSQL port |
| `PRODUCTION_DB_NAME` | Database name |
| `PRODUCTION_DB_USERNAME` | Database user |
| `PRODUCTION_DB_PASSWORD` | Database password |

Optional:

| Variable | Purpose |
|---|---|
| `TEAMS_WEBHOOK_URL` | Teams webhook destination |
| `GEMINI_API_KEY_OG` or `GEMINI_API_KEY` | Gemini market brief generation |
| `GEMINI_KEY_POOL` | Comma-separated Gemini key pool fallback |

GitHub Actions uses repository secrets with the same names.

## Run locally

Run deterministic tests:

```bash
python test_main.py
python -m py_compile main.py app/*.py test_main.py
```

Run a dry extraction and print the GitHub-style summary:

```bash
python main.py --date 2026-08-29 --lookback-days 3 --dry-run --print-card
```

Run a live ingestion:

```bash
python main.py --lookback-days 3
```

Useful options:

```text
--date YYYY-MM-DD       Trade date; defaults to today
--lookback-days N       Rolling extraction window; defaults to 3
--workers N             Concurrent HTTP workers; defaults to 8
--dry-run               Skip PostgreSQL writes
--run-mode preliminary  Label the Teams report as an early snapshot
--run-mode final        Label the Teams report as authoritative
--print-card            Write a Markdown summary to stdout
```

## Scheduled reports

The workflow runs Monday through Saturday:

| UTC | IST | Report |
|---|---|---|
| 13:30 | 19:00 | Preliminary Market Update; late state submissions may still arrive |
| 18:30 | 00:00 | Final Reconciliation Report; authoritative rolling snapshot |

Manual workflow runs default to the final report format. Both runs are safe to repeat because observations are upserted idempotently.

## Data quality and state attribution

AGMARKNET does not consistently return `stateName` in every market block. The requested `stateId` is therefore authoritative and is resolved through `app/config.py`. This prevents missing or stale response metadata from collapsing national analytics into a single blank state.

The pipeline rejects non-positive prices and inverted price bounds. Check the structured workflow logs and the Teams state breakdown when coverage looks suspicious.

## CI and release checklist

Every change should pass:

```bash
python test_main.py
python -m py_compile main.py app/*.py test_main.py
```

Before a production run, confirm that database, webhook, and optional Gemini secrets exist in GitHub Actions. Do not print secret values, include them in URLs, or add `.env` files to commits.

## Security and operations

- Treat webhook URLs and database credentials as secrets.
- Rotate credentials immediately if they appear in logs or commits.
- Use dry-run mode to validate extraction without database writes.
- Review state counts, date counts, accepted rows, and database upsert results—not only the process exit code.
- See `LOGS_AUDIT_REPORT.md` for the previous one-state incident and remediation history.

## License

No license has been declared yet. Until the repository owner adds one, all rights remain with the copyright holder.

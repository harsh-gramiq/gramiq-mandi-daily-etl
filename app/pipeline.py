"""GramIQ MandiBhav National Daily ETL & 7-Day Rolling Reconciliation Orchestrator."""

from datetime import date
import json
import sys
import time
from typing import Any, Optional

from app.analytics import compute_clean_market_analytics
from app.config import Config
from app.extractors import extract_national_agmarknet_parallel
from app.notifications import (
    build_adaptive_card,
    dispatch_card_to_teams,
    generate_gemini_market_brief,
    write_github_step_summary,
)
from app.storage import upsert_to_postgresql
from app.storage.lakehouse import export_to_lakehouse

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


def run_pipeline(
    target_date: str = "",
    lookback_days: int = Config.DEFAULT_LOOKBACK_DAYS,
    workers: int = Config.DEFAULT_WORKERS,
    dry_run: bool = False,
    print_card: bool = False,
    matrix_path: Optional[str] = None,
    export_parquet: bool = True,
) -> dict[str, Any]:
    """
    Executes the full national MandiBhav ETL and 7-day rolling reconciliation workflow:
    1. Multi-threaded block harvesting with round-robin state scheduling & backoff
    2. Statistical IQR/Median outlier scrubbing, Price Velocity, and Arbitrage corridors
    3. Dual-layer storage: Idempotent PostgreSQL/Supabase write & Partitioned Parquet Lakehouse
    4. Authentic Gemini AI market brief generation with agronomic guardrails
    5. Microsoft Teams Adaptive Card v1.5 construction with data gap container & dispatch
    6. GitHub Actions step summary markdown generation
    """
    t_start = time.time()
    effective_date = target_date if target_date else date.today().isoformat()

    print("=" * 85)
    print(f"🌾 [GramIQ MandiBhav] National Daily ETL & 7-Day Rolling Reconciliation")
    print(f"   Target Date   : {effective_date}")
    print(f"   Lookback Days : {lookback_days} days")
    print(f"   Concurrency   : {workers} workers (Paced & State-Interleaved)")
    print(f"   Execution Mode: {'DRY RUN (No DB Write)' if dry_run else 'PRODUCTION LIVE'}")
    print("=" * 85)

    # 1. High-speed multi-threaded extraction with rate-limit resilience
    records, telemetry = extract_national_agmarknet_parallel(
        target_date_iso=effective_date,
        lookback_days=lookback_days,
        max_workers=workers,
        matrix_path=matrix_path,
    )

    # 2. Statistical Outlier Scrubbing & Clean Arbitrage Analytics
    print("\n🔍 Computing clean market analytics, price velocity, and outlier-scrubbed spreads...")
    metrics = compute_clean_market_analytics(records, effective_date, telemetry=telemetry)
    print(
        f"   + Validated Rows: {metrics['total_rows']:,} across {metrics['active_commodities']} commodities in {metrics['active_states']} states"
    )
    print(f"   + Clean Spreads Computed: {len(metrics['spreads'])} commodities")
    print(f"   + Arbitrage Corridors Identified: {len(metrics['arbitrage_corridors'])} routes")
    print(f"   + Pipeline Reliability: {metrics['tasks_with_data']}/{metrics['total_tasks_probed']} tasks active | {metrics['tasks_market_closed']} closed/off-season | {metrics['tasks_failed']} failed")

    # 3. Dual-Layer Storage: Lakehouse Parquet & PostgreSQL Database
    lakehouse_info = {}
    if export_parquet and records:
        print("\n📦 Exporting validated dataset to Partitioned Parquet Lakehouse...")
        lakehouse_info = export_to_lakehouse(records, effective_date)

    db_upserted = 0
    if not dry_run and records:
        print("\n💾 Ingesting records into Production Database...")
        db_upserted, _ = upsert_to_postgresql(records)
    elif dry_run:
        print("\n💾 [DRY RUN] Skipped PostgreSQL write.")

    # 4. Authentic Gemini Market Intelligence Brief
    print("\n🤖 Generating authentic AI market brief...")
    ai_brief = generate_gemini_market_brief(metrics, effective_date)
    print(f"   Brief: \"{ai_brief}\"")

    # 5. Adaptive Card v1.5 Construction & Dispatch
    elapsed = time.time() - t_start
    card = build_adaptive_card(metrics, ai_brief, effective_date, elapsed)

    if print_card:
        print("\n--- TEAMS ADAPTIVE CARD JSON ---")
        print(json.dumps(card, indent=2))

    print("\n🚀 Dispatching Adaptive Card to Teams...")
    dispatch_card_to_teams(card)

    # 6. GitHub Step Summary Output (if running in CI)
    write_github_step_summary(metrics, ai_brief, effective_date, lookback_days, elapsed)

    print("=" * 85)
    print(f"✅ DAILY RECONCILIATION COMPLETED in {elapsed:.1f}s | Status: 🟢 SUCCESS")
    print("=" * 85)

    return {
        "status": "SUCCESS",
        "target_date": effective_date,
        "lookback_days": lookback_days,
        "elapsed_s": round(elapsed, 2),
        "total_records": len(records),
        "metrics": metrics,
        "ai_brief": ai_brief,
        "card": card,
        "db_upserted": db_upserted,
        "telemetry": telemetry,
    }

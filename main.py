#!/usr/bin/env python3
"""
GramIQ MandiBhav All-Crops National Daily ETL & 7-Day Rolling Reconciliation CLI.

Entrypoint for scheduled GitHub Actions runners and manual backfill operations.
"""

import argparse
import sys
from app.pipeline import run_pipeline

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


def main() -> int:
    parser = argparse.ArgumentParser(
        description="GramIQ MandiBhav All-Crops National Daily ETL & 7-Day Reconciliation"
    )
    parser.add_argument("--date", default="", help="Target trade date (YYYY-MM-DD, defaults to today)")
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=7,
        help="Rolling lookback window in calendar days (default: 7)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=24,
        help="Worker thread concurrency (default: 24)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Execute extraction and analytics without database writes",
    )
    parser.add_argument("--print-card", action="store_true", help="Print card JSON to stdout")
    parser.add_argument("--matrix-path", default=None, help="Custom path to active national matrix JSON")
    args = parser.parse_args()

    result = run_pipeline(
        target_date=args.date,
        lookback_days=args.lookback_days,
        workers=args.workers,
        dry_run=args.dry_run,
        print_card=args.print_card,
        matrix_path=args.matrix_path,
    )

    return 0 if result.get("status") == "SUCCESS" else 1


if __name__ == "__main__":
    sys.exit(main())

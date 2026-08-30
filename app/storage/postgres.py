"""PostgreSQL / Supabase Ingestion & Idempotent Summary Refresh Engine."""

import os
import sys
from typing import Any

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


def upsert_to_postgresql(records: list[dict[str, Any]]) -> tuple[int, int]:
    """
    Idempotently upserts records into PostgreSQL / Supabase with ON CONFLICT (observation_hash) DO UPDATE.
    Returns (inserted_or_updated_count, quarantined_count).
    """
    if not records:
        return 0, 0

    db_url = os.environ.get("DATABASE_URL") or os.environ.get("PRODUCTION_DB_URL")
    if not db_url:
        print("  [PostgreSQL] ⚠️ DATABASE_URL not set. Skipping cloud database write.")
        return len(records), 0

    try:
        import psycopg2
        import psycopg2.extras
    except (ImportError, ModuleNotFoundError):
        print("  [PostgreSQL] ⚠️ psycopg2 not installed. Skipping cloud database write.")
        return len(records), 0

    print(f"  [PostgreSQL] Connecting to production database for batched upsert ({len(records):,} records)...")

    upsert_sql = """
    INSERT INTO mandi_observations (
        observation_hash, source, trade_date, state, district, market,
        commodity, variety, grade, raw_min_price, raw_modal_price, raw_max_price,
        raw_price_unit, normalized_min_price_qtl, normalized_modal_price_qtl,
        normalized_max_price_qtl, raw_arrival_quantity, raw_arrival_unit,
        quality_status, created_at
    ) VALUES (
        %(observation_hash)s, %(source)s, %(trade_date)s, %(state)s, %(district)s, %(market)s,
        %(commodity)s, %(variety)s, %(grade)s, %(raw_min_price)s, %(raw_modal_price)s, %(raw_max_price)s,
        %(raw_price_unit)s, %(normalized_min_price_qtl)s, %(normalized_modal_price_qtl)s,
        %(normalized_max_price_qtl)s, %(raw_arrival_quantity)s, %(raw_arrival_unit)s,
        %(quality_status)s, %(created_at)s
    )
    ON CONFLICT (observation_hash) DO UPDATE SET
        normalized_modal_price_qtl = EXCLUDED.normalized_modal_price_qtl,
        normalized_min_price_qtl = EXCLUDED.normalized_min_price_qtl,
        normalized_max_price_qtl = EXCLUDED.normalized_max_price_qtl,
        raw_arrival_quantity = EXCLUDED.raw_arrival_quantity;
    """

    summary_refresh_sql = """
    INSERT INTO mandi_price_summary (
        apmc_id, commodity, latest_trade_date, latest_price,
        min_price_7d, max_price_7d, avg_price_7d, updated_at
    )
    SELECT 
        mo.apmc_id,
        mo.commodity,
        MAX(mo.trade_date) as latest_trade_date,
        (ARRAY_AGG(mo.normalized_modal_price_qtl ORDER BY mo.trade_date DESC))[1] as latest_price,
        MIN(mo.normalized_modal_price_qtl) as min_price_7d,
        MAX(mo.normalized_modal_price_qtl) as max_price_7d,
        ROUND(AVG(mo.normalized_modal_price_qtl)::numeric, 2) as avg_price_7d,
        NOW() as updated_at
    FROM mandi_observations mo
    WHERE mo.apmc_id IS NOT NULL 
      AND mo.trade_date >= (CURRENT_DATE - INTERVAL '7 days')
      AND mo.normalized_modal_price_qtl > 0
    GROUP BY mo.apmc_id, mo.commodity
    ON CONFLICT (apmc_id, commodity) DO UPDATE SET
        latest_trade_date = EXCLUDED.latest_trade_date,
        latest_price = EXCLUDED.latest_price,
        min_price_7d = EXCLUDED.min_price_7d,
        max_price_7d = EXCLUDED.max_price_7d,
        avg_price_7d = EXCLUDED.avg_price_7d,
        updated_at = NOW();
    """

    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(db_url)
        cursor = conn.cursor()
        psycopg2.extras.execute_batch(cursor, upsert_sql, records, page_size=1000)
        conn.commit()
        print(f"  [PostgreSQL] ✅ Successfully committed {len(records):,} observations.")

        # Trigger incremental refresh of mandi_price_summary if table exists
        try:
            print("  [PostgreSQL] Refreshing precalculated summary table (mandi_price_summary)...")
            cursor.execute(summary_refresh_sql)
            conn.commit()
            print("  [PostgreSQL] ✅ Incremental summary table refreshed successfully.")
        except Exception as summ_err:
            conn.rollback()
            print(f"  [PostgreSQL] ℹ️ mandi_price_summary refresh note: {summ_err}")

        return len(records), 0
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"  [PostgreSQL] ❌ Batch upsert failed: {e}")
        return 0, len(records)
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

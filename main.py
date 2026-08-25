import os
import sys
import json
import time
import hashlib
import logging
import argparse
import urllib.request
import urllib.parse
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, List, Any, Optional
from dotenv import load_dotenv

# Load local .env if present
load_dotenv()

# Structured JSON Logger
logging.basicConfig(
    level=logging.INFO,
    format='{"timestamp":"%(asctime)s","level":"%(levelname)s","service":"gramiq-mandi-etl","message":"%(message)s"}',
    datefmt='%Y-%m-%dT%H:%M:%SZ'
)
logger = logging.getLogger("mandi_etl")

# Canonical Agricultural Matrix Configuration
COMMODITY_MAP = {
    1: "Wheat", 2: "Paddy(Dhan)(Common)", 3: "Maize", 4: "Bengal Gram(Gram)(Whole)",
    5: "Jowar(Sorghum)", 6: "Bajra(Pearl Millet/Cumbu)", 8: "Barley(Jau)", 9: "Ragi(Finger Millet)",
    10: "Green Gram(Moong)(Whole)", 11: "Black Gram(Urd Beans)(Whole)", 12: "Mustard",
    13: "Soyabean", 14: "Groundnut", 15: "Cotton", 23: "Onion", 24: "Potato", 28: "Tomato",
    45: "Banana", 65: "Turmeric"
}

# State-level partitioned producing states for high-volume staples (Zero-504 Gateway Timeout Architecture)
PRODUCING_STATES = {
    1: [19, 34, 28, 29, 11, 20],      # Wheat: MP, UP, PB, RJ, GJ, MH
    2: [28, 34, 19, 12, 11, 20, 16],  # Paddy: PB, UP, MP, HR, GJ, MH, KA
    3: [19, 20, 16, 29, 11, 34],      # Maize: MP, MH, KA, RJ, GJ, UP
    4: [19, 20, 29, 11, 16, 34],      # Chana: MP, MH, RJ, GJ, KA, UP
    12: [29, 19, 12, 34, 11],         # Mustard: RJ, MP, HR, UP, GJ
    13: [19, 20, 29, 16, 11],         # Soyabean: MP, MH, RJ, KA, GJ
    15: [20, 11, 29, 28, 12, 19],     # Cotton: MH, GJ, RJ, PB, HR, MP
    23: [20, 19, 11, 29, 16, 34],     # Onion: MH, MP, GJ, RJ, KA, UP
    24: [34, 28, 19, 11, 20, 29],     # Potato: UP, PB, MP, GJ, MH, RJ
    28: [20, 16, 19, 11, 34, 29],     # Tomato: MH, KA, MP, GJ, UP, RJ
}

def compute_observation_hash(source: str, trade_date: str, state: str, market: str, commodity: str, variety: str, grade: str) -> str:
    token = f"{source}|{trade_date}|{state.strip().upper()}|{market.strip().upper()}|{commodity.strip().upper()}|{(variety or '').strip().upper()}|{(grade or '').strip().upper()}"
    return hashlib.sha256(token.encode('utf-8')).hexdigest()

def extract_agmarknet_live(target_date: str) -> List[Dict[str, Any]]:
    """Extracts live daily APMC arrivals and modal prices from DMI AGMARKNET 2.0 API."""
    try:
        d_obj = date.fromisoformat(target_date)
        dmi_date = d_obj.strftime("%d-%b-%Y")
    except Exception:
        d_obj = date.today()
        dmi_date = d_obj.strftime("%d-%b-%Y")

    logger.info(f"Initiating AGMARKNET extraction for trade date: {dmi_date} ({target_date})")
    records: List[Dict[str, Any]] = []

    api_url = "https://api.agmarknet.gov.in/DMI-Report/nationalDailyReportPriceArrivalsCommodityWiseList"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://agmarknet.gov.in",
        "Referer": "https://agmarknet.gov.in/",
        "Content-Type": "application/json;charset=UTF-8"
    }

    tasks = []
    for c_id, c_name in COMMODITY_MAP.items():
        if c_id in PRODUCING_STATES:
            for s_id in PRODUCING_STATES[c_id]:
                tasks.append((c_id, str(s_id)))
        else:
            tasks.append((c_id, "100000"))

    created_at = datetime.now(timezone.utc).isoformat()

    for c_id, s_id in tasks:
        payload = json.dumps({"commodityId": str(c_id), "stateId": s_id, "tradeDate": dmi_date}).encode("utf-8")
        for attempt in range(3):
            try:
                req = urllib.request.Request(api_url, data=payload, headers=headers, method="POST")
                with urllib.request.urlopen(req, timeout=20) as resp:
                    if resp.status == 200:
                        data = json.loads(resp.read().decode("utf-8"))
                        raw_rows = data if isinstance(data, list) else data.get("data", [])
                        for r in raw_rows:
                            if not isinstance(r, dict):
                                continue
                            c_name = r.get("commodityName") or COMMODITY_MAP.get(c_id, "Unknown")
                            state = r.get("stateName") or ""
                            district = r.get("districtName") or ""
                            market = r.get("marketName") or ""
                            try:
                                modal_p = float(r.get("modalPrice") or 0)
                                min_p = float(r.get("minPrice") or modal_p)
                                max_p = float(r.get("maxPrice") or modal_p)
                                arrival = float(r.get("arrivals") or 0)
                            except (ValueError, TypeError):
                                continue
                            
                            # Validation: Fail-closed logic
                            if c_name and modal_p > 0 and (min_p <= modal_p <= max_p or min_p == modal_p):
                                var = r.get("varietyName") or "Common"
                                grd = r.get("gradeName") or "FAQ"
                                obs_hash = compute_observation_hash("agmarknet_official_v2", target_date, state, market, c_name, var, grd)
                                records.append({
                                    "observation_hash": obs_hash,
                                    "source": "agmarknet_official_v2",
                                    "trade_date": target_date,
                                    "state": state,
                                    "district": district,
                                    "market": market,
                                    "commodity": c_name,
                                    "variety": var,
                                    "grade": grd,
                                    "raw_min_price": min_p,
                                    "raw_modal_price": modal_p,
                                    "raw_max_price": max_p,
                                    "raw_price_unit": "INR/Quintal",
                                    "normalized_min_price_qtl": min_p,
                                    "normalized_modal_price_qtl": modal_p,
                                    "normalized_max_price_qtl": max_p,
                                    "raw_arrival_quantity": arrival,
                                    "raw_arrival_unit": "Tonnes",
                                    "quality_status": "accepted",
                                    "created_at": created_at
                                })
                        break
            except Exception as e:
                time.sleep(1.0 + attempt * 1.5)
        time.sleep(0.05)

    logger.info(f"Extracted {len(records)} valid records from AGMARKNET API")
    return records

def get_postgres_connection():
    import psycopg2
    # Support both connection URL and individual env parameters
    db_url = os.getenv("DATABASE_URL") or os.getenv("DIRECT_URL")
    if db_url:
        return psycopg2.connect(db_url)
    
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "postgres"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "")
    )

def setup_database_schema(conn):
    """Initializes canonical PostgreSQL tables and unified views."""
    with conn.cursor() as cur:
        cur.execute("""
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

            CREATE INDEX IF NOT EXISTS idx_mandi_lookup ON mandi_observations(commodity, market, trade_date DESC);
            CREATE INDEX IF NOT EXISTS idx_mandi_date ON mandi_observations(trade_date);
            CREATE INDEX IF NOT EXISTS idx_mandi_state ON mandi_observations(state, commodity);

            CREATE OR REPLACE VIEW v_unified_mandi_rates AS
            SELECT 
                observation_hash, source, trade_date, state, district, market,
                TRIM(REGEXP_REPLACE(market, ' (APMC)| (Mandi)| APMC| Mandi', '', 'g')) AS clean_market,
                commodity,
                CASE WHEN commodity = 'Paddy(Common)' THEN 'Paddy(Dhan)(Common)' ELSE commodity END AS clean_commodity,
                variety, grade,
                normalized_modal_price_qtl AS modal_price,
                normalized_min_price_qtl AS min_price,
                normalized_max_price_qtl AS max_price,
                raw_price_unit AS unit,
                raw_arrival_quantity AS arrival_quantity,
                raw_arrival_unit AS arrival_unit,
                quality_status, created_at
            FROM mandi_observations;
        """)
        conn.commit()
    logger.info("PostgreSQL schema and analytical views verified")

def load_records_to_postgres(records: List[Dict[str, Any]]) -> int:
    """Streams validated mandi records into PostgreSQL with ON CONFLICT idempotency."""
    if not records:
        logger.warning("No records to insert into PostgreSQL")
        return 0

    try:
        conn = get_postgres_connection()
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        raise e

    setup_database_schema(conn)

    insert_query = """
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
            raw_modal_price = EXCLUDED.raw_modal_price,
            raw_min_price = EXCLUDED.raw_min_price,
            raw_max_price = EXCLUDED.raw_max_price,
            normalized_modal_price_qtl = EXCLUDED.normalized_modal_price_qtl,
            raw_arrival_quantity = EXCLUDED.raw_arrival_quantity;
    """

    with conn.cursor() as cur:
        from psycopg2.extras import execute_batch
        execute_batch(cur, insert_query, records, page_size=1000)
        conn.commit()

    conn.close()
    logger.info(f"Successfully upserted {len(records)} records into PostgreSQL")
    return len(records)

def main():
    parser = argparse.ArgumentParser(description="GramIQ Daily AGMARKNET Ingestion Engine")
    parser.add_argument("--date", default="", help="Trade date in YYYY-MM-DD format (defaults to today)")
    parser.add_argument("--dry-run", action="store_true", help="Extract and validate without writing to database")
    args = parser.parse_args()

    t_start = time.time()
    target_date = args.date if args.date else date.today().isoformat()
    logger.info(f"Starting GramIQ Mandi Ingestion Workflow (Date: {target_date})")

    records = extract_agmarknet_live(target_date)

    if args.dry_run:
        logger.info(f"[DRY-RUN] Extracted & validated {len(records)} records. Skipping database write.")
    else:
        inserted = load_records_to_postgres(records)
        logger.info(f"Ingestion complete: {inserted} records processed in {time.time() - t_start:.2f}s")

if __name__ == "__main__":
    main()

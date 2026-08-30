"""
GramIQ MandiBhav — High-Performance AGMARKNET 2.0 Harvester
============================================================
Handles multi-threaded monthly block extractions across active national tasks
with exponential backoff jitter on HTTP 429/503 rate-limits, in-memory 7-day
rolling lookback filtering, deterministic SHA-256 hashing, and complete pipeline telemetry.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import random
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from typing import Any

from app.config import (
    AGMARKNET_HEADERS,
    AGMARKNET_MONTHLY_ENDPOINT,
    HTTP_TIMEOUT_SECONDS,
    HTTP_MAX_RETRIES,
    HTTP_RETRY_BACKOFF_BASE,
    THREAD_DISPATCH_STAGGER_S,
    DEFAULT_WORKERS,
    DEFAULT_LOOKBACK_DAYS,
)
from app.matrix import load_active_task_matrix

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


def _parse_date_str(raw_str: str) -> str:
    """Normalizes various Agmarknet date formats (DD/MM/YYYY, DD-Mon-YYYY) to YYYY-MM-DD."""
    if not raw_str:
        return ""
    raw_str = raw_str.strip()
    for fmt in ("%d/%m/%Y", "%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return ""


def parse_market_dates_payload(
    payload: dict[str, Any] | list[Any],
    task: dict[str, Any],
    lookback_dates: set[str],
) -> list[dict[str, Any]]:
    """
    Parses markets payload from AGMARKNET monthly block endpoint and extracts records
    whose trade date falls within lookback_dates.
    """
    cid = str(task.get("commodity_id", ""))
    c_name = task.get("commodity_name", "")
    s_name = task.get("state_name", "")

    records: list[dict[str, Any]] = []
    now_iso = datetime.now(timezone.utc).isoformat()

    data_body = payload.get("data", payload) if isinstance(payload, dict) else payload
    markets_list = (
        data_body.get("markets", [])
        if isinstance(data_body, dict)
        else (data_body if isinstance(data_body, list) else [])
    )

    for mkt in markets_list:
        m_name = (mkt.get("marketName") or mkt.get("MarketName") or "").strip()
        dist_name = (mkt.get("districtName") or mkt.get("DistrictName") or s_name).strip()

        for d_obj in mkt.get("dates", []):
            dt_raw = str(d_obj.get("arrivalDate") or d_obj.get("ArrivalDate") or "").strip()
            trade_date_iso = _parse_date_str(dt_raw)

            # In-memory lookback filter: zero extra network requests
            if trade_date_iso not in lookback_dates:
                continue

            for row in d_obj.get("data", []):
                try:
                    modal_p = float(row.get("modalPrice") or row.get("ModalPrice") or 0)
                    if modal_p <= 0:
                        continue
                    min_p = float(row.get("minimumPrice") or row.get("MinPrice") or modal_p)
                    max_p = float(row.get("maximumPrice") or row.get("MaxPrice") or modal_p)
                    arrivals = float(row.get("arrivals") or row.get("Arrivals") or 0)
                    variety = str(row.get("variety") or row.get("Variety") or "Common").strip()
                    grade = str(row.get("grade") or row.get("Grade") or "FAQ").strip()

                    # Deterministic SHA-256 fingerprint
                    hash_str = f"agmarknet_official_v2|{trade_date_iso}|{s_name}|{dist_name}|{m_name}|{c_name}|{variety}|{modal_p}"
                    obs_hash = hashlib.sha256(hash_str.encode("utf-8")).hexdigest()

                    records.append({
                        "observation_hash": obs_hash,
                        "source": "agmarknet_official_v2",
                        "trade_date": trade_date_iso,
                        "state": s_name,
                        "district": dist_name,
                        "market": m_name,
                        "commodity": c_name,
                        "commodity_code": cid,
                        "variety": variety,
                        "grade": grade,
                        "raw_min_price": min_p,
                        "raw_modal_price": modal_p,
                        "raw_max_price": max_p,
                        "raw_price_unit": "INR/Quintal",
                        "normalized_min_price_qtl": min_p,
                        "normalized_modal_price_qtl": modal_p,
                        "normalized_max_price_qtl": max_p,
                        "raw_arrival_quantity": arrivals,
                        "raw_arrival_unit": "Tonnes",
                        "quality_status": "accepted",
                        "created_at": now_iso,
                    })
                except (ValueError, TypeError):
                    continue

    return records


def fetch_monthly_block_task(
    task: dict[str, Any],
    target_year: int,
    target_month: int,
    lookback_dates: set[str],
    headers: dict[str, str] | None = None,
    timeout_s: int = HTTP_TIMEOUT_SECONDS,
    max_retries: int = HTTP_MAX_RETRIES,
    backoff_base: float = HTTP_RETRY_BACKOFF_BASE,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Fetches the full monthly ledger block for a given (commodityId, stateId, year, month)
    with exponential backoff jitter on 429/503 and returns (records, telemetry).
    """
    cid = str(task["commodity_id"])
    sid = str(task["state_id"])
    s_name = task.get("state_name", f"State_{sid}")
    c_name = task.get("commodity_name", f"Commodity_{cid}")
    req_headers = headers or AGMARKNET_HEADERS

    url = (
        f"{AGMARKNET_MONTHLY_ENDPOINT}"
        f"?commodityId={cid}&stateId={sid}&year={target_year}&month={target_month}&includeExcel=false"
    )

    telemetry: dict[str, Any] = {
        "commodity_id": int(cid) if cid.isdigit() else cid,
        "state_id": int(sid) if sid.isdigit() else sid,
        "commodity_name": c_name,
        "state_name": s_name,
        "status": "PENDING",
        "http_code": None,
        "retries_attempted": 0,
        "records_harvested": 0,
        "error_message": None,
    }

    for attempt in range(max_retries):
        telemetry["retries_attempted"] = attempt
        try:
            req = urllib.request.Request(url, headers=req_headers)
            with urllib.request.urlopen(req, timeout=timeout_s) as resp:
                status_code = resp.status
                telemetry["http_code"] = status_code

                if status_code == 200:
                    payload = json.loads(resp.read().decode("utf-8"))
                    recs = parse_market_dates_payload(payload, task, lookback_dates)
                    telemetry["records_harvested"] = len(recs)
                    telemetry["status"] = (
                        "OK_WITH_DATA" if recs else "OK_MARKET_CLOSED_OR_NO_ARRIVAL"
                    )
                    return recs, telemetry
                elif status_code in (404, 204):
                    telemetry["status"] = "OK_NO_DATA"
                    return [], telemetry

        except urllib.error.HTTPError as e:
            telemetry["http_code"] = e.code
            telemetry["error_message"] = f"HTTP {e.code}: {e.reason}"

            if e.code in (429, 503):
                # Upstream WAF rate limit or gateway overload: exponential backoff with jitter
                backoff = (backoff_base ** attempt) + random.uniform(0.1, 0.5)
                # Check for Retry-After header
                retry_after = e.headers.get("Retry-After") if hasattr(e, "headers") else None
                if retry_after and retry_after.isdigit():
                    backoff = max(float(retry_after), backoff)

                if attempt < max_retries - 1:
                    time.sleep(backoff)
                    continue
                else:
                    telemetry["status"] = "RATE_LIMITED_EXHAUSTED"
                    return [], telemetry
            elif e.code in (404, 204):
                telemetry["status"] = "OK_NO_DATA"
                return [], telemetry
            else:
                telemetry["status"] = f"HTTP_ERROR_{e.code}"
                return [], telemetry

        except (urllib.error.URLError, TimeoutError) as e:
            telemetry["error_message"] = f"Network Timeout/URLError: {e}"
            if attempt < max_retries - 1:
                backoff = 1.0 * (attempt + 1) + random.uniform(0.1, 0.4)
                time.sleep(backoff)
                continue
            else:
                telemetry["status"] = "TIMEOUT_EXHAUSTED"
                return [], telemetry

        except Exception as e:
            telemetry["error_message"] = f"{type(e).__name__}: {e}"
            if attempt < max_retries - 1:
                time.sleep(0.5)
                continue
            else:
                telemetry["status"] = "ERROR_EXHAUSTED"
                return [], telemetry

    telemetry["status"] = "UNKNOWN_EXHAUSTED"
    return [], telemetry


def extract_national_agmarknet_parallel(
    target_date_iso: str,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    max_workers: int = DEFAULT_WORKERS,
    task_matrix: list[dict[str, Any]] | None = None,
    matrix_path: str | Path | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Executes parallel multi-threaded extraction across all active national tasks
    with round-robin state scheduling, exponential backoff, and full gap telemetry.
    Returns: (unique_records, extraction_telemetry)
    """
    t0 = time.time()
    target_dt = date.fromisoformat(target_date_iso)
    lookback_dates = {(target_dt - timedelta(days=i)).isoformat() for i in range(lookback_days + 1)}

    target_year = target_dt.year
    target_month = target_dt.month

    tasks = task_matrix or load_active_task_matrix(matrix_path)
    print(f"\n⚡ Starting National Mandi Extraction across {len(tasks):,} tasks ({max_workers} worker threads)...")
    print(f"   Lookback Window: {min(lookback_dates)} to {max(lookback_dates)} ({lookback_days + 1} calendar days)")

    all_records: list[dict[str, Any]] = []
    task_telemetries: list[dict[str, Any]] = []
    completed_count = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for idx, t in enumerate(tasks):
            # Gentle staggered dispatch to prevent simultaneous TCP handshakes
            if idx > 0 and idx % max_workers == 0:
                time.sleep(THREAD_DISPATCH_STAGGER_S)
            future = executor.submit(
                fetch_monthly_block_task,
                t, target_year, target_month, lookback_dates, AGMARKNET_HEADERS
            )
            futures[future] = t

        for future in as_completed(futures):
            completed_count += 1
            recs, telem = future.result()
            task_telemetries.append(telem)

            if recs:
                all_records.extend(recs)

            if completed_count % 300 == 0 or completed_count == len(tasks):
                print(f"   ... Progress: {completed_count:,}/{len(tasks):,} tasks ({len(all_records):,} records harvested)")

    elapsed = time.time() - t0
    # Deduplicate by observation_hash
    unique_map = {r["observation_hash"]: r for r in all_records}
    unique_records = list(unique_map.values())

    # Build telemetry aggregation
    probed_states = sorted(set(t["state_name"] for t in tasks if t.get("state_name")))
    probed_crops = sorted(set(t["commodity_name"] for t in tasks if t.get("commodity_name")))

    active_states = sorted(set(r["state"] for r in unique_records))
    active_crops = sorted(set(r["commodity"] for r in unique_records))

    missing_states = [s for s in probed_states if s not in active_states]
    missing_crops = [c for c in probed_crops if c not in active_crops]

    telemetry_summary = {
        "total_tasks_probed": len(tasks),
        "tasks_with_data": sum(1 for t in task_telemetries if t["status"] == "OK_WITH_DATA"),
        "tasks_market_closed": sum(
            1 for t in task_telemetries if t["status"] in ("OK_MARKET_CLOSED_OR_NO_ARRIVAL", "OK_NO_DATA")
        ),
        "tasks_rate_limited_recovered": sum(
            1 for t in task_telemetries if t["retries_attempted"] > 0 and t["status"] == "OK_WITH_DATA"
        ),
        "tasks_rate_limited_failed": sum(
            1 for t in task_telemetries if t["status"] == "RATE_LIMITED_EXHAUSTED"
        ),
        "tasks_network_failed": sum(
            1 for t in task_telemetries if t["status"] in ("TIMEOUT_EXHAUSTED", "ERROR_EXHAUSTED")
        ),
        "total_monitored_states": len(probed_states),
        "active_states_count": len(active_states),
        "active_states": active_states,
        "missing_states": missing_states,
        "total_monitored_crops": len(probed_crops),
        "active_crops_count": len(active_crops),
        "active_crops": active_crops,
        "missing_crops": missing_crops,
        "elapsed_seconds": round(elapsed, 2),
        "task_details": task_telemetries,
    }

    print(
        f"✅ Extraction Finished in {elapsed:.1f}s: Harvested {len(unique_records):,} validated records "
        f"({telemetry_summary['active_states_count']}/{len(probed_states)} states | {telemetry_summary['active_crops_count']}/{len(probed_crops)} commodities)"
    )
    if missing_states:
        print(f"   ℹ️ Non-Reporting / Market-Closed States ({len(missing_states)}): {', '.join(missing_states[:8])}{'...' if len(missing_states) > 8 else ''}")

    return unique_records, telemetry_summary

"""Statistical Outlier Scrubbing & Clean Arbitrage Analytics."""

import collections
from typing import Any


def compute_clean_market_analytics(records: list[dict[str, Any]], target_date_iso: str) -> dict[str, Any]:
    """
    Computes rigorous market analytics with IQR/Median outlier scrubbing to prevent
    clerical typos (₹700 Chana or ₹13,200 Maize) from distorting spread highlights.

    Clamps price outliers outside [0.35 * median, 2.5 * median] for robust spread analysis.
    """
    if not records:
        return {
            "total_rows": 0,
            "active_mandis": 0,
            "active_commodities": 0,
            "active_states": 0,
            "total_volume_tonnes": 0.0,
            "top_volume_crop": "N/A",
            "top_volume_val": 0.0,
            "top_trading_hub": "N/A",
            "top_hub_lots": 0,
            "spreads": [],
            "date_counts": {},
            "state_counts": {},
        }

    # Group by commodity for outlier filtering
    comm_groups: dict[str, list[dict[str, Any]]] = collections.defaultdict(list)
    state_rows: dict[str, dict[str, Any]] = collections.defaultdict(
        lambda: {"rows": 0, "mandis": set(), "volume": 0.0}
    )
    date_counts: dict[str, int] = collections.defaultdict(int)
    market_counts: dict[str, int] = collections.defaultdict(int)

    for r in records:
        comm = r.get("commodity", "")
        comm_groups[comm].append(r)
        st = r.get("state", "Unknown")
        state_rows[st]["rows"] += 1
        state_rows[st]["mandis"].add(r.get("market", "Unknown"))
        state_rows[st]["volume"] += float(r.get("raw_arrival_quantity") or 0.0)
        date_counts[r.get("trade_date", target_date_iso)] += 1
        market_counts[r.get("market", "Unknown")] += 1

    clean_spreads = []

    for comm, rows in comm_groups.items():
        if len(rows) < 3:
            continue

        prices = [r["normalized_modal_price_qtl"] for r in rows if r.get("normalized_modal_price_qtl", 0) > 0]
        if not prices:
            continue

        prices.sort()
        med_p = prices[len(prices) // 2]

        # Clamp extreme typos (allow 0.35x to 2.5x of median)
        valid_prices = [p for p in prices if med_p * 0.35 <= p <= med_p * 2.5]
        if len(valid_prices) < 2:
            continue

        min_p = min(valid_prices)
        max_p = max(valid_prices)
        spread_pct = ((max_p - min_p) / min_p) * 100.0

        total_vol = sum(float(r.get("raw_arrival_quantity") or 0.0) for r in rows)

        clean_spreads.append({
            "commodity": comm,
            "min_price": min_p,
            "max_price": max_p,
            "median_price": med_p,
            "spread_pct": round(spread_pct, 1),
            "observations": len(rows),
            "volume_tonnes": round(total_vol, 1),
        })

    # Sort spreads by percentage descending
    clean_spreads.sort(key=lambda x: x["spread_pct"], reverse=True)

    # Top volume commodity
    top_vol_comm = max(clean_spreads, key=lambda x: x["volume_tonnes"]) if clean_spreads else None
    top_vol_crop = top_vol_comm["commodity"] if top_vol_comm else "N/A"
    top_vol_val = top_vol_comm["volume_tonnes"] if top_vol_comm else 0.0

    # Top trading hub
    top_hub = max(market_counts.items(), key=lambda x: x[1]) if market_counts else ("N/A", 0)

    # Format state counts
    formatted_states = {}
    for st, d in sorted(state_rows.items(), key=lambda x: x[1]["rows"], reverse=True):
        formatted_states[st] = {
            "rows": d["rows"],
            "mandis": len(d["mandis"]),
            "volume": round(d["volume"], 1),
        }

    total_vol_all = sum(float(r.get("raw_arrival_quantity") or 0.0) for r in records)

    return {
        "total_rows": len(records),
        "active_mandis": len(set(r.get("market") for r in records if r.get("market"))),
        "active_commodities": len(comm_groups),
        "active_states": len(state_rows),
        "total_volume_tonnes": round(total_vol_all, 1),
        "top_volume_crop": top_vol_crop,
        "top_volume_val": top_vol_val,
        "top_trading_hub": top_hub[0],
        "top_hub_lots": top_hub[1],
        "spreads": clean_spreads[:5],
        "date_counts": dict(sorted(date_counts.items(), reverse=True)),
        "state_counts": formatted_states,
    }

"""Statistical Outlier Scrubbing, Clean Arbitrage, Price Velocity & Pipeline Gap Analytics."""

import collections
import datetime
from typing import Any

from app.analytics.msp_registry import evaluate_msp_status


def compute_clean_market_analytics(
    records: list[dict[str, Any]],
    target_date_iso: str,
    telemetry: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Computes rigorous market analytics with IQR/Median outlier scrubbing,
    DoD/WoW price velocity, inter-mandi arbitrage corridors, volume shock detection,
    and MSP benchmark compliance.
    """
    telem = telemetry or {}

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
            "arbitrage_corridors": [],
            "price_velocity": {},
            "volume_shocks": [],
            "msp_evaluations": {},
            "volatility_alert": None,
            "date_counts": {},
            "state_counts": {},
            "crop_counts": {},
            "telemetry": telem,
            "total_tasks_probed": telem.get("total_tasks_probed", 0),
            "tasks_with_data": telem.get("tasks_with_data", 0),
            "tasks_market_closed": telem.get("tasks_market_closed", 0),
            "tasks_failed": telem.get("tasks_rate_limited_failed", 0) + telem.get("tasks_network_failed", 0),
            "missing_states": telem.get("missing_states", []),
            "missing_crops": telem.get("missing_crops", []),
            "total_monitored_states": telem.get("total_monitored_states", 31),
            "total_monitored_crops": telem.get("total_monitored_crops", 348),
        }

    # Group by commodity and date
    comm_groups: dict[str, list[dict[str, Any]]] = collections.defaultdict(list)
    comm_date_groups: dict[tuple[str, str], list[dict[str, Any]]] = collections.defaultdict(list)
    state_rows: dict[str, dict[str, Any]] = collections.defaultdict(
        lambda: {"rows": 0, "mandis": set(), "volume": 0.0}
    )
    date_counts: dict[str, int] = collections.defaultdict(int)
    market_counts: dict[str, int] = collections.defaultdict(int)

    for r in records:
        comm = r.get("commodity", "")
        t_date = r.get("trade_date", target_date_iso)
        comm_groups[comm].append(r)
        comm_date_groups[(comm, t_date)].append(r)

        st = r.get("state", "Unknown")
        state_rows[st]["rows"] += 1
        state_rows[st]["mandis"].add(r.get("market", "Unknown"))
        state_rows[st]["volume"] += float(r.get("raw_arrival_quantity") or 0.0)
        date_counts[t_date] += 1
        market_counts[r.get("market", "Unknown")] += 1

    clean_spreads = []
    arbitrage_corridors = []

    # Identify latest date for day-specific arbitrage and velocity
    all_dates = sorted(date_counts.keys(), reverse=True)
    latest_date = all_dates[0] if all_dates else target_date_iso

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

        spread_item = {
            "commodity": comm,
            "min_price": min_p,
            "max_price": max_p,
            "median_price": med_p,
            "spread_pct": round(spread_pct, 1),
            "observations": len(rows),
            "volume_tonnes": round(total_vol, 1),
        }
        clean_spreads.append(spread_item)

        # Detect arbitrage corridors across distinct markets on the same trading date
        clean_rows = [
            r for r in rows
            if r.get("trade_date") == latest_date and r.get("normalized_modal_price_qtl", 0) in valid_prices
        ]
        if len(clean_rows) >= 2:
            sorted_by_price = sorted(clean_rows, key=lambda x: x["normalized_modal_price_qtl"])
            cheapest = sorted_by_price[0]
            costliest = sorted_by_price[-1]

            if cheapest.get("market") != costliest.get("market") and costliest["normalized_modal_price_qtl"] > cheapest["normalized_modal_price_qtl"]:
                spread_diff = costliest["normalized_modal_price_qtl"] - cheapest["normalized_modal_price_qtl"]
                if spread_diff >= 150.0:  # Minimum ₹150/Qtl to cover transport
                    arbitrage_corridors.append({
                        "commodity": comm,
                        "origin_mandi": f"{cheapest.get('market')} ({cheapest.get('state')})",
                        "origin_price": cheapest["normalized_modal_price_qtl"],
                        "dest_mandi": f"{costliest.get('market')} ({costliest.get('state')})",
                        "dest_price": costliest["normalized_modal_price_qtl"],
                        "gross_spread_rs": round(spread_diff, 1),
                        "spread_pct": round((spread_diff / cheapest["normalized_modal_price_qtl"]) * 100.0, 1),
                        "total_volume": round(total_vol, 1),
                    })

    # Sort spreads and corridors
    clean_spreads.sort(key=lambda x: x["spread_pct"], reverse=True)
    arbitrage_corridors.sort(key=lambda x: x["gross_spread_rs"], reverse=True)

    # Compute Day-over-Day & Week-over-Week Price Velocity
    price_velocity: dict[str, dict[str, Any]] = {}
    volume_shocks: list[dict[str, Any]] = []
    msp_evals: dict[str, Any] = {}

    for comm, rows in comm_groups.items():
        # Latest date price vs previous dates
        latest_rows = [r for r in rows if r.get("trade_date") == latest_date and r.get("normalized_modal_price_qtl", 0) > 0]
        prev_rows = [r for r in rows if r.get("trade_date") != latest_date and r.get("normalized_modal_price_qtl", 0) > 0]

        if latest_rows:
            latest_prices = [r["normalized_modal_price_qtl"] for r in latest_rows]
            latest_avg = sum(latest_prices) / len(latest_prices)
            latest_vol = sum(float(r.get("raw_arrival_quantity") or 0.0) for r in latest_rows)

            # Evaluate MSP compliance
            msp_eval = evaluate_msp_status(comm, latest_avg)
            if msp_eval:
                msp_evals[comm] = msp_eval

            if prev_rows:
                prev_prices = [r["normalized_modal_price_qtl"] for r in prev_rows]
                prev_avg = sum(prev_prices) / len(prev_prices)
                delta_rs = round(latest_avg - prev_avg, 1)
                delta_pct = round((delta_rs / prev_avg) * 100.0, 1) if prev_avg > 0 else 0.0

                trend = "STABLE"
                if delta_pct >= 2.5:
                    trend = "RALLY"
                elif delta_pct <= -2.5:
                    trend = "DECLINE"

                price_velocity[comm] = {
                    "latest_price": round(latest_avg, 0),
                    "prev_price": round(prev_avg, 0),
                    "delta_rs": delta_rs,
                    "delta_pct": delta_pct,
                    "trend": trend,
                    "indicator": f"🟢 +₹{delta_rs:,.0f} (+{delta_pct}%)" if delta_rs > 0 else (f"🔴 -₹{abs(delta_rs):,.0f} ({delta_pct}%)" if delta_rs < 0 else f"⚪ ₹0 (0.0%)"),
                }

                # Volume shock check
                daily_vols = [sum(float(r.get("raw_arrival_quantity") or 0.0) for r in comm_date_groups[(comm, d)]) for d in all_dates if (comm, d) in comm_date_groups]
                mean_vol = sum(daily_vols) / len(daily_vols) if daily_vols else 0.0

                if mean_vol >= 50.0:
                    if latest_vol >= (mean_vol * 1.6):
                        volume_shocks.append({
                            "commodity": comm,
                            "type": "GLUT_SURGE",
                            "latest_vol": round(latest_vol, 1),
                            "avg_vol": round(mean_vol, 1),
                            "shock_pct": round(((latest_vol - mean_vol) / mean_vol) * 100.0, 1),
                        })
                    elif latest_vol <= (mean_vol * 0.4):
                        volume_shocks.append({
                            "commodity": comm,
                            "type": "SUPPLY_DEFICIT",
                            "latest_vol": round(latest_vol, 1),
                            "avg_vol": round(mean_vol, 1),
                            "shock_pct": round(((latest_vol - mean_vol) / mean_vol) * 100.0, 1),
                        })

    # High Volatility Alert Banner (if any top crop price moved > 15%)
    volatility_alert = None
    for comm, vel in price_velocity.items():
        if abs(vel["delta_pct"]) >= 15.0:
            volatility_alert = f"⚠️ HIGH VOLATILITY ALERT: {comm} prices shifted by {vel['delta_pct']:+0.1f}% (₹{vel['delta_rs']:+0.0f}/Qtl) across reporting mandis."
            break

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

    # Format crop counts sorted by volume descending
    formatted_crops = {}
    for comm, rows in sorted(comm_groups.items(), key=lambda x: sum(float(r.get("raw_arrival_quantity") or 0.0) for r in x[1]), reverse=True):
        vol = sum(float(r.get("raw_arrival_quantity") or 0.0) for r in rows)
        prices = [r["normalized_modal_price_qtl"] for r in rows if r.get("normalized_modal_price_qtl", 0) > 0]
        avg_p = sum(prices) / len(prices) if prices else 0.0
        mandis_set = set(r.get("market") for r in rows if r.get("market"))
        vel_info = price_velocity.get(comm, {})

        formatted_crops[comm] = {
            "rows": len(rows),
            "mandis": len(mandis_set),
            "volume": round(vol, 1),
            "avg_price": round(avg_p, 0),
            "min_price": min(prices) if prices else 0.0,
            "max_price": max(prices) if prices else 0.0,
            "price_indicator": vel_info.get("indicator", "⚪ N/A"),
            "price_trend": vel_info.get("trend", "STABLE"),
        }

    total_vol_all = sum(float(r.get("raw_arrival_quantity") or 0.0) for r in records)
    total_probed = telem.get("total_tasks_probed", len(records))
    tasks_with_data = telem.get("tasks_with_data", len(records))
    tasks_closed = telem.get("tasks_market_closed", 0)
    tasks_failed = telem.get("tasks_rate_limited_failed", 0) + telem.get("tasks_network_failed", 0)
    missing_st = telem.get("missing_states", [])
    missing_cr = telem.get("missing_crops", [])
    total_states_mon = telem.get("total_monitored_states", len(state_rows))
    total_crops_mon = telem.get("total_monitored_crops", len(comm_groups))

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
        "arbitrage_corridors": arbitrage_corridors[:5],
        "price_velocity": price_velocity,
        "volume_shocks": volume_shocks[:5],
        "msp_evaluations": msp_evals,
        "volatility_alert": volatility_alert,
        "date_counts": dict(sorted(date_counts.items(), reverse=True)),
        "state_counts": formatted_states,
        "crop_counts": formatted_crops,
        "telemetry": telem,
        "total_tasks_probed": total_probed,
        "tasks_with_data": tasks_with_data,
        "tasks_market_closed": tasks_closed,
        "tasks_failed": tasks_failed,
        "missing_states": missing_st,
        "missing_crops": missing_cr,
        "total_monitored_states": total_states_mon,
        "total_monitored_crops": total_crops_mon,
    }

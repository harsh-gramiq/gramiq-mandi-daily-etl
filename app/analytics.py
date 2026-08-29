"""Pure analytics for validated mandi observations."""

from typing import Any, Dict, List
import pandas as pd


def compute_live_market_analytics(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not records:
        return {"record_count": 0, "crops_count": 0, "mandis_count": 0, "states_count": 0, "total_arrivals_tonnes": 0.0,
                "top_crop": "N/A", "top_crop_volume": 0.0, "top_mandi": "N/A", "top_mandi_trades": 0,
                "top_divergence_crop": "N/A", "top_divergence_spread_pct": 0.0, "top_divergence_corridor": "N/A",
                "top_spreads": [], "state_breakdown": {}, "date_breakdown": {}}
    df = pd.DataFrame(records)
    crop_vol = df.groupby("commodity")["raw_arrival_quantity"].sum()
    mandi_trades = df["market"].value_counts()
    spread_list = []
    for comm, group in df.groupby("commodity"):
        if len(group) < 2:
            continue
        min_p = float(group["normalized_modal_price_qtl"].min())
        max_p = float(group["normalized_modal_price_qtl"].max())
        mean_p = float(group["normalized_modal_price_qtl"].mean())
        if mean_p > 0 and max_p > min_p:
            spread_list.append({"commodity": comm, "min_price": min_p, "max_price": max_p,
                                "spread_pct": round((max_p - min_p) / mean_p * 100.0, 1),
                                "mandis_count": group["market"].nunique(),
                                "corridor": f"₹{min_p:,.0f} - ₹{max_p:,.0f} / Qtl"})
    spread_list.sort(key=lambda item: item["spread_pct"], reverse=True)
    top = spread_list[0] if spread_list else {"commodity": "N/A", "spread_pct": 0.0, "corridor": "Stable (<2% variance)"}
    state_breakdown = {st: {"records": len(g), "mandis": g["market"].nunique(), "commodities": g["commodity"].nunique(),
                            "arrivals_tonnes": round(float(g["raw_arrival_quantity"].sum()), 1)} for st, g in df.groupby("state")}
    date_breakdown = {str(d): {"records": len(g), "mandis": g["market"].nunique(), "commodities": g["commodity"].nunique(),
                               "arrivals_tonnes": round(float(g["raw_arrival_quantity"].sum()), 1)} for d, g in df.groupby("trade_date")}
    return {"record_count": len(df), "crops_count": df["commodity"].nunique(), "mandis_count": df["market"].nunique(),
            "states_count": df["state"].nunique(), "total_arrivals_tonnes": round(float(df["raw_arrival_quantity"].sum()), 1),
            "top_crop": str(crop_vol.idxmax()), "top_crop_volume": round(float(crop_vol.max()), 1),
            "top_mandi": str(mandi_trades.index[0]), "top_mandi_trades": int(mandi_trades.iloc[0]),
            "top_divergence_crop": top["commodity"], "top_divergence_spread_pct": top["spread_pct"],
            "top_divergence_corridor": top["corridor"], "top_spreads": spread_list[:5],
            "state_breakdown": state_breakdown, "date_breakdown": date_breakdown}

"""Authentic Gemini AI Executive Market Brief Synthesizer."""

import json
import os
import sys
import urllib.request
from typing import Any

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


def generate_gemini_market_brief(metrics: dict[str, Any], target_date_iso: str) -> str:
    """
    Synthesizes an executive market intelligence brief via Google Gemini API.
    Enforces strict agronomic guardrails: price spreads >150% must be noted as
    quality grade variance or lot differences rather than simple arbitrage.
    """
    api_key = os.environ.get("GEMINI_API_KEY_OG") or os.environ.get("GEMINI_API_KEY")
    if not api_key:
        print("  [Gemini AI] ⚠️ GEMINI_API_KEY not set. Using deterministic quantitative brief.")
        top_spread = metrics["spreads"][0] if metrics.get("spreads") else None
        spread_str = (
            f"Highest spread observed in {top_spread['commodity']} "
            f"({top_spread['spread_pct']}% spread, ₹{top_spread['min_price']:,.0f} - ₹{top_spread['max_price']:,.0f}/Qtl)."
            if top_spread
            else ""
        )
        return (
            f"Daily national mandi ingestion for {target_date_iso} synchronized {metrics.get('total_rows', 0):,} validated observations "
            f"across {metrics.get('active_commodities', 0)} commodities and {metrics.get('active_mandis', 0)} reporting APMCs in {metrics.get('active_states', 0)} states. "
            f"Top volume arrival was {metrics.get('top_volume_crop', 'N/A')} ({metrics.get('top_volume_val', 0):,.0f} Tonnes). {spread_str}"
        ).strip()

    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={api_key}"
    headers = {"Content-Type": "application/json"}

    spread_summary = "\n".join(
        [
            f"- {s['commodity']}: {s['spread_pct']}% spread (₹{s['min_price']:,.0f} - ₹{s['max_price']:,.0f}/Qtl, Median: ₹{s['median_price']:,.0f}, Vol: {s['volume_tonnes']:,.0f} T)"
            for s in metrics.get("spreads", [])[:4]
        ]
    )

    prompt = f"""You are the Chief Quantitative Agronomist for GramIQ MandiBhav.
Analyze today's national agricultural market settlement snapshot for {target_date_iso}:
- Total Ingested Observations: {metrics.get('total_rows', 0):,} rows
- Active APMCs: {metrics.get('active_mandis', 0)} across {metrics.get('active_states', 0)} States & UTs
- Active Commodities: {metrics.get('active_commodities', 0)}
- Total Traded Volume: {metrics.get('total_volume_tonnes', 0):,.0f} Tonnes
- Top Volume Commodity: {metrics.get('top_volume_crop', 'N/A')} ({metrics.get('top_volume_val', 0):,.0f} Tonnes)
- Key Trading Hub: {metrics.get('top_trading_hub', 'N/A')} ({metrics.get('top_hub_lots', 0)} lots)
- Key Inter-Mandi Clean Spreads:
{spread_summary}

Write a concise 2-sentence executive trading brief for agricultural procurement desks and farmers.
Cite exact commodities, volumes, and price corridors.
CRITICAL GUARDRAIL: If any spread exceeds 150%, do NOT label it as simple spatial arbitrage; identify it as lot/variety quality grade variance or regional supply tightness."""

    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.2, "maxOutputTokens": 300},
    }

    try:
        req = urllib.request.Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            if resp.status == 200:
                result = json.loads(resp.read().decode("utf-8"))
                text = result["candidates"][0]["content"]["parts"][0]["text"].strip()
                print("  [Gemini AI] ✅ Successfully generated executive market brief.")
                return text
    except Exception as e:
        print(f"  [Gemini AI] ⚠️ Inference call failed ({e}), falling back to deterministic brief.")

    # Fallback
    top_spread = metrics["spreads"][0] if metrics.get("spreads") else None
    spread_str = (
        f"Highest clean spread observed in {top_spread['commodity']} "
        f"({top_spread['spread_pct']}% spread, ₹{top_spread['min_price']:,.0f} - ₹{top_spread['max_price']:,.0f}/Qtl)."
        if top_spread
        else ""
    )
    return (
        f"Daily national mandi ingestion for {target_date_iso} synchronized {metrics.get('total_rows', 0):,} validated observations "
        f"across {metrics.get('active_commodities', 0)} commodities and {metrics.get('active_mandis', 0)} reporting APMCs in {metrics.get('active_states', 0)} states. "
        f"Top volume arrival was {metrics.get('top_volume_crop', 'N/A')} ({metrics.get('top_volume_val', 0):,.0f} Tonnes). {spread_str}"
    ).strip()

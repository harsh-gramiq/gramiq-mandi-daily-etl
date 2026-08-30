"""Microsoft Teams Adaptive Card v1.5 Builder & Webhook Dispatcher."""

from datetime import datetime
import json
import os
from typing import Any
import urllib.request


def build_adaptive_card(
    metrics: dict[str, Any], ai_brief: str, target_date_iso: str, elapsed_s: float
) -> dict[str, Any]:
    """
    Constructs the Microsoft Teams Adaptive Card v1.5 payload with clear date disambiguation,
    clean outlier-scrubbed spreads, interactive toggleable state breakdown, and
    pipeline reliability / data gap telemetry.
    """
    # Date breakdown text
    date_counts = metrics.get("date_counts", {})
    today_rows = date_counts.get(target_date_iso, 0)
    lookback_rows = metrics.get("total_rows", 0) - today_rows

    # Arbitrage facts
    spread_facts = []
    for s in metrics.get("spreads", [])[:4]:
        spread_facts.append({
            "title": f"🌾 {s['commodity']}",
            "value": f"{s['spread_pct']}% Spread (₹{s['min_price']:,.0f} - ₹{s['max_price']:,.0f} / Qtl)",
        })

    # State breakdown facts for toggle container (top 20 reporting states)
    state_facts = []
    for st, d in list(metrics.get("state_counts", {}).items())[:20]:
        state_facts.append({
            "title": f"📍 {st}",
            "value": f"{d['rows']:,} rows | {d['mandis']} APMCs | {d['volume']:,} T",
        })

    # Crop breakdown facts for toggle container (top 20 commodities)
    crop_facts = []
    crop_counts = metrics.get("crop_counts", {})
    for crop, d in list(crop_counts.items())[:20]:
        crop_facts.append({
            "title": f"🌾 {crop}",
            "value": f"{d['volume']:,} T | {d['mandis']} APMCs | Avg: ₹{d['avg_price']:,.0f}/Qtl (₹{d['min_price']:,.0f} - ₹{d['max_price']:,.0f})",
        })

    # Crop choices for compact Input.ChoiceSet dropdown
    crop_choices = []
    for crop, d in list(crop_counts.items())[:40]:
        crop_choices.append({
            "title": f"{crop} ({d['volume']:,} T | {d['mandis']} APMCs | ₹{d['avg_price']:,.0f}/Qtl)",
            "value": crop,
        })
    if not crop_choices:
        crop_choices.append({"title": "No Active Commodities", "value": "N/A"})

    # Gap telemetry facts
    missing_states = metrics.get("missing_states", [])
    missing_crops = metrics.get("missing_crops", [])
    failed_tasks_count = metrics.get("tasks_failed", 0)
    recovered_tasks_count = metrics.get("telemetry", {}).get("tasks_rate_limited_recovered", 0)

    gap_facts = [
        {
            "title": "📍 Non-Reporting States",
            "value": (
                f"{len(missing_states)} states ({', '.join(missing_states[:6])}{'...' if len(missing_states) > 6 else ''})"
                if missing_states
                else "None (100% National Coverage)"
            ),
        },
        {
            "title": "🌾 Off-Season / No Arrivals",
            "value": (
                f"{len(missing_crops)} crops with 0 arrivals in trailing 7 days"
                if missing_crops
                else "None"
            ),
        },
        {
            "title": "🔄 Rate-Limit Retries Recovered",
            "value": f"{recovered_tasks_count} tasks automatically recovered with backoff",
        },
        {
            "title": "❌ Unrecoverable Failures",
            "value": f"{failed_tasks_count} tasks failed after max retries",
        },
    ]

    card = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard",
                    "version": "1.5",
                    "body": [
                        {
                            "type": "ColumnSet",
                            "columns": [
                                {
                                    "type": "Column",
                                    "width": "auto",
                                    "items": [
                                        {
                                            "type": "Image",
                                            "url": "https://raw.githubusercontent.com/microsoft/fluentui-system-icons/main/assets/Plant/SVG/ic_fluent_plant_24_filled.svg",
                                            "size": "Medium",
                                        }
                                    ],
                                },
                                {
                                    "type": "Column",
                                    "width": "stretch",
                                    "items": [
                                        {
                                            "type": "TextBlock",
                                            "text": "🟢 Final National Reconciliation Report",
                                            "weight": "Bolder",
                                            "size": "Large",
                                        },
                                        {
                                            "type": "TextBlock",
                                            "text": f"Trade Date: {target_date_iso} (Rolling 7-Day Window) • Dispatched at {datetime.now().strftime('%d %B %Y | %H:%M IST')}",
                                            "spacing": "None",
                                            "isSubtle": True,
                                            "size": "Small",
                                        },
                                    ],
                                },
                                {
                                    "type": "Column",
                                    "width": "auto",
                                    "items": [
                                        {
                                            "type": "TextBlock",
                                            "text": f"⏱️ {elapsed_s:.1f}s",
                                            "weight": "Bolder",
                                            "color": "Good",
                                        }
                                    ],
                                },
                            ],
                        },
                        {
                            "type": "Container",
                            "style": "good",
                            "bleed": True,
                            "items": [
                                {
                                    "type": "TextBlock",
                                    "text": "✅ Authoritative daily snapshot after rolling lookback reconciliation.",
                                    "weight": "Bolder",
                                    "wrap": True,
                                }
                            ],
                        },
                        {
                            "type": "TextBlock",
                            "text": "📊 DAILY HARVEST SNAPSHOT",
                            "weight": "Bolder",
                            "size": "Medium",
                            "spacing": "Medium",
                        },
                        {
                            "type": "ColumnSet",
                            "columns": [
                                {
                                    "type": "Column",
                                    "width": "1",
                                    "items": [
                                        {"type": "TextBlock", "text": "VALIDATED ROWS", "size": "Small", "isSubtle": True},
                                        {
                                            "type": "TextBlock",
                                            "text": f"{metrics.get('total_rows', 0):,}",
                                            "size": "ExtraLarge",
                                            "weight": "Bolder",
                                            "color": "Accent",
                                        },
                                    ],
                                },
                                {
                                    "type": "Column",
                                    "width": "1",
                                    "items": [
                                        {"type": "TextBlock", "text": "ACTIVE APMCS", "size": "Small", "isSubtle": True},
                                        {
                                            "type": "TextBlock",
                                            "text": f"{metrics.get('active_mandis', 0):,}",
                                            "size": "ExtraLarge",
                                            "weight": "Bolder",
                                            "color": "Accent",
                                        },
                                    ],
                                },
                                {
                                    "type": "Column",
                                    "width": "1",
                                    "items": [
                                        {"type": "TextBlock", "text": "COMMODITIES", "size": "Small", "isSubtle": True},
                                        {
                                            "type": "TextBlock",
                                            "text": f"{metrics.get('active_commodities', 0)}",
                                            "size": "ExtraLarge",
                                            "weight": "Bolder",
                                            "color": "Accent",
                                        },
                                    ],
                                },
                                {
                                    "type": "Column",
                                    "width": "1",
                                    "items": [
                                        {"type": "TextBlock", "text": "REPORTING STATES", "size": "Small", "isSubtle": True},
                                        {
                                            "type": "TextBlock",
                                            "text": f"{metrics.get('active_states', 0)} States",
                                            "size": "ExtraLarge",
                                            "weight": "Bolder",
                                            "color": "Accent",
                                        },
                                    ],
                                },
                            ],
                        },
                        {
                            "type": "TextBlock",
                            "text": "🤖 AI MARKET INTELLIGENCE BRIEF",
                            "weight": "Bolder",
                            "size": "Medium",
                            "spacing": "Medium",
                        },
                        {
                            "type": "Container",
                            "style": "emphasis",
                            "items": [
                                {"type": "TextBlock", "text": ai_brief, "wrap": True}
                            ],
                        },
                        {
                            "type": "TextBlock",
                            "text": "🌾 COMMODITY MARKET SELECTOR",
                            "weight": "Bolder",
                            "size": "Medium",
                            "spacing": "Medium",
                        },
                        {
                            "type": "Input.ChoiceSet",
                            "id": "cropSelector",
                            "style": "compact",
                            "placeholder": "🔍 Browse Ingested Crop Rates...",
                            "value": crop_choices[0]["value"] if crop_choices else "Potato",
                            "choices": crop_choices,
                        },
                        {
                            "type": "TextBlock",
                            "text": "📈 QUANTITATIVE ARBITRAGE & VOLUME HIGHLIGHTS",
                            "weight": "Bolder",
                            "size": "Medium",
                            "spacing": "Medium",
                        },
                        {
                            "type": "FactSet",
                            "facts": [
                                {
                                    "title": "📦 Top Volume Crop",
                                    "value": f"{metrics.get('top_volume_crop', 'N/A')} ({metrics.get('top_volume_val', 0):,} Tonnes)",
                                },
                                {
                                    "title": "🏛️ Top Trading Hub",
                                    "value": f"{metrics.get('top_trading_hub', 'N/A')} ({metrics.get('top_hub_lots', 0)} active lots)",
                                },
                                {
                                    "title": "⚡ Total Ingested Volume",
                                    "value": f"{metrics.get('total_volume_tonnes', 0):,} Tonnes",
                                },
                                {
                                    "title": "🗓️ Ingestion Scope",
                                    "value": f"Today ({target_date_iso}): {today_rows:,} rows • Trailing Reconciled: {lookback_rows:,} rows",
                                },
                            ]
                            + spread_facts,
                        },
                        {
                            "type": "TextBlock",
                            "text": "🔍 PIPELINE RELIABILITY & DATA GAP TELEMETRY",
                            "weight": "Bolder",
                            "size": "Medium",
                            "spacing": "Medium",
                        },
                        {
                            "type": "ColumnSet",
                            "columns": [
                                {
                                    "type": "Column",
                                    "width": "1",
                                    "items": [
                                        {"type": "TextBlock", "text": "TASKS PROBED", "size": "Small", "isSubtle": True},
                                        {
                                            "type": "TextBlock",
                                            "text": f"{metrics.get('total_tasks_probed', 0):,}",
                                            "size": "Medium",
                                            "weight": "Bolder",
                                        },
                                    ],
                                },
                                {
                                    "type": "Column",
                                    "width": "1",
                                    "items": [
                                        {"type": "TextBlock", "text": "ACTIVE DATA", "size": "Small", "isSubtle": True},
                                        {
                                            "type": "TextBlock",
                                            "text": f"{metrics.get('tasks_with_data', 0):,}",
                                            "size": "Medium",
                                            "weight": "Bolder",
                                            "color": "Good",
                                        },
                                    ],
                                },
                                {
                                    "type": "Column",
                                    "width": "1",
                                    "items": [
                                        {"type": "TextBlock", "text": "MARKETS CLOSED", "size": "Small", "isSubtle": True},
                                        {
                                            "type": "TextBlock",
                                            "text": f"{metrics.get('tasks_market_closed', 0):,}",
                                            "size": "Medium",
                                            "weight": "Bolder",
                                            "color": "Warning",
                                        },
                                    ],
                                },
                                {
                                    "type": "Column",
                                    "width": "1",
                                    "items": [
                                        {"type": "TextBlock", "text": "NETWORK FAILS", "size": "Small", "isSubtle": True},
                                        {
                                            "type": "TextBlock",
                                            "text": f"{failed_tasks_count}",
                                            "size": "Medium",
                                            "weight": "Bolder",
                                            "color": "Attention" if failed_tasks_count > 0 else "Good",
                                        },
                                    ],
                                },
                            ],
                        },
                        {
                            "type": "FactSet",
                            "facts": gap_facts,
                        },
                        {
                            "type": "Container",
                            "id": "cropBreakdownContainer",
                            "isVisible": False,
                            "items": [
                                {
                                    "type": "TextBlock",
                                    "text": "🌾 TOP COMMODITY ARRIVALS & PRICE LEDGER",
                                    "weight": "Bolder",
                                    "size": "Medium",
                                    "spacing": "Medium",
                                },
                                {"type": "FactSet", "facts": crop_facts},
                            ],
                        },
                        {
                            "type": "Container",
                            "id": "stateBreakdownContainer",
                            "isVisible": False,
                            "items": [
                                {
                                    "type": "TextBlock",
                                    "text": "🗺️ STATE-BY-STATE ARRIVAL BREAKDOWN",
                                    "weight": "Bolder",
                                    "size": "Medium",
                                    "spacing": "Medium",
                                },
                                {"type": "FactSet", "facts": state_facts},
                            ],
                        },
                    ],
                    "actions": [
                        {
                            "type": "Action.ToggleVisibility",
                            "title": "🌾 Toggle Crop Breakdown",
                            "targetElements": ["cropBreakdownContainer"],
                        },
                        {
                            "type": "Action.ToggleVisibility",
                            "title": "🗺️ Toggle State Breakdown",
                            "targetElements": ["stateBreakdownContainer"],
                        },
                    ],
                },
            }
        ],
    }
    return card


def dispatch_card_to_teams(card: dict[str, Any]) -> bool:
    """Dispatches the Adaptive Card to Microsoft Teams Webhook."""
    webhook_url = os.environ.get("TEAMS_WEBHOOK_URL")
    if not webhook_url:
        print("  [Teams Dispatch] ⚠️ TEAMS_WEBHOOK_URL not configured. Card dispatch skipped.")
        return False

    headers = {"Content-Type": "application/json"}
    payload_bytes = json.dumps(card).encode("utf-8")

    try:
        req = urllib.request.Request(webhook_url, data=payload_bytes, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=15) as resp:
            if resp.status in (200, 202):
                print("  [Teams Dispatch] ✅ Successfully posted Adaptive Card to Teams Channel.")
                return True
            else:
                print(f"  [Teams Dispatch] ⚠️ Webhook returned HTTP {resp.status}")
    except Exception as e:
        print(f"  [Teams Dispatch] ❌ Failed to send card: {e}")
    return False

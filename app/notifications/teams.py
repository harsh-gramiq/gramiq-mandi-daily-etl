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
    clean outlier-scrubbed spreads, and interactive toggleable state breakdown.
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

    # State breakdown facts for toggle container
    state_facts = []
    for st, d in list(metrics.get("state_counts", {}).items())[:18]:
        state_facts.append({
            "title": f"📍 {st}",
            "value": f"{d['rows']:,} rows | {d['mandis']} APMCs | {d['volume']:,} T",
        })

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
                            "title": "🗺️ Toggle State Breakdown",
                            "targetElements": ["stateBreakdownContainer"],
                        }
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

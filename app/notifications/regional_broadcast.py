"""Farmer-First Regional WhatsApp / Telegram Digest Generator (hi-IN).

Formats authentic Hindi market summaries using traditional agricultural terms
(क्विंटल, आवक, भाव, तेजी/मंदी, न्यूनतम समर्थन मूल्य - MSP) for FPOs and farmer channels.
"""

from typing import Any


def format_hindi_farmer_digest(
    metrics: dict[str, Any],
    target_date: str,
) -> str:
    """
    Generates a clear, authentic Hindi broadcast message for farmer WhatsApp/Telegram groups.
    Adheres strictly to the Farmer-First Multilingual Standard.
    """
    crop_counts = metrics.get("crop_counts", {})
    vol_tonnes = metrics.get("total_volume_tonnes", 0)
    mandis_cnt = metrics.get("active_mandis", 0)
    states_cnt = metrics.get("active_states", 0)

    lines = [
        f"🌾 *ग्रामआईक्यू (GramIQ) राष्ट्रीय मंडी भाव बुलेटिन*",
        f"📅 दिनांक: *{target_date}*",
        f"📊 कुल आवक: *{vol_tonnes:,.0f} टन* | सक्रिय मंडियां: *{mandis_cnt}* ({states_cnt} राज्य)",
        "─────────────────────────",
        "📌 *प्रमुख फसलों के औसत थोक भाव (प्रति क्विंटल):*",
        "",
    ]

    # Map popular crops to authentic Hindi names
    hindi_crop_names = {
        "Wheat": "गेहूं (Wheat)",
        "Paddy(Dhan)(Common)": "धान (Paddy)",
        "Gram Raw(Chana)": "चना (Chana)",
        "Chana(Gram)": "चना (Chana)",
        "Mustard": "सरसों / राई (Mustard)",
        "Soyabean": "सोयाबीन (Soyabean)",
        "Maize": "मक्का (Maize)",
        "Cotton": "कपास (Cotton)",
        "Potato": "आलू (Potato)",
        "Onion": "प्याज (Onion)",
        "Tomato": "टमाटर (Tomato)",
        "Groundnut": "मूंगफली (Groundnut)",
    }

    count = 0
    for crop, data in crop_counts.items():
        if count >= 8:
            break
        h_name = hindi_crop_names.get(crop, crop)
        avg_p = data.get("avg_price", 0)
        min_p = data.get("min_price", 0)
        max_p = data.get("max_price", 0)
        vol = data.get("volume", 0)
        trend = data.get("price_trend", "STABLE")

        trend_icon = "🟢 तेजी" if trend == "RALLY" else ("🔴 मंदी" if trend == "DECLINE" else "⚪ स्थिर")

        lines.append(
            f"• *{h_name}*: ₹{avg_p:,.0f} / क्विंटल ({trend_icon})\n"
            f"   रेंज: ₹{min_p:,.0f} - ₹{max_p:,.0f} | आवक: {vol:,.0f} टन"
        )
        count += 1

    # Arbitrage trade corridor highlights if available
    corridors = metrics.get("arbitrage_corridors", [])
    if corridors:
        lines.append("\n📈 *बड़ी मंडियों में भाव का अंतर (Arbitrage):*")
        for c in corridors[:2]:
            lines.append(
                f"• *{c['commodity']}*: {c['origin_mandi']} में ₹{c['origin_price']:,.0f} ➔ {c['dest_mandi']} में ₹{c['dest_price']:,.0f} (अंतर: +₹{c['gross_spread_rs']:,.0f}/क्विंटल)"
            )

    lines.append("\n─────────────────────────")
    lines.append(
        "ℹ️ *सूचना:* यह भाव विश्लेषण Agmarknet डेटा पर आधारित है। अपनी फसल का विक्रय करने से पहले संबंधित मंडी समिति से भाव की पुष्टि अवश्य करें।"
    )

    return "\n".join(lines)

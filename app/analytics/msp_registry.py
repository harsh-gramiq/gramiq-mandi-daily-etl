"""Official Government Minimum Support Price (MSP) Registry & Benchmark Comparator.

Tracks active CACP / Ministry of Agriculture MSP benchmarks for major Kharif and Rabi crops.
Calculates price variance against MSP to flag distressed sales and price floors.
"""

from typing import Any

# Canonical MSP Benchmarks (in ₹ per Quintal / 100 kg)
MSP_BENCHMARK_RATES_QTL: dict[str, float] = {
    "Wheat": 2425.0,
    "Paddy(Dhan)(Common)": 2300.0,
    "Paddy(Dhan)(Grade A)": 2320.0,
    "Rice": 3400.0,
    "Gram Raw(Chana)": 5650.0,
    "Chana(Gram)": 5650.0,
    "Mustard": 5950.0,
    "Soyabean": 4892.0,
    "Maize": 2225.0,
    "Cotton": 7521.0,
    "Groundnut": 6783.0,
    "Moong(Green Gram)": 8682.0,
    "Urad(Black Gram)": 7400.0,
    "Arhar (Tur/Red Gram)": 7550.0,
    "Bajra(Pearl Millet/Cumbu)": 2625.0,
    "Jowar(Sorghum)": 3371.0,
    "Barley (Jau)": 1850.0,
    "Sunflower": 7280.0,
    "Sesamum(Sesame,Gingelly,Til)": 9267.0,
}


def get_msp_for_commodity(commodity_name: str) -> float | None:
    """Returns official MSP rate for a commodity or None if not an MSP crop."""
    if not commodity_name:
        return None
    # Direct match
    if commodity_name in MSP_BENCHMARK_RATES_QTL:
        return MSP_BENCHMARK_RATES_QTL[commodity_name]

    # Substring / partial match
    norm_name = commodity_name.lower().strip()
    for msp_crop, rate in MSP_BENCHMARK_RATES_QTL.items():
        if norm_name in msp_crop.lower() or msp_crop.lower() in norm_name:
            return rate
    return None


def evaluate_msp_status(
    commodity: str,
    modal_price: float,
) -> dict[str, Any] | None:
    """
    Evaluates whether a trade price is at, above, or below the government support floor.
    """
    msp = get_msp_for_commodity(commodity)
    if msp is None or modal_price <= 0:
        return None

    delta_rs = round(modal_price - msp, 1)
    variance_pct = round((delta_rs / msp) * 100.0, 1)

    is_distress = modal_price < (msp * 0.95)  # 5% or more below MSP
    is_premium = modal_price > (msp * 1.10)   # 10% or more above MSP

    status = "AT_MSP"
    if is_distress:
        status = "BELOW_MSP"
    elif is_premium:
        status = "ABOVE_MSP"

    return {
        "commodity": commodity,
        "msp_rate_qtl": msp,
        "modal_price_qtl": modal_price,
        "delta_rs": delta_rs,
        "variance_pct": variance_pct,
        "status": status,
        "is_distress": is_distress,
    }

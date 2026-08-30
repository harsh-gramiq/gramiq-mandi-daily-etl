"""GitHub Actions Step Summary Generator."""

import os
from typing import Any


def write_github_step_summary(
    metrics: dict[str, Any],
    ai_brief: str,
    target_date: str,
    lookback_days: int,
    elapsed_s: float,
) -> bool:
    """
    Writes formatted markdown execution and market intelligence summary to GITHUB_STEP_SUMMARY
    if running in a GitHub Actions CI workflow runner.
    """
    step_summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not step_summary_path:
        return False

    try:
        with open(step_summary_path, "a", encoding="utf-8") as f:
            f.write("## 🌾 National Mandi Ingestion & 7-Day Rolling Reconciliation\n\n")
            f.write("| Metric | Value |\n| :--- | :--- |\n")
            f.write(f"| **Target Trade Date** | `{target_date}` |\n")
            f.write(f"| **Lookback Window** | `{lookback_days} calendar days` |\n")
            f.write(f"| **Validated Observations** | **{metrics.get('total_rows', 0):,} rows** |\n")
            f.write(f"| **Active Reporting APMCs** | **{metrics.get('active_mandis', 0):,} mandis** |\n")
            f.write(f"| **Active Commodities** | **{metrics.get('active_commodities', 0)} crops** |\n")
            f.write(f"| **Reporting States** | **{metrics.get('active_states', 0)} States/UTs** |\n")
            f.write(f"| **Total Volume Harvested** | **{metrics.get('total_volume_tonnes', 0):,} Tonnes** |\n")
            f.write(
                f"| **Top Volume Crop** | **{metrics.get('top_volume_crop', 'N/A')} ({metrics.get('top_volume_val', 0):,} T)** |\n"
            )
            f.write(f"| **Pipeline Execution Time** | **{elapsed_s:.1f} seconds** |\n\n")
            f.write(f"### 🤖 AI Market Intelligence Brief\n> {ai_brief}\n\n")

            if metrics.get("spreads"):
                f.write("### 📈 Top Clean Inter-Mandi Spreads (Outlier Scrubbed)\n\n")
                f.write("| Commodity | Min Price | Median Price | Max Price | Spread % | Volume |\n")
                f.write("| :--- | :--- | :--- | :--- | :--- | :--- |\n")
                for s in metrics["spreads"]:
                    f.write(
                        f"| **{s['commodity']}** | ₹{s['min_price']:,.0f} | ₹{s['median_price']:,.0f} | ₹{s['max_price']:,.0f} | **{s['spread_pct']}%** | {s['volume_tonnes']:,} T |\n"
                    )
                f.write("\n")
        return True
    except Exception as e:
        print(f"  [CI Step Summary] ⚠️ Could not write step summary: {e}")
        return False

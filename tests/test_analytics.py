"""Unit tests for market analytics and statistical outlier scrubbing."""

import unittest
from app.analytics.market_stats import compute_clean_market_analytics


class TestMarketAnalytics(unittest.TestCase):
    def test_empty_records(self):
        metrics = compute_clean_market_analytics([], "2026-08-29")
        self.assertEqual(metrics["total_rows"], 0)
        self.assertEqual(metrics["active_mandis"], 0)
        self.assertEqual(metrics["active_commodities"], 0)
        self.assertEqual(metrics["active_states"], 0)
        self.assertEqual(metrics["top_volume_crop"], "N/A")
        self.assertEqual(len(metrics["spreads"]), 0)

    def test_outlier_scrubbing(self):
        # Create dataset for Mustard with normal prices around 5200-5800,
        # but with a clerical typo of ₹700 and ₹15,000
        records = [
            {"commodity": "Mustard", "state": "Rajasthan", "market": "Alwar", "trade_date": "2026-08-29", "normalized_modal_price_qtl": 5200.0, "raw_arrival_quantity": "50"},
            {"commodity": "Mustard", "state": "Rajasthan", "market": "Bharatpur", "trade_date": "2026-08-29", "normalized_modal_price_qtl": 5400.0, "raw_arrival_quantity": "60"},
            {"commodity": "Mustard", "state": "Rajasthan", "market": "Kota", "trade_date": "2026-08-29", "normalized_modal_price_qtl": 5500.0, "raw_arrival_quantity": "40"},
            {"commodity": "Mustard", "state": "Haryana", "market": "Rewari", "trade_date": "2026-08-29", "normalized_modal_price_qtl": 5800.0, "raw_arrival_quantity": "30"},
            # Clerical typos that should be scrubbed:
            {"commodity": "Mustard", "state": "Rajasthan", "market": "Jaipur", "trade_date": "2026-08-29", "normalized_modal_price_qtl": 700.0, "raw_arrival_quantity": "10"},
            {"commodity": "Mustard", "state": "Rajasthan", "market": "Tonk", "trade_date": "2026-08-29", "normalized_modal_price_qtl": 16000.0, "raw_arrival_quantity": "10"},
        ]

        metrics = compute_clean_market_analytics(records, "2026-08-29")
        self.assertEqual(metrics["total_rows"], 6)
        self.assertEqual(metrics["active_commodities"], 1)
        self.assertEqual(metrics["active_states"], 2)
        self.assertEqual(metrics["active_mandis"], 6)

        # Spread should be calculated ONLY on the clean set [5200, 5400, 5500, 5800]
        # (700 and 16000 are < 0.35 * 5450 or > 2.5 * 5450)
        self.assertEqual(len(metrics["spreads"]), 1)
        mustard_spread = metrics["spreads"][0]
        self.assertEqual(mustard_spread["commodity"], "Mustard")
        self.assertEqual(mustard_spread["min_price"], 5200.0)
        self.assertEqual(mustard_spread["max_price"], 5800.0)
        expected_spread_pct = round(((5800.0 - 5200.0) / 5200.0) * 100.0, 1)
        self.assertEqual(mustard_spread["spread_pct"], expected_spread_pct)

    def test_state_and_hub_aggregation(self):
        records = [
            {"commodity": "Wheat", "state": "Madhya Pradesh", "market": "Indore", "trade_date": "2026-08-29", "normalized_modal_price_qtl": 2500.0, "raw_arrival_quantity": "100"},
            {"commodity": "Wheat", "state": "Madhya Pradesh", "market": "Indore", "trade_date": "2026-08-28", "normalized_modal_price_qtl": 2550.0, "raw_arrival_quantity": "120"},
            {"commodity": "Wheat", "state": "Madhya Pradesh", "market": "Ujjain", "trade_date": "2026-08-29", "normalized_modal_price_qtl": 2480.0, "raw_arrival_quantity": "80"},
            {"commodity": "Wheat", "state": "Punjab", "market": "Khanna", "trade_date": "2026-08-29", "normalized_modal_price_qtl": 2600.0, "raw_arrival_quantity": "200"},
        ]
        metrics = compute_clean_market_analytics(records, "2026-08-29")
        self.assertEqual(metrics["top_trading_hub"], "Indore")
        self.assertEqual(metrics["top_hub_lots"], 2)
        self.assertEqual(metrics["total_volume_tonnes"], 500.0)
        self.assertEqual(metrics["state_counts"]["Madhya Pradesh"]["rows"], 3)
        self.assertEqual(metrics["state_counts"]["Madhya Pradesh"]["mandis"], 2)
        self.assertEqual(metrics["state_counts"]["Punjab"]["rows"], 1)
        self.assertIn("Wheat", metrics["crop_counts"])
        self.assertEqual(metrics["crop_counts"]["Wheat"]["rows"], 4)
        self.assertEqual(metrics["crop_counts"]["Wheat"]["mandis"], 3)
        self.assertEqual(metrics["crop_counts"]["Wheat"]["volume"], 500.0)
        self.assertEqual(metrics["crop_counts"]["Wheat"]["min_price"], 2480.0)
        self.assertEqual(metrics["crop_counts"]["Wheat"]["max_price"], 2600.0)


if __name__ == "__main__":
    unittest.main()

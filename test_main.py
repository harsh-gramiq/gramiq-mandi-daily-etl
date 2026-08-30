import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
from datetime import datetime

TEST_DIR = Path(__file__).resolve().parent
if str(TEST_DIR) not in sys.path:
    sys.path.insert(0, str(TEST_DIR))

import main


class MandiDailyETLTests(unittest.TestCase):

    def test_load_active_task_matrix(self):
        matrix = main.load_active_task_matrix()
        self.assertIsInstance(matrix, list)
        self.assertGreater(len(matrix), 50)
        first = matrix[0]
        self.assertIn("commodity_id", first)
        self.assertIn("state_id", first)
        self.assertIn("commodity_name", first)
        self.assertIn("state_name", first)

    def test_parse_date_str(self):
        self.assertEqual(main._parse_date_str("29/08/2026"), "2026-08-29")
        self.assertEqual(main._parse_date_str("29-Aug-2026"), "2026-08-29")
        self.assertEqual(main._parse_date_str("2026-08-29"), "2026-08-29")
        self.assertEqual(main._parse_date_str(""), "")

    def test_compute_clean_market_analytics_and_outlier_clamping(self):
        # Create records including extreme outlier (₹700 typo when median is ₹7,000)
        records = [
            {"commodity": "Bengal Gram", "state": "Madhya Pradesh", "market": "Indore",
             "raw_arrival_quantity": 10.0, "normalized_modal_price_qtl": 7000.0, "trade_date": "2026-08-29"},
            {"commodity": "Bengal Gram", "state": "Maharashtra", "market": "Latur",
             "raw_arrival_quantity": 15.0, "normalized_modal_price_qtl": 7200.0, "trade_date": "2026-08-29"},
            {"commodity": "Bengal Gram", "state": "Rajasthan", "market": "Kota",
             "raw_arrival_quantity": 8.0, "normalized_modal_price_qtl": 6900.0, "trade_date": "2026-08-29"},
            {"commodity": "Bengal Gram", "state": "Gujarat", "market": "Rajkot",
             "raw_arrival_quantity": 5.0, "normalized_modal_price_qtl": 7100.0, "trade_date": "2026-08-29"},
            # Typo price (should be clamped by IQR / median rule)
            {"commodity": "Bengal Gram", "state": "Karnataka", "market": "Gulbarga",
             "raw_arrival_quantity": 2.0, "normalized_modal_price_qtl": 700.0, "trade_date": "2026-08-29"},
        ]

        metrics = main.compute_clean_market_analytics(records, "2026-08-29")
        self.assertEqual(metrics["total_rows"], 5)
        self.assertEqual(metrics["active_commodities"], 1)
        self.assertEqual(metrics["active_states"], 5)
        self.assertEqual(metrics["active_mandis"], 5)

        # Verify outlier ₹700 was filtered from spreads
        self.assertEqual(len(metrics["spreads"]), 1)
        spread = metrics["spreads"][0]
        self.assertEqual(spread["commodity"], "Bengal Gram")
        self.assertGreaterEqual(spread["min_price"], 6900.0)
        self.assertEqual(spread["max_price"], 7200.0)

    def test_build_adaptive_card_structure(self):
        dummy_metrics = {
            "total_rows": 100,
            "active_mandis": 25,
            "active_commodities": 10,
            "active_states": 5,
            "total_volume_tonnes": 5000.0,
            "top_volume_crop": "Wheat",
            "top_volume_val": 2500.0,
            "top_trading_hub": "Indore",
            "top_hub_lots": 15,
            "spreads": [{
                "commodity": "Wheat", "min_price": 2400.0, "median_price": 2500.0,
                "max_price": 2700.0, "spread_pct": 12.5, "observations": 20, "volume_tonnes": 1000.0
            }],
            "date_counts": {"2026-08-29": 40, "2026-08-28": 60},
            "state_counts": {"Madhya Pradesh": {"rows": 50, "mandis": 12, "volume": 3000.0}}
        }

        card = main.build_adaptive_card(dummy_metrics, "Executive brief test.", "2026-08-29", 1.5)
        self.assertEqual(card["type"], "message")
        self.assertEqual(card["attachments"][0]["content"]["version"], "1.5")


if __name__ == "__main__":
    unittest.main()

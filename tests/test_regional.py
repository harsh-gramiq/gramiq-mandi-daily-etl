"""Unit tests for Farmer-First Regional Broadcast (hi-IN)."""

import unittest
from app.notifications.regional_broadcast import format_hindi_farmer_digest


class TestRegionalBroadcast(unittest.TestCase):
    def test_hindi_broadcast_formatting(self):
        sample_metrics = {
            "total_volume_tonnes": 54000.0,
            "active_mandis": 250,
            "active_states": 18,
            "crop_counts": {
                "Wheat": {
                    "avg_price": 2450.0,
                    "min_price": 2300.0,
                    "max_price": 2600.0,
                    "volume": 20000.0,
                    "price_trend": "RALLY",
                },
                "Soyabean": {
                    "avg_price": 4850.0,
                    "min_price": 4600.0,
                    "max_price": 5100.0,
                    "volume": 15000.0,
                    "price_trend": "DECLINE",
                },
            },
            "arbitrage_corridors": [
                {
                    "commodity": "Wheat",
                    "origin_mandi": "Indore",
                    "origin_price": 2400.0,
                    "dest_mandi": "Gazipur",
                    "dest_price": 2750.0,
                    "gross_spread_rs": 350.0,
                }
            ],
        }

        digest = format_hindi_farmer_digest(sample_metrics, "2026-08-30")
        self.assertIn("राष्ट्रीय मंडी भाव बुलेटिन", digest)
        self.assertIn("गेहूं (Wheat)", digest)
        self.assertIn("सोयाबीन (Soyabean)", digest)
        self.assertIn("2026-08-30", digest)
        self.assertIn("तेजी", digest)
        self.assertIn("मंदी", digest)
        self.assertIn("सूचना", digest)


if __name__ == "__main__":
    unittest.main()

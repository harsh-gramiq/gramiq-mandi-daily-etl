"""Unit tests for Mandi REST API Service and OpenAPI Specification."""

import unittest
from app.api.router import MandiAPIService, get_openapi_schema


class TestMandiAPIService(unittest.TestCase):
    def setUp(self):
        self.sample_records = [
            {"commodity": "Wheat", "state": "Madhya Pradesh", "market": "Indore", "normalized_modal_price_qtl": 2450.0},
            {"commodity": "Wheat", "state": "Rajasthan", "market": "Kota", "normalized_modal_price_qtl": 2500.0},
            {"commodity": "Soyabean", "state": "Maharashtra", "market": "Latur", "normalized_modal_price_qtl": 4800.0},
        ]
        self.sample_metrics = {
            "arbitrage_corridors": [
                {
                    "commodity": "Wheat",
                    "origin_mandi": "Indore (Madhya Pradesh)",
                    "origin_price": 2450.0,
                    "dest_mandi": "Kota (Rajasthan)",
                    "dest_price": 2750.0,
                    "gross_spread_rs": 300.0,
                    "spread_pct": 12.2,
                }
            ],
            "price_velocity": {
                "Wheat": {"latest_price": 2450.0, "delta_rs": 50.0, "delta_pct": 2.1, "trend": "RALLY"}
            },
        }
        self.service = MandiAPIService(self.sample_metrics, self.sample_records)

    def test_openapi_schema(self):
        schema = get_openapi_schema()
        self.assertEqual(schema["openapi"], "3.1.0")
        self.assertIn("/api/v1/mandi/latest-rates", schema["paths"])
        self.assertIn("/api/v1/mandi/arbitrage", schema["paths"])
        self.assertIn("/api/v1/mandi/velocity", schema["paths"])
        self.assertIn("/api/v1/mandi/msp-status", schema["paths"])

    def test_get_rates_filtering_and_pagination(self):
        res = self.service.handle_get_rates(commodity="Wheat", limit=1)
        self.assertEqual(res["status"], "SUCCESS")
        self.assertEqual(res["total_count"], 2)
        self.assertEqual(len(res["items"]), 1)
        self.assertEqual(res["items"][0]["commodity"], "Wheat")

    def test_get_arbitrage(self):
        res = self.service.handle_get_arbitrage(commodity="Wheat", min_spread_rs=200.0)
        self.assertEqual(res["status"], "SUCCESS")
        self.assertEqual(res["count"], 1)
        self.assertEqual(res["corridors"][0]["gross_spread_rs"], 300.0)

    def test_get_velocity(self):
        res = self.service.handle_get_velocity("Wheat")
        self.assertEqual(res["status"], "SUCCESS")
        self.assertIn("Wheat", res["velocity"])
        self.assertEqual(res["velocity"]["Wheat"]["trend"], "RALLY")


if __name__ == "__main__":
    unittest.main()

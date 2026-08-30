"""Unit tests for Agmarknet extractor and payload parsing."""

import unittest
from app.extractors.agmarknet import _parse_date_str, parse_market_dates_payload


class TestAgmarknetExtractor(unittest.TestCase):
    def test_parse_date_str(self):
        self.assertEqual(_parse_date_str("29-Aug-2026"), "2026-08-29")
        self.assertEqual(_parse_date_str("01-Jan-2025"), "2025-01-01")
        self.assertEqual(_parse_date_str("15/08/2026"), "2026-08-15")
        self.assertEqual(_parse_date_str("2026-08-29"), "2026-08-29")
        self.assertEqual(_parse_date_str("invalid-date-string"), "")

    def test_parse_market_dates_payload(self):
        payload = {
            "data": {
                "markets": [
                    {
                        "marketName": "Indore",
                        "districtName": "Indore",
                        "dates": [
                            {
                                "arrivalDate": "29-Aug-2026",
                                "data": [
                                    {
                                        "modalPrice": "2600",
                                        "minimumPrice": "2400",
                                        "maximumPrice": "2800",
                                        "arrivals": "150.5",
                                        "variety": "Lokwan",
                                        "grade": "FAQ",
                                    }
                                ],
                            },
                            {
                                "arrivalDate": "01-Jan-2020",  # Out of lookback
                                "data": [
                                    {
                                        "modalPrice": "2000",
                                        "minimumPrice": "1900",
                                        "maximumPrice": "2100",
                                        "arrivals": "50",
                                        "variety": "Lokwan",
                                        "grade": "FAQ",
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "marketName": "Ujjain",
                        "districtName": "Ujjain",
                        "dates": [
                            {
                                "arrivalDate": "29-Aug-2026",
                                "data": [
                                    {
                                        "modalPrice": "0",  # Invalid price, should be skipped
                                        "minimumPrice": "0",
                                        "maximumPrice": "0",
                                        "arrivals": "10",
                                        "variety": "Desi",
                                        "grade": "FAQ",
                                    }
                                ],
                            }
                        ],
                    },
                ]
            }
        }

        task = {
            "commodity_id": 1,
            "state_id": 16,
            "commodity_name": "Wheat",
            "state_name": "Madhya Pradesh",
        }
        lookback_dates = {"2026-08-29", "2026-08-28"}

        records = parse_market_dates_payload(payload, task, lookback_dates)

        self.assertEqual(len(records), 1)

        wheat_rec = records[0]
        self.assertEqual(wheat_rec["commodity"], "Wheat")
        self.assertEqual(wheat_rec["state"], "Madhya Pradesh")
        self.assertEqual(wheat_rec["market"], "Indore")
        self.assertEqual(wheat_rec["normalized_modal_price_qtl"], 2600.0)
        self.assertEqual(wheat_rec["normalized_min_price_qtl"], 2400.0)
        self.assertEqual(wheat_rec["normalized_max_price_qtl"], 2800.0)
        self.assertEqual(wheat_rec["trade_date"], "2026-08-29")
        self.assertEqual(wheat_rec["quality_status"], "accepted")
        self.assertTrue(len(wheat_rec["observation_hash"]) == 64)


if __name__ == "__main__":
    unittest.main()

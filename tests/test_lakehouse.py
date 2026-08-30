"""Unit tests for Partitioned Parquet Lakehouse Exporter."""

import os
import unittest
from pathlib import Path
from app.storage.lakehouse import export_to_lakehouse


class TestLakehouseExporter(unittest.TestCase):
    def test_export_empty_records(self):
        res = export_to_lakehouse([], "2026-08-29")
        self.assertEqual(res["status"], "EMPTY")
        self.assertEqual(res["record_count"], 0)

    def test_export_valid_records(self):
        records = [
            {
                "trade_date": "2026-08-29",
                "state": "Madhya Pradesh",
                "district": "Indore",
                "market": "Indore",
                "commodity": "Wheat",
                "variety": "Lokwan",
                "grade": "FAQ",
                "normalized_modal_price_qtl": 2500.0,
                "min_price_qtl": 2400.0,
                "max_price_qtl": 2600.0,
                "raw_arrival_quantity": "150",
            },
            {
                "trade_date": "2026-08-29",
                "state": "Rajasthan",
                "district": "Kota",
                "market": "Kota",
                "commodity": "Mustard",
                "variety": "Standard",
                "grade": "FAQ",
                "normalized_modal_price_qtl": 5800.0,
                "min_price_qtl": 5600.0,
                "max_price_qtl": 6000.0,
                "raw_arrival_quantity": "80",
            },
        ]
        res = export_to_lakehouse(records, "2026-08-29", output_dir="data/lakehouse")
        self.assertEqual(res["status"], "SUCCESS")
        self.assertEqual(res["record_count"], 2)
        self.assertTrue(os.path.exists(res["file_path"]))
        self.assertIn("2026-08-29", res["file_path"])


if __name__ == "__main__":
    unittest.main()

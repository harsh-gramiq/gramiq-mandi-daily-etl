"""Unit tests for Watermark Delta Caching Engine."""

import json
import unittest
from unittest.mock import mock_open, patch
from app.extractor.watermark import WatermarkCache


class TestWatermarkCache(unittest.TestCase):
    def test_cache_miss_and_hit(self):
        wc = WatermarkCache("data/cache/test_watermarks.json")
        # Ensure clean state
        wc.cache = {}

        # Initially not cached
        self.assertFalse(wc.is_cached_complete("Madhya Pradesh", "Wheat", "2026-08-22", "2026-08-29"))

        # Record completed result
        wc.record_task_result("Madhya Pradesh", "Wheat", "2026-08-22", "2026-08-29", "COMPLETED_WITH_DATA", 50)

        # Now should be hit
        self.assertTrue(wc.is_cached_complete("Madhya Pradesh", "Wheat", "2026-08-22", "2026-08-29"))

        # Market closed should also be cached
        wc.record_task_result("Punjab", "Rice", "2026-08-22", "2026-08-29", "MARKET_CLOSED", 0)
        self.assertTrue(wc.is_cached_complete("Punjab", "Rice", "2026-08-22", "2026-08-29"))

        # Incomplete / failed status should NOT count as complete
        wc.record_task_result("Haryana", "Cotton", "2026-08-22", "2026-08-29", "FAILED", 0)
        self.assertFalse(wc.is_cached_complete("Haryana", "Cotton", "2026-08-22", "2026-08-29"))

    def test_cache_serialization_and_load(self):
        mock_data = {
            "key1": {
                "state": "Madhya Pradesh",
                "crop": "Wheat",
                "from_date": "2026-08-22",
                "to_date": "2026-08-29",
                "status": "COMPLETED_WITH_DATA",
                "record_count": 50,
            }
        }
        with patch("pathlib.Path.exists", return_value=True):
            with patch("builtins.open", mock_open(read_data=json.dumps(mock_data))):
                wc = WatermarkCache("data/cache/dummy.json")
                self.assertIn("key1", wc.cache)
                self.assertEqual(wc.cache["key1"]["record_count"], 50)


if __name__ == "__main__":
    unittest.main()

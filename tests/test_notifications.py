"""Unit tests for Gemini brief, Adaptive Card v1.5 builder, and step summary."""

import json
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from app.notifications.gemini import generate_gemini_market_brief
from app.notifications.teams import build_adaptive_card, dispatch_card_to_teams
from app.notifications.step_summary import write_github_step_summary


class TestNotifications(unittest.TestCase):
    def setUp(self):
        self.sample_metrics = {
            "total_rows": 5000,
            "active_mandis": 350,
            "active_commodities": 65,
            "active_states": 20,
            "total_volume_tonnes": 45000.0,
            "top_volume_crop": "Wheat",
            "top_volume_val": 15000.0,
            "top_trading_hub": "Indore",
            "top_hub_lots": 85,
            "spreads": [
                {
                    "commodity": "Chana(Gram)",
                    "min_price": 5400.0,
                    "max_price": 7200.0,
                    "median_price": 6100.0,
                    "spread_pct": 33.3,
                    "observations": 120,
                    "volume_tonnes": 3400.0,
                }
            ],
            "date_counts": {"2026-08-29": 4200, "2026-08-28": 800},
            "state_counts": {
                "Madhya Pradesh": {"rows": 2000, "mandis": 110, "volume": 18000.0},
                "Rajasthan": {"rows": 1500, "mandis": 90, "volume": 12000.0},
            },
        }

    def test_gemini_fallback_without_api_key(self):
        with patch.dict(os.environ, {}, clear=True):
            brief = generate_gemini_market_brief(self.sample_metrics, "2026-08-29")
            self.assertIn("5,000 validated observations", brief)
            self.assertIn("Wheat (15,000 Tonnes)", brief)
            self.assertIn("Chana(Gram)", brief)

    def test_adaptive_card_structure(self):
        card = build_adaptive_card(self.sample_metrics, "AI Brief Test", "2026-08-29", 45.2)

        self.assertEqual(card["type"], "message")
        self.assertIn("attachments", card)
        attachment = card["attachments"][0]
        self.assertEqual(attachment["contentType"], "application/vnd.microsoft.card.adaptive")

        content = attachment["content"]
        self.assertEqual(content["type"], "AdaptiveCard")
        self.assertEqual(content["version"], "1.5")
        self.assertEqual(content["$schema"], "http://adaptivecards.io/schemas/adaptive-card.json")

        # Verify Toggle Visibility action exists
        actions = content.get("actions", [])
        self.assertTrue(any(a["type"] == "Action.ToggleVisibility" for a in actions))

    def test_github_step_summary_writer(self):
        with tempfile.NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8") as tf:
            summary_file = tf.name

        try:
            with patch.dict(os.environ, {"GITHUB_STEP_SUMMARY": summary_file}):
                success = write_github_step_summary(
                    self.sample_metrics,
                    "AI Market Intelligence Brief Test",
                    "2026-08-29",
                    7,
                    32.5,
                )
                self.assertTrue(success)

            with open(summary_file, "r", encoding="utf-8") as f:
                content = f.read()

            self.assertIn("National Mandi Ingestion & 7-Day Rolling Reconciliation", content)
            self.assertIn("5,000 rows", content)
            self.assertIn("Chana(Gram)", content)
            self.assertIn("AI Market Intelligence Brief Test", content)
        finally:
            if os.path.exists(summary_file):
                os.remove(summary_file)


if __name__ == "__main__":
    unittest.main()
